"""
告警评估模块 (Alert Evaluator):
- 周期性对告警规则求值
- 维护告警状态机 (pending -> firing -> resolved)
- 基于 for_duration 的持续触发逻辑
- 生成 Alert 对象并传递给通知系统
"""
from typing import Dict, List, Optional, Callable
import threading
import time
import hashlib
import copy

from config import (
    AlertRule, Alert, LabelSet, LabelMatcher,
)
from query import QueryEngine


def _compute_fingerprint(labels: LabelSet, rule_name: str) -> str:
    """计算告警指纹，用于去重和状态跟踪"""
    raw = f"{rule_name}|{labels.key}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class AlertState:
    """单条告警的内部状态跟踪"""

    def __init__(self, rule: AlertRule, labels: LabelSet, fingerprint: str,
                 annotations: Dict[str, str]):
        self.rule = rule
        self.labels = labels
        self.fingerprint = fingerprint
        self.annotations = annotations
        self.active_at: Optional[float] = None
        self.triggered_at: Optional[float] = None
        self.last_value: float = 0.0
        self.last_evaluated: Optional[float] = None
        self.status: str = "inactive"

    def update(self, firing: bool, value: float, now: float) -> Optional[str]:
        """
        更新告警状态，返回状态变化事件
        事件: 'pending', 'firing', 'resolved', None(无变化)
        """
        self.last_value = value
        self.last_evaluated = now

        if firing:
            if self.active_at is None:
                self.active_at = now
                self.status = "pending"
                return "pending"

            elapsed = now - self.active_at
            if elapsed >= self.rule.for_duration:
                if self.status != "firing":
                    self.status = "firing"
                    self.triggered_at = now
                    return "firing"
                return None
            return None
        else:
            prev_status = self.status
            self.active_at = None
            if prev_status in ("firing", "pending"):
                self.status = "inactive"
                return "resolved"
            self.status = "inactive"
            return None

    def to_alert(self, now: Optional[float] = None) -> Alert:
        now = now or time.time()
        starts_at = self.triggered_at if self.status == "firing" else (self.active_at or now)
        return Alert(
            fingerprint=self.fingerprint,
            rule_name=self.rule.name,
            labels=self.labels,
            annotations=dict(self.annotations),
            value=self.last_value,
            starts_at=starts_at,
            ends_at=None if self.status in ("firing", "pending") else now,
            status=self.status,
            last_sent=None
        )


class AlertManager:
    """
    告警管理器:
    - 周期性评估所有告警规则
    - 维护告警状态机
    - 通过回调通知上层 (如 Notifier)
    """

    def __init__(self, query_engine: QueryEngine, evaluation_interval: float = 15.0):
        self._qe = query_engine
        self._interval = evaluation_interval
        self._rules: Dict[str, AlertRule] = {}
        self._states: Dict[str, AlertState] = {}
        self._listeners: List[Callable[[List[Alert]], None]] = []
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._last_evaluation: Optional[float] = None

    # ===== 规则管理 =====

    def add_rule(self, rule: AlertRule) -> None:
        with self._lock:
            self._rules[rule.name] = rule

    def remove_rule(self, name: str) -> None:
        with self._lock:
            self._rules.pop(name, None)

    def set_rules(self, rules: List[AlertRule]) -> None:
        with self._lock:
            self._rules = {r.name: r for r in rules}

    def get_rules(self) -> List[AlertRule]:
        with self._lock:
            return list(self._rules.values())

    # ===== 事件监听 =====

    def add_listener(self, listener: Callable[[List[Alert]], None]) -> None:
        with self._lock:
            self._listeners.append(listener)

    # ===== 获取告警状态 =====

    def get_active_alerts(self) -> List[Alert]:
        now = time.time()
        result = []
        with self._lock:
            for state in self._states.values():
                if state.status in ("firing", "pending"):
                    result.append(state.to_alert(now))
        return result

    def get_all_states(self) -> List[Alert]:
        now = time.time()
        with self._lock:
            return [s.to_alert(now) for s in self._states.values()]

    # ===== 评估循环 =====

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._eval_loop, daemon=True, name="alerter"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def evaluate_now(self) -> List[Alert]:
        """立即执行一次评估，返回有状态变化的告警"""
        return self._do_evaluation()

    @property
    def last_evaluation(self) -> Optional[float]:
        return self._last_evaluation

    # ===== 内部方法 =====

    def _eval_loop(self) -> None:
        self._do_evaluation()
        while not self._stop.is_set():
            if self._stop.wait(self._interval):
                break
            self._do_evaluation()

    def _do_evaluation(self) -> List[Alert]:
        now = time.time()
        changed_alerts: List[Alert] = []

        with self._lock:
            rules = list(self._rules.values())

        for rule in rules:
            rule_changed = self._evaluate_rule(rule, now)
            changed_alerts.extend(rule_changed)

        self._last_evaluation = now

        if changed_alerts:
            with self._lock:
                listeners = list(self._listeners)
            for listener in listeners:
                try:
                    listener(list(changed_alerts))
                except Exception as e:
                    print(f"[AlertManager] Listener error: {e}")

        return changed_alerts

    def _evaluate_rule(self, rule: AlertRule, now: float) -> List[Alert]:
        """评估单个告警规则"""
        changed: List[Alert] = []

        try:
            result = self._qe.instant_query(
                metric_name=rule.metric_name,
                label_matchers=rule.label_matchers,
                at=now
            )
        except Exception as e:
            print(f"[AlertManager] Query error for rule {rule.name}: {e}")
            return changed

        firing_fingerprints = set()

        for item in result:
            firing = rule.evaluate_condition(item.value)
            if not firing:
                continue

            merged_labels = self._merge_alert_labels(rule, item.labels)
            fp = _compute_fingerprint(merged_labels, rule.name)
            firing_fingerprints.add(fp)

            state = self._states.get(fp)
            if state is None:
                annotations = self._render_annotations(rule, item.labels, item.value)
                state = AlertState(rule, merged_labels, fp, annotations)
                self._states[fp] = state

            event = state.update(firing=True, value=item.value, now=now)
            if event in ("firing", "resolved"):
                changed.append(state.to_alert(now))
            elif event == "pending":
                changed.append(state.to_alert(now))

        inactive_keys = []
        for fp, state in self._states.items():
            if state.rule.name == rule.name and fp not in firing_fingerprints:
                event = state.update(firing=False, value=state.last_value, now=now)
                if event == "resolved":
                    changed.append(state.to_alert(now))
                if state.status == "inactive" and state.last_evaluated is not None:
                    if now - state.last_evaluated > 3600:
                        inactive_keys.append(fp)

        for fp in inactive_keys:
            self._states.pop(fp, None)

        return changed

    @staticmethod
    def _merge_alert_labels(rule: AlertRule, metric_labels: LabelSet) -> LabelSet:
        merged = dict(metric_labels.labels)
        for k, v in rule.labels.items():
            merged[k] = v
        if "alertname" not in merged:
            merged["alertname"] = rule.name
        return LabelSet(merged)

    @staticmethod
    def _render_annotations(rule: AlertRule, labels: LabelSet,
                            value: float) -> Dict[str, str]:
        result = {}
        for k, tmpl in rule.annotations.items():
            rendered = tmpl
            for ln, lv in labels.labels.items():
                rendered = rendered.replace("{{$labels." + ln + "}}", lv)
            rendered = rendered.replace("{{$value}}", str(value))
            result[k] = rendered
        return result
