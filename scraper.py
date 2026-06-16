"""
抓取器模块 (Scraper): 负责从目标端点定期拉取指标
- Prometheus 文本格式解析
- 定期调度抓取
- 目标健康状态维护
"""
from typing import Dict, List, Optional
import threading
import time
import re
import urllib.request
import urllib.error
import socket

from config import LabelSet, Sample, Metric, ScrapeTarget
from tsdb import TSDB


# ==================== Prometheus 文本格式解析 ====================

_METRIC_LINE_RE = re.compile(
    r'^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\s*'
    r'(?:\{(?P<labels>[^}]*)\})?\s*'
    r'(?P<value>-?[0-9]+\.?[0-9]*(?:[eE][-+]?[0-9]+)?|NaN|\+Inf|-Inf)\s*'
    r'(?P<timestamp>[0-9]+)?\s*$'
)

_LABEL_RE = re.compile(
    r'(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"(?P<value>(?:[^"\\]|\\.)*)"'
)


def parse_prometheus_text(text: str, scrape_timestamp: Optional[float] = None) -> List[Metric]:
    """
    解析 Prometheus 文本格式的指标数据
    格式: metric_name{label1="val1",...} value [timestamp_ms]
    """
    if scrape_timestamp is None:
        scrape_timestamp = time.time()

    metrics: List[Metric] = []
    current_help: Dict[str, str] = {}
    current_type: Dict[str, str] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            if line.startswith("# HELP"):
                parts = line.split(None, 3)
                if len(parts) >= 3:
                    current_help[parts[2]] = parts[3] if len(parts) > 3 else ""
            elif line.startswith("# TYPE"):
                parts = line.split(None, 3)
                if len(parts) >= 3:
                    current_type[parts[2]] = parts[3] if len(parts) > 3 else ""
            continue

        m = _METRIC_LINE_RE.match(line)
        if not m:
            continue

        name = m.group("name")
        value_str = m.group("value")
        timestamp_str = m.group("timestamp")
        labels_str = m.group("labels")

        try:
            if value_str == "NaN":
                value = float("nan")
            elif value_str in ("+Inf", "Inf"):
                value = float("inf")
            elif value_str == "-Inf":
                value = float("-inf")
            else:
                value = float(value_str)
        except ValueError:
            continue

        if timestamp_str:
            ts = float(timestamp_str) / 1000.0
        else:
            ts = scrape_timestamp

        labels: Dict[str, str] = {}
        if labels_str:
            for lm in _LABEL_RE.finditer(labels_str):
                labels[lm.group("name")] = _unescape_label_value(lm.group("value"))

        metrics.append(Metric(
            name=name,
            labels=LabelSet(labels),
            sample=Sample(timestamp=ts, value=value)
        ))

    return metrics


def _unescape_label_value(s: str) -> str:
    return (s.replace('\\"', '"')
             .replace('\\\\', '\\')
             .replace('\\n', '\n'))


def format_prometheus_text(metrics: List[Metric]) -> str:
    """将指标序列化为 Prometheus 文本格式（用于模拟目标端点）"""
    lines = []
    for m in metrics:
        label_parts = []
        for k, v in sorted(m.labels.labels.items()):
            escaped = v.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
            label_parts.append(f'{k}="{escaped}"')

        if label_parts:
            label_str = "{" + ",".join(label_parts) + "}"
        else:
            label_str = ""

        ts_ms = int(m.sample.timestamp * 1000)
        lines.append(f"{m.name}{label_str} {m.sample.value} {ts_ms}")

    return "\n".join(lines) + "\n"


# ==================== 抓取器 ====================

class ScraperManager:
    """
    抓取管理器: 管理所有目标的定期抓取
    - 为每个目标创建独立的抓取循环
    - 记录目标健康状态 (up/down)
    - 抓取失败时更新目标状态并记录错误
    """

    def __init__(self, tsdb: TSDB):
        self._tsdb = tsdb
        self._targets: Dict[str, ScrapeTarget] = {}
        self._threads: Dict[str, threading.Thread] = {}
        self._stop_events: Dict[str, threading.Event] = {}
        self._lock = threading.RLock()

    def add_target(self, target: ScrapeTarget) -> None:
        key = self._target_key(target)
        with self._lock:
            if key in self._targets:
                self.remove_target(target)
            self._targets[key] = target
            stop_evt = threading.Event()
            self._stop_events[key] = stop_evt
            t = threading.Thread(
                target=self._scrape_loop,
                args=(key, target, stop_evt),
                daemon=True,
                name=f"scraper-{target.job}"
            )
            self._threads[key] = t
            t.start()

    def remove_target(self, target: ScrapeTarget) -> None:
        key = self._target_key(target)
        with self._lock:
            if key in self._stop_events:
                self._stop_events[key].set()
            if key in self._threads:
                self._threads[key].join(timeout=5)
            self._targets.pop(key, None)
            self._threads.pop(key, None)
            self._stop_events.pop(key, None)

    def set_targets(self, targets: List[ScrapeTarget]) -> None:
        new_keys = {self._target_key(t) for t in targets}
        with self._lock:
            existing_keys = set(self._targets.keys())
            for key in existing_keys - new_keys:
                t = self._targets[key]
                self.remove_target(t)
            for t in targets:
                self.add_target(t)

    def get_targets(self) -> List[ScrapeTarget]:
        with self._lock:
            return list(self._targets.values())

    def get_target(self, job: str, url: str) -> Optional[ScrapeTarget]:
        key = f"{job}|{url}"
        return self._targets.get(key)

    def scrape_once(self, target: ScrapeTarget) -> List[Metric]:
        """执行一次抓取（同步）"""
        start = time.time()
        try:
            req = urllib.request.Request(target.url, headers={"Accept": "text/plain"})
            with urllib.request.urlopen(req, timeout=target.scrape_timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")

            metrics = parse_prometheus_text(raw, scrape_timestamp=start)
            result = self._apply_static_labels(metrics, target)

            duration = time.time() - start
            target.mark_healthy(duration)
            self._emit_up_metric(target, 1.0, start)
            self._emit_scrape_duration(target, duration, start)
            self._emit_scrape_samples(target, len(result), start)

            self._tsdb.append_batch(result)
            return result

        except (urllib.error.URLError, socket.timeout, OSError, Exception) as e:
            duration = time.time() - start
            target.mark_unhealthy(str(e))
            self._emit_up_metric(target, 0.0, start)
            self._emit_scrape_duration(target, duration, start)
            self._emit_scrape_samples(target, 0, start)
            return []

    # ===== 内部方法 =====

    @staticmethod
    def _target_key(target: ScrapeTarget) -> str:
        return f"{target.job}|{target.url}"

    def _scrape_loop(self, key: str, target: ScrapeTarget, stop: threading.Event):
        """每个目标的独立抓取循环"""
        self.scrape_once(target)
        while not stop.is_set():
            interval = target.scrape_interval
            if stop.wait(interval):
                break
            self.scrape_once(target)

    def _apply_static_labels(self, metrics: List[Metric], target: ScrapeTarget) -> List[Metric]:
        """为抓取的指标附加静态标签 (job, instance 等)"""
        result = []
        for m in metrics:
            new_labels = dict(target.static_labels)
            new_labels["job"] = target.job
            for k, v in m.labels.labels.items():
                if k not in new_labels:
                    new_labels[k] = v
            result.append(Metric(
                name=m.name,
                labels=LabelSet(new_labels),
                sample=m.sample
            ))
        return result

    def _emit_up_metric(self, target: ScrapeTarget, value: float, ts: float):
        """写入目标健康状态指标 up=1/0"""
        labels = dict(target.static_labels)
        labels["job"] = target.job
        labels["instance"] = target.url
        self._tsdb.append(Metric(
            name="up",
            labels=LabelSet(labels),
            sample=Sample(timestamp=ts, value=value)
        ))

    def _emit_scrape_duration(self, target: ScrapeTarget, value: float, ts: float):
        labels = dict(target.static_labels)
        labels["job"] = target.job
        labels["instance"] = target.url
        self._tsdb.append(Metric(
            name="scrape_duration_seconds",
            labels=LabelSet(labels),
            sample=Sample(timestamp=ts, value=value)
        ))

    def _emit_scrape_samples(self, target: ScrapeTarget, count: int, ts: float):
        labels = dict(target.static_labels)
        labels["job"] = target.job
        labels["instance"] = target.url
        self._tsdb.append(Metric(
            name="scrape_samples_scraped",
            labels=LabelSet(labels),
            sample=Sample(timestamp=ts, value=float(count))
        ))

    def shutdown(self) -> None:
        with self._lock:
            for key in list(self._targets.keys()):
                t = self._targets[key]
                self.remove_target(t)
