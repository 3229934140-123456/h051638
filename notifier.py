"""
通知模块 (Notifier):
- 告警去重 (同一告警只发一次，除非 repeat_interval 到了)
- 告警分组 (按 group_by 标签合并，避免告警风暴)
- 告警路由 (按标签匹配发送到不同渠道)
- 多种通知渠道 (控制台、Webhook、邮件模拟)
"""
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import threading
import time
import json
import urllib.request
import urllib.error

from config import Alert, NotificationRoute, LabelMatcher


# ==================== 通知渠道 ====================

class NotificationChannel:
    """通知渠道基类"""

    def __init__(self, name: str):
        self.name = name

    def send(self, group_key: str, alerts: List[Alert]) -> bool:
        raise NotImplementedError


class ConsoleChannel(NotificationChannel):
    """控制台渠道: 打印到标准输出"""

    def __init__(self, name: str = "console"):
        super().__init__(name)

    def send(self, group_key: str, alerts: List[Alert]) -> bool:
        print("\n" + "=" * 60)
        print(f"[NOTIFICATION] Channel: {self.name} | Group: {group_key}")
        print(f"  Alerts count: {len(alerts)}")
        for a in alerts:
            status_icon = "🔴" if a.status == "firing" else ("🟡" if a.status == "pending" else "🟢")
            print(f"  {status_icon} [{a.status.upper()}] {a.rule_name}")
            print(f"     Fingerprint: {a.fingerprint}")
            print(f"     Labels: {dict(a.labels.labels)}")
            print(f"     Value: {a.value}")
            print(f"     Duration: {a.duration():.1f}s")
            if a.annotations:
                for k, v in a.annotations.items():
                    print(f"     {k}: {v}")
        print("=" * 60 + "\n")
        return True


class WebhookChannel(NotificationChannel):
    """Webhook 渠道: POST JSON 到指定 URL"""

    def __init__(self, name: str, url: str, timeout: float = 10.0):
        super().__init__(name)
        self._url = url
        self._timeout = timeout

    def send(self, group_key: str, alerts: List[Alert]) -> bool:
        payload = {
            "group_key": group_key,
            "channel": self.name,
            "alerts": [
                {
                    "fingerprint": a.fingerprint,
                    "rule_name": a.rule_name,
                    "status": a.status,
                    "labels": dict(a.labels.labels),
                    "annotations": dict(a.annotations),
                    "value": a.value,
                    "starts_at": a.starts_at,
                    "ends_at": a.ends_at,
                }
                for a in alerts
            ],
            "sent_at": time.time(),
        }
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self._url, data=data, method="POST",
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return 200 <= resp.status < 300
        except (urllib.error.URLError, OSError) as e:
            print(f"[Webhook] Failed to send to {self._url}: {e}")
            return False


class EmailChannel(NotificationChannel):
    """邮件渠道 (模拟)"""

    def __init__(self, name: str, to_address: str):
        super().__init__(name)
        self._to = to_address
        self._sent_log: List[Dict] = []

    def send(self, group_key: str, alerts: List[Alert]) -> bool:
        for a in alerts:
            self._sent_log.append({
                "to": self._to,
                "subject": f"[ALERT] {a.status.upper()}: {a.rule_name}",
                "body": f"Labels: {dict(a.labels.labels)}\nValue: {a.value}\n"
                        f"Duration: {a.duration():.1f}s",
                "sent_at": time.time(),
            })
        print(f"[EMAIL] Sent {len(alerts)} alert(s) to {self._to} (simulated)")
        return True

    @property
    def sent_count(self) -> int:
        return len(self._sent_log)


# ==================== 分组与去重状态 ====================

class PendingGroup:
    """等待发送的告警组 (用于 group_wait 聚合)"""

    def __init__(self, group_key: str):
        self.group_key = group_key
        self.alerts: Dict[str, Alert] = {}
        self.created_at = time.time()
        self.timer: Optional[threading.Timer] = None

    def add_or_update(self, alert: Alert):
        self.alerts[alert.fingerprint] = alert

    def get_alerts(self) -> List[Alert]:
        return list(self.alerts.values())


# ==================== 通知管理器 ====================

class Notifier:
    """
    通知管理器:
    - 接收 AlertManager 推送的告警
    - 根据路由规则分发到通知渠道
    - 支持: 去重、分组、延迟聚合、重复间隔控制
    """

    def __init__(self, routes: Optional[List[NotificationRoute]] = None):
        self._channels: Dict[str, NotificationChannel] = {}
        self._routes: List[NotificationRoute] = routes or []
        self._pending_groups: Dict[Tuple[str, str], PendingGroup] = {}
        self._sent_timestamps: Dict[Tuple[str, str, str], float] = {}
        self._lock = threading.RLock()
        self._timers_lock = threading.Lock()
        self._notified_log: List[Dict] = []

    # ===== 渠道与路由管理 =====

    def register_channel(self, channel: NotificationChannel) -> None:
        with self._lock:
            self._channels[channel.name] = channel

    def set_routes(self, routes: List[NotificationRoute]) -> None:
        with self._lock:
            self._routes = list(routes)

    def add_route(self, route: NotificationRoute) -> None:
        with self._lock:
            self._routes.append(route)

    # ===== 接收告警入口 =====

    def handle_alerts(self, alerts: List[Alert]) -> None:
        """接收一批告警事件 (来自 AlertManager)"""
        if not alerts:
            return

        grouped = self._group_by_routes(alerts)
        for (route_name, channel_name, group_key), group_alerts in grouped.items():
            self._enqueue_group(route_name, channel_name, group_key, group_alerts)

    # ===== 内部: 路由匹配与分组 =====

    def _group_by_routes(self, alerts: List[Alert]) -> Dict[Tuple[str, str, str], List[Alert]]:
        """
        将告警按 (路由名, 渠道名, 组键) 归类
        - 告警匹配哪个路由?
        - 路由配置的 group_by 标签有哪些?
        - 发送到哪些渠道?
        """
        result: Dict[Tuple[str, str, str], List[Alert]] = defaultdict(list)

        for alert in alerts:
            matched_routes = self._match_routes(alert)
            if not matched_routes:
                if self._routes:
                    continue
                route = NotificationRoute(
                    name="default", label_matchers=[],
                    channels=["console"], group_by=["alertname"]
                )
                matched_routes = [route]

            for route in matched_routes:
                group_key = self._compute_group_key(alert, route)
                for ch_name in route.channels:
                    key = (route.name, ch_name, group_key)
                    result[key].append(alert)

        return result

    def _match_routes(self, alert: Alert) -> List[NotificationRoute]:
        """找出匹配告警标签的所有路由"""
        matched = []
        for route in self._routes:
            if self._route_matches(route, alert):
                matched.append(route)
        return matched

    @staticmethod
    def _route_matches(route: NotificationRoute, alert: Alert) -> bool:
        for matcher in route.label_matchers:
            lv = alert.labels.get(matcher.name)
            if not matcher.matches(lv):
                return False
        return True

    @staticmethod
    def _compute_group_key(alert: Alert, route: NotificationRoute) -> str:
        """根据 group_by 标签计算组键"""
        if not route.group_by:
            return "__all__"
        parts = []
        for ln in sorted(route.group_by):
            parts.append(f"{ln}={alert.labels.get(ln) or ''}")
        return "|".join(parts)

    # ===== 内部: 组队列 (group_wait) =====

    def _enqueue_group(self, route_name: str, channel_name: str,
                       group_key: str, alerts: List[Alert]) -> None:
        """将一组告警加入待发送队列，等待 group_wait 以便聚合"""
        queue_key = (route_name, channel_name, group_key)

        with self._timers_lock:
            pending = self._pending_groups.get(queue_key)
            if pending is None:
                route = self._find_route(route_name)
                group_wait = route.group_wait if route else 10.0
                pending = PendingGroup(group_key)
                self._pending_groups[queue_key] = pending

                timer = threading.Timer(
                    group_wait,
                    self._flush_group,
                    args=(queue_key, channel_name, route_name)
                )
                pending.timer = timer
                timer.daemon = True
                timer.start()

            for a in alerts:
                pending.add_or_update(a)

    def _flush_group(self, queue_key: Tuple[str, str, str],
                     channel_name: str, route_name: str) -> None:
        """group_wait 到期后执行实际发送"""
        with self._timers_lock:
            pending = self._pending_groups.pop(queue_key, None)
        if pending is None:
            return

        now = time.time()
        route = self._find_route(route_name)
        repeat_interval = route.repeat_interval if route else 300.0

        to_send = []
        for alert in pending.get_alerts():
            ts_key = (queue_key[0], queue_key[1], alert.fingerprint)
            last_sent = self._sent_timestamps.get(ts_key, 0)

            if alert.status == "resolved":
                if last_sent > 0:
                    to_send.append(alert)
                    self._sent_timestamps.pop(ts_key, None)
            elif alert.status == "firing":
                if now - last_sent >= repeat_interval:
                    to_send.append(alert)
                    self._sent_timestamps[ts_key] = now

        if not to_send:
            return

        channel = self._channels.get(channel_name)
        if channel is None:
            if channel_name == "console":
                channel = ConsoleChannel()
                self._channels[channel_name] = channel
            else:
                print(f"[Notifier] Unknown channel: {channel_name}")
                return

        try:
            success = channel.send(pending.group_key, to_send)
            if success:
                for a in to_send:
                    self._notified_log.append({
                        "channel": channel_name,
                        "route": route_name,
                        "group_key": pending.group_key,
                        "fingerprint": a.fingerprint,
                        "rule_name": a.rule_name,
                        "status": a.status,
                        "value": a.value,
                        "sent_at": now,
                    })
        except Exception as e:
            print(f"[Notifier] Error sending via {channel_name}: {e}")

    def _find_route(self, name: str) -> Optional[NotificationRoute]:
        for r in self._routes:
            if r.name == name:
                return r
        return None

    # ===== 统计与调试 =====

    @property
    def notified_count(self) -> int:
        return len(self._notified_log)

    def recent_notifications(self, limit: int = 10) -> List[Dict]:
        return self._notified_log[-limit:]

    def shutdown(self) -> None:
        with self._timers_lock:
            for pending in list(self._pending_groups.values()):
                if pending.timer:
                    pending.timer.cancel()
            self._pending_groups.clear()
