"""
目标动态发现模块 (Discovery):
- 静态目标配置
- 基于 DNS 的服务发现 (模拟)
- 基于文件的目标发现
- 定期刷新目标列表并通知抓取器
"""
from typing import Dict, List, Optional, Callable
import threading
import time
import json
import os

from config import ScrapeTarget


class TargetProvider:
    """目标提供器基类"""

    def discover(self) -> List[ScrapeTarget]:
        raise NotImplementedError


class StaticTargetProvider(TargetProvider):
    """静态目标提供器"""

    def __init__(self, targets: List[ScrapeTarget]):
        self._targets = list(targets)

    def discover(self) -> List[ScrapeTarget]:
        return list(self._targets)

    def update(self, targets: List[ScrapeTarget]) -> None:
        self._targets = list(targets)


class FileTargetProvider(TargetProvider):
    """基于文件的目标发现: 定期读取 JSON 文件"""

    def __init__(self, filepath: str, default_interval: float = 15.0,
                 default_timeout: float = 10.0):
        self._filepath = filepath
        self._default_interval = default_interval
        self._default_timeout = default_timeout
        self._last_mtime = None

    def discover(self) -> List[ScrapeTarget]:
        if not os.path.exists(self._filepath):
            return []
        try:
            mtime = os.path.getmtime(self._filepath)
            if mtime == self._last_mtime:
                return []
            self._last_mtime = mtime

            with open(self._filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            targets = []
            for item in data:
                targets.append(ScrapeTarget(
                    job=item.get("job", "unknown"),
                    url=item["url"],
                    scrape_interval=float(item.get("scrape_interval", self._default_interval)),
                    scrape_timeout=float(item.get("scrape_timeout", self._default_timeout)),
                    static_labels=item.get("labels", {})
                ))
            return targets
        except (json.JSONDecodeError, KeyError, OSError):
            return []


class DNSTargetProvider(TargetProvider):
    """
    DNS 服务发现 (模拟实现):
    实际场景中可通过 DNS SRV 记录查询或 A 记录轮询获取后端实例
    """

    def __init__(self, job: str, dns_name: str, port: int = 8080,
                 metrics_path: str = "/metrics",
                 default_interval: float = 15.0,
                 default_timeout: float = 10.0):
        self._job = job
        self._dns_name = dns_name
        self._port = port
        self._metrics_path = metrics_path
        self._default_interval = default_interval
        self._default_timeout = default_timeout
        self._simulated_hosts: List[str] = []

    def set_simulated_hosts(self, hosts: List[str]) -> None:
        """模拟 DNS 查询返回的主机列表"""
        self._simulated_hosts = list(hosts)

    def discover(self) -> List[ScrapeTarget]:
        targets = []
        for host in self._simulated_hosts:
            targets.append(ScrapeTarget(
                job=self._job,
                url=f"http://{host}:{self._port}{self._metrics_path}",
                scrape_interval=self._default_interval,
                scrape_timeout=self._default_timeout,
                static_labels={"instance": host}
            ))
        return targets


class DiscoveryManager:
    """
    发现管理器:
    - 组合多个目标提供器
    - 定期刷新目标列表
    - 通过回调通知监听者 (如 ScraperManager)
    """

    def __init__(self, interval: float = 60.0):
        self._providers: Dict[str, TargetProvider] = {}
        self._interval = interval
        self._listeners: List[Callable[[List[ScrapeTarget]], None]] = []
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._current_targets: List[ScrapeTarget] = []

    def add_provider(self, name: str, provider: TargetProvider) -> None:
        with self._lock:
            self._providers[name] = provider

    def remove_provider(self, name: str) -> None:
        with self._lock:
            self._providers.pop(name, None)

    def add_listener(self, listener: Callable[[List[ScrapeTarget]], None]) -> None:
        with self._lock:
            self._listeners.append(listener)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="discovery")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def refresh_now(self) -> List[ScrapeTarget]:
        """立即执行一次发现"""
        return self._do_discovery()

    @property
    def current_targets(self) -> List[ScrapeTarget]:
        with self._lock:
            return list(self._current_targets)

    # ===== 内部 =====

    def _loop(self) -> None:
        self._do_discovery()
        while not self._stop.is_set():
            if self._stop.wait(self._interval):
                break
            self._do_discovery()

    def _do_discovery(self) -> List[ScrapeTarget]:
        with self._lock:
            all_targets: List[ScrapeTarget] = []
            seen = set()
            for provider in self._providers.values():
                for t in provider.discover():
                    key = f"{t.job}|{t.url}"
                    if key not in seen:
                        seen.add(key)
                        all_targets.append(t)

            changed = self._targets_changed(all_targets, self._current_targets)
            self._current_targets = all_targets

            if changed:
                for listener in self._listeners:
                    try:
                        listener(list(all_targets))
                    except Exception:
                        pass

            return all_targets

    @staticmethod
    def _targets_changed(a: List[ScrapeTarget], b: List[ScrapeTarget]) -> bool:
        key_a = {f"{t.job}|{t.url}" for t in a}
        key_b = {f"{t.job}|{t.url}" for t in b}
        return key_a != key_b
