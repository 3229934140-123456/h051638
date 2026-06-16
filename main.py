"""
主入口程序: 集成所有模块并提供完整演示
- 启动模拟目标端点 (HTTP server 暴露指标)
- 装配系统各模块
- 演示场景: 动态发现 -> 抓取 -> 查询 -> 告警 -> 通知
"""
import sys
import time
import threading
import random
import socket
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

from config import (
    Config, ScrapeTarget, AlertRule, LabelMatcher, NotificationRoute,
    LabelSet, Sample, Metric,
)
from tsdb import TSDB
from scraper import ScraperManager, parse_prometheus_text, format_prometheus_text
from discovery import (
    DiscoveryManager, StaticTargetProvider, FileTargetProvider, DNSTargetProvider,
)
from query import QueryEngine
from alerter import AlertManager
from notifier import (
    Notifier, ConsoleChannel, WebhookChannel, EmailChannel,
)


# ==================== 模拟目标端点 ====================

class SimulatedMetricsState:
    """模拟目标的指标状态"""

    def __init__(self, service_name: str, instance_id: str):
        self.service_name = service_name
        self.instance_id = instance_id
        self._start = time.time()
        self._request_count = 0
        self._error_count = 0
        self._cpu_usage = random.uniform(10, 40)
        self._memory_usage = random.uniform(30, 60)
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._request_count += random.randint(5, 50)
            self._error_count += random.choice([0, 0, 0, 0, 1, 2])
            self._cpu_usage += random.uniform(-3, 3)
            self._cpu_usage = max(5, min(95, self._cpu_usage))
            self._memory_usage += random.uniform(-1, 1)
            self._memory_usage = max(10, min(95, self._memory_usage))

    def get_metrics(self) -> str:
        with self._lock:
            now = time.time()
            uptime = now - self._start
            metrics = [
                Metric(
                    name="http_requests_total",
                    labels=LabelSet({
                        "service": self.service_name,
                        "instance": self.instance_id,
                        "method": "GET",
                        "status": "200",
                    }),
                    sample=Sample(timestamp=now, value=float(self._request_count * 0.9)),
                ),
                Metric(
                    name="http_requests_total",
                    labels=LabelSet({
                        "service": self.service_name,
                        "instance": self.instance_id,
                        "method": "POST",
                        "status": "200",
                    }),
                    sample=Sample(timestamp=now, value=float(self._request_count * 0.1)),
                ),
                Metric(
                    name="http_requests_total",
                    labels=LabelSet({
                        "service": self.service_name,
                        "instance": self.instance_id,
                        "method": "GET",
                        "status": "500",
                    }),
                    sample=Sample(timestamp=now, value=float(self._error_count)),
                ),
                Metric(
                    name="process_cpu_usage_percent",
                    labels=LabelSet({
                        "service": self.service_name,
                        "instance": self.instance_id,
                    }),
                    sample=Sample(timestamp=now, value=self._cpu_usage),
                ),
                Metric(
                    name="process_memory_usage_percent",
                    labels=LabelSet({
                        "service": self.service_name,
                        "instance": self.instance_id,
                    }),
                    sample=Sample(timestamp=now, value=self._memory_usage),
                ),
                Metric(
                    name="process_uptime_seconds",
                    labels=LabelSet({
                        "service": self.service_name,
                        "instance": self.instance_id,
                    }),
                    sample=Sample(timestamp=now, value=uptime),
                ),
            ]
            return format_prometheus_text(metrics)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/metrics"):
            state: SimulatedMetricsState = self.server.metrics_state
            state.tick()
            body = state.get_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


def find_free_port(start: int = 9100, max_tries: int = 50) -> int:
    for port in range(start, start + max_tries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
            return port
        except OSError:
            continue
    raise RuntimeError("No free port available")


class SimulatedTargetServer:
    """封装一个模拟的目标服务"""

    def __init__(self, service_name: str, instance_id: str):
        self.service_name = service_name
        self.instance_id = instance_id
        self.port = find_free_port()
        self.state = SimulatedMetricsState(service_name, instance_id)
        self._server = None
        self._thread = None

    @property
    def metrics_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/metrics"

    def start(self):
        self._server = HTTPServer(("127.0.0.1", self.port), MetricsHandler)
        self._server.metrics_state = self.state
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._thread.join(timeout=2)

    def spike_cpu(self, amount: float = 50):
        """模拟 CPU 飙升（触发告警用）"""
        with self.state._lock:
            self.state._cpu_usage += amount
            self.state._cpu_usage = min(99, self.state._cpu_usage)

    def spike_errors(self, count: int = 100):
        """模拟错误请求飙升"""
        with self.state._lock:
            self.state._error_count += count


# ==================== 系统装配类 ====================

class MonitoringSystem:
    """完整监控系统: 装配所有模块"""

    def __init__(self, config: Config):
        self.config = config
        self.tsdb = TSDB(retention_period=config.tsdb_retention_period)
        self.query_engine = QueryEngine(self.tsdb)
        self.scraper_manager = ScraperManager(self.tsdb)
        self.discovery = DiscoveryManager(interval=config.discovery_interval)
        self.alert_manager = AlertManager(
            self.query_engine, evaluation_interval=config.evaluation_interval
        )
        self.notifier = Notifier(routes=config.routes)

        self.alert_manager.add_listener(self.notifier.handle_alerts)
        self.discovery.add_listener(self.scraper_manager.set_targets)

        self.alert_manager.set_rules(config.alert_rules)

    def start(self):
        self.discovery.start()
        self.alert_manager.start()

    def stop(self):
        self.discovery.stop()
        self.alert_manager.stop()
        self.scraper_manager.shutdown()
        self.notifier.shutdown()


# ==================== 演示主程序 ====================

def build_demo_config() -> Config:
    """构建演示用的配置"""
    return Config(
        global_scrape_interval=3.0,
        global_scrape_timeout=5.0,
        evaluation_interval=3.0,
        tsdb_retention_period=3600,
        discovery_interval=15.0,
        alert_rules=[
            AlertRule(
                name="HighCPUUsage",
                metric_name="process_cpu_usage_percent",
                condition=">",
                threshold=70.0,
                for_duration=6.0,
                labels={"severity": "warning", "team": "infra"},
                annotations={
                    "summary": "High CPU usage detected on {{$labels.instance}}",
                    "description": "CPU usage is {{$value}}%, exceeding threshold 70%",
                },
            ),
            AlertRule(
                name="HighErrorRate",
                expression="sum(rate(http_requests_total{status=\"500\"}[30s])) by (service)",
                condition=">",
                threshold=0.5,
                for_duration=5.0,
                labels={"severity": "critical", "team": "api"},
                annotations={
                    "summary": "High error rate on service {{$labels.service}}",
                    "description": "500 error rate: {{$value}} req/s (threshold: 0.5 req/s)",
                },
            ),
            AlertRule(
                name="TargetDown",
                metric_name="up",
                condition="==",
                threshold=0.0,
                for_duration=8.0,
                labels={"severity": "critical", "team": "infra"},
                annotations={
                    "summary": "Target {{$labels.job}}/{{$labels.instance}} is DOWN",
                    "description": "up metric is 0 for more than 8s",
                },
            ),
        ],
        routes=[
            NotificationRoute(
                name="critical-alerts",
                label_matchers=[LabelMatcher(name="severity", value="critical", operator="=")],
                channels=["console", "email", "webhook"],
                group_by=["alertname", "service", "severity"],
                group_wait=3.0,
                repeat_interval=60.0,
            ),
            NotificationRoute(
                name="warning-alerts",
                label_matchers=[LabelMatcher(name="severity", value="warning", operator="=")],
                channels=["console"],
                group_by=["alertname", "instance"],
                group_wait=2.0,
                repeat_interval=120.0,
            ),
        ],
    )


def main():
    print("=" * 70)
    print("  Mini Prometheus - 指标采集与告警系统 演示")
    print("=" * 70)

    # 1. 启动模拟目标端点
    print("\n[Step 1] 启动模拟目标端点...")
    api_server_1 = SimulatedTargetServer("api-service", "api-1").start()
    api_server_2 = SimulatedTargetServer("api-service", "api-2").start()
    web_server = SimulatedTargetServer("web-service", "web-1").start()

    print(f"  API-1   -> {api_server_1.metrics_url}")
    print(f"  API-2   -> {api_server_2.metrics_url}")
    print(f"  Web-1   -> {web_server.metrics_url}")

    # 2. 构建系统配置
    config = build_demo_config()

    # 3. 注册通知渠道
    console_ch = ConsoleChannel("console")
    email_ch = EmailChannel("email", "oncall@example.com")
    webhook_ch = WebhookChannel("webhook", "http://127.0.0.1:9999/hook")

    # 4. 初始化系统
    print("\n[Step 2] 初始化监控系统模块...")
    system = MonitoringSystem(config)
    system.notifier.register_channel(console_ch)
    system.notifier.register_channel(email_ch)
    system.notifier.register_channel(webhook_ch)

    static_provider = StaticTargetProvider([
        ScrapeTarget(
            job="api-service",
            url=api_server_1.metrics_url,
            scrape_interval=3.0,
            scrape_timeout=5.0,
            static_labels={"env": "demo", "region": "cn"},
        ),
        ScrapeTarget(
            job="api-service",
            url=api_server_2.metrics_url,
            scrape_interval=3.0,
            scrape_timeout=5.0,
            static_labels={"env": "demo", "region": "cn"},
        ),
        ScrapeTarget(
            job="web-service",
            url=web_server.metrics_url,
            scrape_interval=3.0,
            scrape_timeout=5.0,
            static_labels={"env": "demo", "region": "cn"},
        ),
    ])
    system.discovery.add_provider("static", static_provider)

    # 5. 启动系统
    print("\n[Step 3] 启动系统 (发现/抓取/告警评估循环)...")
    system.start()
    time.sleep(1)

    # 6. 演示各功能
    try:
        # ---------- 演示 1: 等待抓取积累数据 ----------
        print("\n[Demo 1] 等待 10 秒让抓取器积累样本...")
        for i in range(10, 0, -1):
            print(f"  ... {i} 秒剩余 (TSDB: {system.tsdb.series_count()} 序列, "
                  f"{system.tsdb.total_samples()} 样本)")
            time.sleep(1)

        # ---------- 演示 2: 目标健康状态 ----------
        print("\n[Demo 2] 目标健康状态:")
        for t in system.scraper_manager.get_targets():
            print(f"  Job: {t.job:<15}  URL: {t.url:<40}  "
                  f"Health: {'✅UP' if t.health=='up' else '❌DOWN'}  "
                  f"Scrapes: {t.scrape_count}  Errors: {t.error_count}")

        # ---------- 演示 3: 基础查询 ----------
        print("\n[Demo 3] 基础查询 - 即时查询 CPU 使用率:")
        result = system.query_engine.instant_query("process_cpu_usage_percent")
        for item in result:
            print(f"  {dict(item.labels.labels)} => {item.value:.1f}%")

        # ---------- 演示 4: DSL 查询 ----------
        print("\n[Demo 4] DSL 查询: sum(http_requests_total) by (service, status)")
        result = system.query_engine.parse_and_query(
            'sum(http_requests_total) by (service, status)'
        )
        for item in result:
            print(f"  {dict(item.labels.labels)} => {item.value:.0f}")

        # ---------- 演示 5: 速率查询 ----------
        print("\n[Demo 5] 速率查询: rate(http_requests_total{status='200'}[30s])")
        result = system.query_engine.parse_and_query(
            "rate(http_requests_total{status=\"200\"}[30s])"
        )
        for item in result:
            print(f"  {item.labels.get('instance'):<10} {item.labels.get('service'):<15} "
                  f"=> {item.value:.2f} req/s")

        # ---------- 演示 6: 触发告警 - CPU 飙升 ----------
        print("\n[Demo 6] 模拟 CPU 飙升 (api-1) -> 预期触发 HighCPUUsage 告警...")
        api_server_1.spike_cpu(60)

        for i in range(15, 0, -1):
            active = system.alert_manager.get_active_alerts()
            print(f"  ... {i}s  |  活动告警: {len(active)}  "
                  f"[{' | '.join(f'{a.rule_name}:{a.status}' for a in active)}]")
            if i == 8:
                api_server_1.spike_cpu(10)
            time.sleep(1)

        # ---------- 演示 7: 触发告警 - 错误率高 ----------
        print("\n[Demo 7] 模拟 500 错误激增 (api-2) -> 预期触发 HighErrorRate 告警...")
        api_server_2.spike_errors(200)

        for i in range(12, 0, -1):
            active = system.alert_manager.get_active_alerts()
            print(f"  ... {i}s  |  活动告警: {len(active)}  "
                  f"[{' | '.join(f'{a.rule_name}:{a.status}' for a in active)}]")
            if i == 6:
                api_server_2.spike_errors(100)
            time.sleep(1)

        # ---------- 演示 8: 关闭目标 -> TargetDown ----------
        print("\n[Demo 8] 关闭 web-service -> 预期触发 TargetDown 告警...")
        web_server.stop()

        for i in range(14, 0, -1):
            active = system.alert_manager.get_active_alerts()
            print(f"  ... {i}s  |  活动告警: {len(active)}  "
                  f"[{' | '.join(f'{a.rule_name}:{a.status}' for a in active)}]")
            time.sleep(1)

        # ---------- 演示 9: DSL 高级查询 ----------
        print("\n[Demo 9] DSL 高级查询: sum(rate(http_requests_total[30s])) by (service)")
        try:
            result = system.query_engine.parse_and_query(
                "sum(rate(http_requests_total[30s])) by (service)"
            )
            for item in result:
                print(f"  {dict(item.labels.labels)} => {item.value:.2f} req/s")
        except Exception as e:
            print(f"  查询错误 (可能无足够数据): {e}")

        # ---------- 演示 10: 动态发现 - 添加目标 ----------
        print("\n[Demo 10] 动态发现: 运行时新增 target (new-api-3)...")
        new_server = SimulatedTargetServer("api-service", "api-3").start()
        new_targets = static_provider.discover() + [
            ScrapeTarget(
                job="api-service",
                url=new_server.metrics_url,
                scrape_interval=3.0,
                scrape_timeout=5.0,
                static_labels={"env": "demo", "region": "cn"},
            ),
        ]
        static_provider.update(new_targets)
        system.discovery.refresh_now()

        for i in range(8, 0, -1):
            targets = system.scraper_manager.get_targets()
            up_count = sum(1 for t in targets if t.health == "up")
            print(f"  ... {i}s  |  目标数: {len(targets)}, UP: {up_count}, "
                  f"TSDB 序列: {system.tsdb.series_count()}")
            time.sleep(1)

        # ---------- 演示 11: 统计报告 ----------
        print("\n" + "=" * 70)
        print("  演示总结 - 系统状态")
        print("=" * 70)
        print(f"  TSDB 序列数:        {system.tsdb.series_count()}")
        print(f"  TSDB 总样本数:      {system.tsdb.total_samples()}")
        print(f"  指标名:             {', '.join(system.tsdb.all_metric_names())}")
        print(f"  目标总数:           {len(system.scraper_manager.get_targets())}")
        print(f"  活动告警数:         {len(system.alert_manager.get_active_alerts())}")
        print(f"  通知总发送次数:     {system.notifier.notified_count}")
        print(f"  模拟邮件发送数:     {email_ch.sent_count}")
        print(f"\n  活动告警详情:")
        for a in system.alert_manager.get_active_alerts():
            print(f"    [{a.status}] {a.rule_name} | {dict(a.labels.labels)} | value={a.value:.1f}")

        print("\n[完成] 所有演示场景执行完毕，正在清理...")

    except KeyboardInterrupt:
        print("\n\n[中断] 用户中断，正在清理...")

    finally:
        system.stop()
        for s in [api_server_1, api_server_2, web_server]:
            try:
                s.stop()
            except Exception:
                pass
        try:
            new_server.stop()
        except Exception:
            pass
        print("[Done] 系统已关闭。")


if __name__ == "__main__":
    main()
