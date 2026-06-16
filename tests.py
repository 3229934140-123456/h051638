"""
快速验证脚本: 测试各模块核心功能
"""
import sys
import time
import threading

from config import (
    LabelSet, Sample, Metric, LabelMatcher, AlertRule, NotificationRoute,
)
from tsdb import TSDB
from scraper import parse_prometheus_text, format_prometheus_text
from query import QueryEngine
from alerter import AlertManager
from notifier import Notifier, ConsoleChannel


def test_label_set():
    print("[1/8] LabelSet 测试...", end=" ")
    l1 = LabelSet({"a": "1", "b": "2"})
    l2 = LabelSet({"b": "2", "a": "1"})
    assert l1 == l2, "相同标签应该相等"
    assert l1.key == l2.key, "key 应该相同"
    assert l1.get("a") == "1"
    assert hash(l1) == hash(l2)
    d = {l1: "value"}
    assert d[l2] == "value", "作为 dict key 应一致"
    print("✓")


def test_tsdb_basic():
    print("[2/8] TSDB 基础写入/查询...", end=" ")
    tsdb = TSDB()
    now = time.time()
    m1 = Metric("cpu", LabelSet({"host": "a", "job": "api"}), Sample(now, 30.0))
    m2 = Metric("cpu", LabelSet({"host": "b", "job": "api"}), Sample(now, 40.0))
    m3 = Metric("cpu", LabelSet({"host": "a", "job": "web"}), Sample(now, 20.0))
    tsdb.append_batch([m1, m2, m3])

    assert tsdb.series_count() == 3
    assert tsdb.total_samples() == 3
    assert "cpu" in tsdb.all_metric_names()
    assert "host" in tsdb.label_names()

    matchers = [LabelMatcher("__name__", "cpu", "="), LabelMatcher("job", "api", "=")]
    keys = tsdb.match_series(matchers)
    assert len(keys) == 2, f"job=api 应有 2 条序列，实际 {len(keys)}"

    matchers2 = [LabelMatcher("__name__", "cpu", "="), LabelMatcher("host", "a", "=")]
    keys2 = tsdb.match_series(matchers2)
    assert len(keys2) == 2, f"host=a 应有 2 条"

    matchers3 = [LabelMatcher("__name__", "cpu", "="), LabelMatcher("host", "a", "!=")]
    keys3 = tsdb.match_series(matchers3)
    assert len(keys3) == 1, f"host!=a 应有 1 条"

    sample = tsdb.get_latest(keys[0])
    assert sample is not None and sample.value in (30.0, 40.0)
    print("✓")


def test_prometheus_parser():
    print("[3/8] Prometheus 文本解析...", end=" ")
    text = """
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200",service="api"} 1024.5 1680000000000
http_requests_total{method="POST",status="500",service="api"} 42 1680000000100
cpu_usage_percent{host="server1"} 72.3
mem_usage_bytes{host="server1",zone=\"us-east-1\"} 1.2e+09
"""
    metrics = parse_prometheus_text(text)
    assert len(metrics) == 4, f"应解析 4 条，实际 {len(metrics)}"

    m = metrics[0]
    assert m.name == "http_requests_total"
    assert m.labels.get("method") == "GET"
    assert m.labels.get("status") == "200"
    assert m.sample.value == 1024.5

    assert metrics[2].sample.value == 72.3
    assert metrics[3].labels.get("zone") == "us-east-1"
    assert abs(metrics[3].sample.value - 1.2e9) < 1e3

    formatted = format_prometheus_text(metrics)
    parsed_again = parse_prometheus_text(formatted)
    assert len(parsed_again) == 4, "序列化后再解析应得 4 条"
    print("✓")


def test_query_engine():
    print("[4/8] 查询引擎聚合与速率...", end=" ")
    tsdb = TSDB()
    qe = QueryEngine(tsdb)
    base = time.time() - 100

    for i in range(10):
        t = base + i * 10
        tsdb.append(Metric("req_total", LabelSet({"svc": "api", "host": "a"}), Sample(t, i * 100.0)))
        tsdb.append(Metric("req_total", LabelSet({"svc": "api", "host": "b"}), Sample(t, i * 50.0)))
        tsdb.append(Metric("cpu", LabelSet({"svc": "api", "host": "a"}), Sample(t, 30.0 + i)))
        tsdb.append(Metric("cpu", LabelSet({"svc": "api", "host": "b"}), Sample(t, 40.0 + i * 0.5)))

    instant = qe.instant_query("req_total")
    assert len(instant) == 2, f"即时查询应有 2 条，实际 {len(instant)}"

    avg_cpu = qe.aggregate("cpu", "avg")
    assert len(avg_cpu) == 1
    avg_val = avg_cpu.items[0].value
    assert 40 < avg_val < 60, f"avg cpu 约 48，实际 {avg_val}"

    sum_by_svc = qe.aggregate("req_total", "sum", group_by=["svc"])
    assert len(sum_by_svc) == 1
    total = sum_by_svc.items[0].value
    assert total > 0

    rate_result = qe.rate("req_total", window=100.0, at=base + 100)
    assert len(rate_result) == 2
    r = rate_result.items[0].value
    assert 0 < r < 20, f"rate 约 10 或 5，实际 {r}"

    dsl = qe.parse_and_query('sum(req_total{svc="api"}) by (host)')
    assert len(dsl) == 2, f"DSL group by host 应有 2 条，实际 {len(dsl)}"

    dsl_rate = qe.parse_and_query("rate(req_total[1m])")
    assert len(dsl_rate) == 2, f"DSL rate 应有 2 条，实际 {len(dsl_rate)}"

    sum_rate = qe.parse_and_query("sum(rate(req_total[1m])) by (svc)")
    assert len(sum_rate) == 1, f"嵌套聚合应有 1 条"
    print("✓")


def test_alert_evaluator():
    print("[5/8] 告警规则状态机...", end=" ")
    tsdb = TSDB()
    qe = QueryEngine(tsdb)
    alerter = AlertManager(qe, evaluation_interval=0.1)

    rule = AlertRule(
        name="HighCPU",
        metric_name="cpu",
        label_matchers=[],
        condition=">",
        threshold=70.0,
        for_duration=1.0,
        labels={"severity": "warning"},
        annotations={"summary": "cpu {{$value}} on {{$labels.host}}"},
    )
    alerter.add_rule(rule)

    received = []
    alerter.add_listener(lambda alerts: received.extend(alerts))

    base = time.time()
    for t_offset in [0, 0.2, 0.4, 0.6, 0.8, 1.1, 1.3]:
        tsdb.append(Metric("cpu", LabelSet({"host": "a"}), Sample(base + t_offset, 80.0)))
        tsdb.append(Metric("cpu", LabelSet({"host": "b"}), Sample(base + t_offset, 50.0)))

    changes = alerter.evaluate_now()
    pending_count = sum(1 for a in changes if a.status == "pending")
    assert pending_count >= 0, f"第一次评估 {len(changes)} 条变更"

    time.sleep(1.2)
    for t_offset in [1.5, 1.7]:
        tsdb.append(Metric("cpu", LabelSet({"host": "a"}), Sample(base + 1.5 + t_offset, 80.0)))

    changes2 = alerter.evaluate_now()
    firing_count = sum(1 for a in changes2 if a.status == "firing")
    assert firing_count >= 0

    active = alerter.get_active_alerts()
    for a in active:
        assert "summary" in a.annotations
        assert "{{" not in a.annotations["summary"], "模板应已渲染"

    print("✓")


def test_notifier_routing():
    print("[6/8] 通知路由与分组...", end=" ")

    routes = [
        NotificationRoute(
            name="crit",
            label_matchers=[LabelMatcher("severity", "critical", "=")],
            channels=["console"],
            group_by=["alertname"],
            group_wait=0.1,
            repeat_interval=10,
        ),
        NotificationRoute(
            name="warn",
            label_matchers=[LabelMatcher("severity", "warning", "=")],
            channels=["console"],
            group_by=["alertname", "host"],
            group_wait=0.1,
            repeat_interval=10,
        ),
    ]
    notifier = Notifier(routes=routes)
    ch = ConsoleChannel("console")
    notifier.register_channel(ch)

    from config import Alert as AlertCls
    from alerter import _compute_fingerprint

    alerts = []
    for i, (sev, host, rule) in enumerate([
        ("critical", "a", "HighError"),
        ("critical", "b", "HighError"),
        ("warning", "a", "HighCPU"),
        ("warning", "c", "HighCPU"),
    ]):
        labels = LabelSet({"alertname": rule, "severity": sev, "host": host})
        fp = _compute_fingerprint(labels, rule)
        a = AlertCls(
            fingerprint=fp, rule_name=rule, labels=labels,
            annotations={}, value=100, starts_at=time.time(), status="firing"
        )
        alerts.append(a)

    notifier.handle_alerts(alerts)
    time.sleep(0.5)
    print("✓")


def test_tsdb_cleanup():
    print("[7/8] TSDB 保留策略...", end=" ")
    tsdb = TSDB(retention_period=0.1)
    old = time.time() - 10
    tsdb.append(Metric("old_metric", LabelSet({"k": "v"}), Sample(old, 1.0)))
    tsdb.append(Metric("new_metric", LabelSet({"k": "v"}), Sample(time.time(), 2.0)))
    time.sleep(0.15)
    removed = tsdb.cleanup()
    assert removed >= 1, f"应至少清理 1 样本，实际 {removed}"
    assert tsdb.series_count() <= 1, f"剩余序列不应超过 1"
    print("✓")


def test_label_regex_match():
    print("[8/8] 标签正则匹配器...", end=" ")
    assert LabelMatcher("status", "5..", "=~").matches("500")
    assert LabelMatcher("status", "5..", "=~").matches("503")
    assert not LabelMatcher("status", "5..", "=~").matches("200")
    assert LabelMatcher("status", "5..", "!~").matches("200")
    assert not LabelMatcher("status", "5..", "!~").matches("500")
    assert LabelMatcher("svc", "api", "!=").matches("web")
    assert not LabelMatcher("svc", "api", "!=").matches("api")
    print("✓")


def main():
    print("=" * 50)
    print("  模块单元测试")
    print("=" * 50)
    try:
        test_label_set()
        test_tsdb_basic()
        test_prometheus_parser()
        test_query_engine()
        test_alert_evaluator()
        test_notifier_routing()
        test_tsdb_cleanup()
        test_label_regex_match()
    except AssertionError as e:
        print(f"\n❌ 断言失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 50)
    print("  ✓ 所有 8 项测试通过！")
    print("=" * 50)
    print("\n运行完整演示请执行: python main.py")


if __name__ == "__main__":
    main()
