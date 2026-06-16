# Mini Prometheus - 精简版指标采集与告警系统

一个从零实现的类 Prometheus 监控系统核心原型，采用 Python 标准库（无需第三方依赖）。

## 项目结构

```
.
├── config.py       # 配置与数据模型 (LabelSet, Sample, Metric, AlertRule 等)
├── tsdb.py         # 时序存储 (内存 TSDB + 倒排索引 + 保留策略)
├── scraper.py      # 抓取器 (Prometheus 文本解析 + 定期调度)
├── discovery.py    # 目标动态发现 (Static/File/DNS Provider)
├── query.py        # 查询引擎 (标签过滤 + 聚合 + 速率 + DSL 解析)
├── alerter.py      # 告警评估 (状态机 + for_duration 持续触发)
├── notifier.py     # 通知模块 (去重 + 分组 + 路由 + 多渠道)
└── main.py         # 演示入口 (模拟目标 + 10+ 演示场景)
```

## 运行演示

```bash
python main.py
```

演示内容约 90 秒完成，包含：
1. 启动 3 个模拟目标端点（API x2, Web x1）
2. 系统初始化与模块装配
3. 等待抓取积累数据
4. 查询演示（即时查询 / DSL / 速率）
5. 触发 CPU 告警 / 错误率告警 / 目标宕机告警
6. 动态发现新增目标
7. 最终系统状态报告

---

## 模块设计说明

### 1. 抓取器 (Scraper) — `scraper.py`

#### 工作原理

系统为每个 `ScrapeTarget` 启动独立的后台线程 (`_scrape_loop`)，按配置的 `scrape_interval` 周期性执行：

```
scrape_interval 到期
    │
    ▼
urllib.request.urlopen(url, timeout=scrape_timeout)
    │
    ├─ 成功 → parse_prometheus_text() 解析文本 → 附加静态标签 → TSDB.append_batch()
    │        → target.mark_healthy(duration) → 写入 up=1, scrape_duration 等元指标
    │
    └─ 失败 → target.mark_unhealthy(error) → 写入 up=0
```

#### Prometheus 文本格式解析

正则 `_METRIC_LINE_RE` 匹配：
```
metric_name{label1="val1", label2="val2"} 123.45 1680000000000
└──┬───────┘ └────────┬───────────────┘ └─┬─┘ └────┬──────┘
   name              labels              value  timestamp(ms, 可选)
```

支持特殊值：`NaN`、`+Inf`、`-Inf`。标签值的转义：`\"`→`"`、`\\`→`\`、`\n`→换行。

#### 目标健康状态 (`ScrapeTarget`)

| 字段 | 含义 |
|------|------|
| `health` | `unknown` / `up` / `down` |
| `up` 指标 | 每次抓取后写入 TSDB，值为 1(成功) 或 0(失败) |
| `last_error` | 最近一次失败原因 |
| `error_count` | 累计失败次数 |

查询 `up == 0` 可配合告警规则实现「目标宕机告警」。

---

### 2. 时序存储 (TSDB) — `tsdb.py`

#### 存储模型

每个**带完整标签的指标**是一条独立的 `TimeSeries`，用有序列表存储 `Sample`：

```
TimeSeries(labels={__name__: "cpu_usage", instance: "a", job: "api"})
  └── samples: [ (t0, 30.1), (t1, 31.5), (t2, 33.8), ... ]
```

全局索引（`TSDB._series`）：`series_key → TimeSeries`

`series_key` 由 `LabelSet.key` 生成：标签按 key 排序后序列化为 `k1=v1|k2=v2|...`，保证同一组标签得到相同 key。

#### 倒排索引 (`_label_index`)

```
_label_index[label_name][label_value] = { series_key_1, series_key_2, ... }
```

查询时先通过倒排索引定位候选集合，再做交集（多标签过滤时），避免全量遍历。

**示例查询**：`process_cpu_usage_percent{job="api", env=~"prod|staging"}`
1. `__name__="process_cpu_usage_percent"` → 集合 A
2. `job="api"` → 集合 B
3. `env=~"prod|staging"` → 遍历 env 下所有 value 做正则匹配取并集 → 集合 C
4. 结果 = A ∩ B ∩ C

#### 保留策略

每次写入时检查距上次清理是否超过 60 秒。`cleanup()` 删除所有 `timestamp < now - retention_period` 的样本，并清理空序列对应的倒排索引条目。

---

### 3. 查询引擎 (Query Engine) — `query.py`

#### 查询类型

| 方法 | 用途 |
|------|------|
| `instant_query(name, matchers, at)` | 即时查询：匹配序列在 at 时刻的最新值 |
| `range_query(name, matchers, start, end)` | 范围查询：匹配序列在时间窗口内的全部样本 |
| `aggregate(name, agg, matchers, group_by)` | 聚合：sum/avg/min/max/count/stddev，支持分组 |
| `rate(name, matchers, window)` | 速率：窗口内 per-second 变化率 |
| `increase(name, matchers, window)` | 增量：窗口内总增量 = rate × 窗口时长 |

#### DSL 语法 (`parse_and_query`)

```
1) 简单查询:
   http_requests_total{job="api", status=~"5..", method!="OPTIONS"}

2) 聚合查询:
   sum(http_requests_total) by (service, status)
   avg(process_cpu_usage_percent{env="prod"}) by (instance)
   count(up)

3) 速率/增量:
   rate(http_requests_total[5m])       # 最近 5 分钟平均速率
   increase(http_requests_total[1h])   # 最近 1 小时总增量

4) 组合:
   sum(rate(http_requests_total[2m])) by (service)
```

支持操作符：`=` / `!=` / `=~` (正则匹配) / `!~` (正则不匹配)

#### 聚合函数实现

```python
AGGREGATORS = {
    "sum":   sum(values),
    "avg":   sum/len,
    "min":   min(values),
    "max":   max(values),
    "count": len(values),
    "stddev": sqrt(E[X²]-E[X]²),
}
```

`group_by` 为空时所有匹配序列合并为一个值；否则按指定标签取 tuple 作为分组 key。

#### 速率计算 (`_calculate_rate`)

取窗口内**第一个**和**最后一个**样本：
```
rate = (last_value - first_value) / (last_ts - first_ts)
```
Counter 重置处理：若 `dv < 0`，视为计数器重置，取 `last_value` 作为增量（简化版）。

---

### 4. 告警评估 (Alert Evaluator) — `alerter.py`

#### 告警规则 (`AlertRule`)

| 字段 | 含义 |
|------|------|
| `metric_name` | 目标指标名 |
| `label_matchers` | 标签过滤条件 |
| `condition` + `threshold` | 触发条件（如 `>` + 70.0） |
| `for_duration` | 持续满足多少秒才触发（防抖动） |
| `labels` | 告警附加标签（用于路由） |
| `annotations` | 告警描述模板（支持 `{{$labels.xxx}}` / `{{$value}}`） |

#### 状态机 (inactive → pending → firing → resolved)

```
              条件满足                      for_duration 到达
inactive ─────────────────► pending ────────────────────────► firing
   ▲                           │                                │
   │                           │ 条件不满足                      │ 条件不满足
   └───────────────────────────┴────────────────────────────────┘
                    resolved（会发出恢复通知）
```

`AlertState.update()` 每次评估返回事件类型：`pending` / `firing` / `resolved` / `None`。

#### 评估循环

每隔 `evaluation_interval`（默认 15s），`_do_evaluation()` 遍历所有规则：
1. 对规则的 `metric_name + label_matchers` 执行 `instant_query`
2. 对结果中**每个序列**（独立标签组合）单独跟踪状态（独立 fingerprint）
3. 状态变化时，通过 listener 回调推送给 Notifier

**Fingerprint**：`sha256(rule_name | labels_key)[:16]`，保证相同规则下同标签序列状态一致。

---

### 5. 通知模块 (Notifier) — `notifier.py`

#### 4 层处理：路由 → 分组 → 延迟聚合 → 去重/重复控制

```
AlertManager 推送的告警列表
        │
        ▼
① 路由匹配 (_match_routes):
   遍历所有 NotificationRoute，若 route.label_matchers 全匹配告警标签 → 纳入此路由
   无匹配路由时: 若配置了路由则丢弃，否则走默认 console

        │  每条告警 × 每个匹配路由 × 每个 channel
        ▼
② 计算组键 (_compute_group_key):
   按 route.group_by 标签拼接，如 alertname=HighCPU|instance=api-1

        │
        ▼
③ 延迟聚合 (group_wait):
   同一 (route, channel, group_key) 的告警在 group_wait 秒内被聚合
   避免短时间大量同类告警分别发送

        │  group_wait 定时器到期 (_flush_group)
        ▼
④ 去重与重复控制:
   - resolved 告警: 若发送过则立即发（通知恢复）
   - firing 告警:   距上次发送 >= repeat_interval 才发送
   记录 timestamp_key = (route, channel, fingerprint) → 上次发送时间
```

#### 通知渠道

| 渠道 | 实现 |
|------|------|
| `ConsoleChannel` | 格式打印到 stdout，带状态图标 |
| `WebhookChannel` | POST JSON 到配置 URL |
| `EmailChannel` | 模拟发送，记录到 `_sent_log` |

扩展新渠道：继承 `NotificationChannel` 实现 `send()` 即可。

#### 通知路由示例

```python
# 严重告警: 所有渠道发送
NotificationRoute(
    name="critical-alerts",
    label_matchers=[LabelMatcher("severity", "critical", "=")],
    channels=["console", "email", "webhook"],
    group_by=["alertname", "service"],   # 按告警名+服务分组
    group_wait=30.0,                       # 30秒内同类聚合
    repeat_interval=300.0,                 # 5分钟内不重复
)
```

---

### 6. 目标动态发现 (Discovery) — `discovery.py`

#### Provider 模型

```
TargetProvider (抽象基类)
    ├── StaticTargetProvider     # 内存静态列表
    ├── FileTargetProvider       # 读取 JSON 文件（按 mtime 感知变化）
    └── DNSTargetProvider        # 基于 DNS 的服务发现（示例）
```

`DiscoveryManager` 组合多个 Provider，每隔 `discovery_interval` 调用所有 `discover()`，合并去重后通过 listener 回调推送给 `ScraperManager.set_targets()`。

`ScraperManager.set_targets()` 会：
- 对新目标：启动抓取线程
- 对消失目标：停止抓取线程（set stop event → join）
- 对保留目标：无操作

**文件发现格式**（JSON）：
```json
[
  {"job": "api", "url": "http://host1:8080/metrics",
   "scrape_interval": 15, "labels": {"env": "prod"}},
  ...
]
```

---

## 数据流总览

```
┌─────────────┐   scrape_interval    ┌─────────────┐    /metrics    ┌───────────┐
│   Target    │◄─────────────────────│   Scraper   │◄───────────────│  目标端点  │
│  Discovery  │    set_targets()     │   Manager   │                │ (HTTP服务) │
└──────┬──────┘                      └──────┬──────┘                └───────────┘
       │                                    │  解析后带标签的样本 []Metric
       │                                    ▼
       │                           ┌─────────────────┐
       │                           │       TSDB      │
       │                           │ 倒排索引 + 序列  │
       │                           └───────┬─────────┘
       │                                   │
       │              instant_query/range  │  aggregate/rate
       │                                   ▼
       │                           ┌─────────────────┐
       │                           │  Query Engine   │◄──── 用户 DSL / 告警规则
       │                           └───────┬─────────┘
       │                                   │  evaluation_interval
       │                                   ▼
       │                           ┌─────────────────┐
       │                           │   AlertManager  │ 状态机 + for_duration
       │                           └───────┬─────────┘
       │                                   │  状态变化的 Alert[]
       │                                   ▼
       │                           ┌─────────────────┐
       └──────────────────────────►│    Notifier     │ 路由+分组+去重
                                   └───────┬─────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
              Console               Webhook POST               Email
```

## 与真实 Prometheus 的差异（简化点）

| 特性 | 真实 Prometheus | 本实现 |
|------|----------------|--------|
| 存储 | 本地 SSD 列式存储 (chunks)，支持 WAL | 内存列表，进程结束丢失 |
| 查询语言 | 完整 PromQL（30+ 函数、子查询、二进制运算） | 7 种核心函数 + 简易 DSL |
| 服务发现 | 20+ 种 (K8s、Consul、EC2...) | Static / File / DNS(示例) |
| 告警路由 | Alertmanager 独立进程，含静默/抑制/继承树 | 单进程，支持基础路由分组 |
| 抓取指标 | histogram/summary 类型完整支持 | counter/gauge 类通用存储 |
| 单样本复杂度 | O(1) 追加 chunk | O(log n) 有序列表追加 |

可作为教学原型理解 Prometheus 核心机制，生产环境请使用真实 Prometheus。
