"""
查询引擎模块 (Query Engine):
- 标签过滤查询
- 范围查询与即时查询
- 聚合函数: sum, avg, min, max, count
- 速率计算: rate (时间差/增量)
- 按标签分组 (group by)
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import time
import math
import re

from config import LabelSet, Sample, LabelMatcher
from tsdb import TSDB


# ==================== 聚合函数 ====================

def _agg_sum(values: List[float]) -> float:
    return sum(values)


def _agg_avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _agg_min(values: List[float]) -> float:
    return min(values) if values else 0.0


def _agg_max(values: List[float]) -> float:
    return max(values) if values else 0.0


def _agg_count(values: List[float]) -> float:
    return float(len(values))


def _agg_stddev(values: List[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


AGGREGATORS = {
    "sum": _agg_sum,
    "avg": _agg_avg,
    "mean": _agg_avg,
    "min": _agg_min,
    "max": _agg_max,
    "count": _agg_count,
    "stddev": _agg_stddev,
}


# ==================== 查询结果 ====================

@dataclass
class QueryResultItem:
    labels: LabelSet
    value: float
    timestamp: float


@dataclass
class QueryResult:
    items: List[QueryResultItem]

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def values(self) -> List[float]:
        return [item.value for item in self.items]

    def items_dict(self) -> List[Dict[str, Any]]:
        result = []
        for item in self.items:
            d = dict(item.labels.labels)
            d["__value__"] = item.value
            d["__timestamp__"] = item.timestamp
            result.append(d)
        return result


# ==================== 速率计算 ====================

def _calculate_rate(samples: List[Sample], window: float) -> Optional[float]:
    """
    计算指定时间窗口内的变化速率 (per second)
    适用于 counter 类型指标
    """
    if len(samples) < 2:
        return None

    now = samples[-1].timestamp
    cutoff = now - window

    first = None
    for s in samples:
        if s.timestamp >= cutoff:
            first = s
            break
    if first is None or first is samples[-1]:
        return None

    dt = samples[-1].timestamp - first.timestamp
    if dt <= 0:
        return None

    dv = samples[-1].value - first.value
    if dv < 0:
        dv = samples[-1].value

    return dv / dt


# ==================== 核心查询引擎 ====================

class QueryEngine:
    """查询引擎: 对外提供统一查询接口"""

    def __init__(self, tsdb: TSDB):
        self._tsdb = tsdb

    # ===== 1. 即时查询 (Instant Query): 获取指定时刻的最新值 =====

    def instant_query(self, metric_name: str,
                      label_matchers: Optional[List[LabelMatcher]] = None,
                      at: Optional[float] = None) -> QueryResult:
        """
        查询匹配条件的所有时间序列在指定时刻的最新样本
        """
        at = at or time.time()
        matchers = self._build_matchers(metric_name, label_matchers)
        keys = self._tsdb.match_series(matchers)

        items = []
        for key in keys:
            ts = self._tsdb.get_series(key)
            if not ts:
                continue
            latest = ts.latest()
            if latest and latest.timestamp <= at:
                items.append(QueryResultItem(
                    labels=ts.labels,
                    value=latest.value,
                    timestamp=latest.timestamp
                ))

        return QueryResult(items)

    # ===== 2. 范围查询 (Range Query): 获取时间范围内的所有样本 =====

    def range_query(self, metric_name: str,
                    label_matchers: Optional[List[LabelMatcher]] = None,
                    start: Optional[float] = None,
                    end: Optional[float] = None) -> Dict[str, List[Sample]]:
        """
        查询匹配条件的所有序列在 [start, end] 范围内的样本
        返回 {series_key: [Sample, ...]}
        """
        end = end or time.time()
        start = start or (end - 3600)
        matchers = self._build_matchers(metric_name, label_matchers)
        keys = self._tsdb.match_series(matchers)
        return self._tsdb.query_samples(keys, start, end)

    # ===== 3. 聚合查询: 对过滤后的序列做聚合 =====

    def aggregate(self, metric_name: str,
                  aggregator: str,
                  label_matchers: Optional[List[LabelMatcher]] = None,
                  group_by: Optional[List[str]] = None,
                  at: Optional[float] = None) -> QueryResult:
        """
        聚合查询:
          aggregator: sum/avg/min/max/count/stddev
          group_by: 按哪些标签分组 (空则聚合所有)
        """
        agg_func = AGGREGATORS.get(aggregator.lower())
        if agg_func is None:
            raise ValueError(f"Unknown aggregator: {aggregator}. "
                             f"Available: {list(AGGREGATORS.keys())}")

        instant = self.instant_query(metric_name, label_matchers, at)
        at = at or time.time()

        groups: Dict[Tuple[str, ...], List[QueryResultItem]] = defaultdict(list)
        for item in instant:
            if group_by:
                key = tuple(item.labels.get(g) or "" for g in group_by)
            else:
                key = ()
            groups[key].append(item)

        result_items = []
        for gkey, members in groups.items():
            values = [m.value for m in members]
            agg_value = agg_func(values)

            if group_by:
                labels_dict = {group_by[i]: gkey[i] for i in range(len(group_by))}
            else:
                labels_dict = {"__name__": metric_name}

            result_items.append(QueryResultItem(
                labels=LabelSet(labels_dict),
                value=agg_value,
                timestamp=at
            ))

        return QueryResult(result_items)

    # ===== 4. 速率查询: rate() =====

    def rate(self, metric_name: str,
             label_matchers: Optional[List[LabelMatcher]] = None,
             window: float = 300.0,
             at: Optional[float] = None) -> QueryResult:
        """
        计算每序列的变化速率 (per second)
        window: 回看时间窗口 (秒)
        """
        at = at or time.time()
        start = at - window
        matchers = self._build_matchers(metric_name, label_matchers)
        keys = self._tsdb.match_series(matchers)

        items = []
        for key in keys:
            ts = self._tsdb.get_series(key)
            if not ts:
                continue
            samples = ts.range(start, at)
            r = _calculate_rate(samples, window)
            if r is not None:
                items.append(QueryResultItem(
                    labels=ts.labels,
                    value=r,
                    timestamp=at
                ))

        return QueryResult(items)

    # ===== 5. increase(): 窗口内的增量值 =====

    def increase(self, metric_name: str,
                 label_matchers: Optional[List[LabelMatcher]] = None,
                 window: float = 300.0,
                 at: Optional[float] = None) -> QueryResult:
        """窗口内的总增量"""
        at = at or time.time()
        start = at - window
        matchers = self._build_matchers(metric_name, label_matchers)
        keys = self._tsdb.match_series(matchers)

        items = []
        for key in keys:
            ts = self._tsdb.get_series(key)
            if not ts:
                continue
            samples = ts.range(start, at)
            r = _calculate_rate(samples, window)
            if r is not None:
                dt = samples[-1].timestamp - samples[0].timestamp if len(samples) >= 2 else window
                items.append(QueryResultItem(
                    labels=ts.labels,
                    value=r * max(dt, 1.0),
                    timestamp=at
                ))

        return QueryResult(items)

    # ===== 6. 简单的 DSL 解析 =====

    def parse_and_query(self, expression: str,
                        at: Optional[float] = None) -> QueryResult:
        """
        解析简易查询表达式并执行:
        格式:
          - 简单查询: http_requests_total{job="api", status=~"5.."}
          - 聚合: sum(http_requests_total{job="api"}) by (status)
          - 速率: rate(http_requests_total{job="api"}[5m])
          - 聚合+速率: sum(rate(http_requests_total[5m])) by (job)
        """
        expression = expression.strip()

        agg_match = self._match_aggregation(expression)
        if agg_match:
            return self._execute_agg_expr(agg_match, at)

        rate_match = self._match_rate_increase(expression)
        if rate_match:
            return self._execute_rate_expr(rate_match, at)

        return self._execute_simple_query(expression, at)

    # ===== 辅助方法 =====

    @staticmethod
    def _build_matchers(metric_name: str,
                        extra: Optional[List[LabelMatcher]]) -> List[LabelMatcher]:
        matchers = [LabelMatcher(name="__name__", value=metric_name, operator="=")]
        if extra:
            matchers.extend(extra)
        return matchers

    _AGG_RE = re.compile(
        r'^(sum|avg|mean|min|max|count|stddev)\s*\(\s*(.+?)\s*\)\s*(?:by\s*\(\s*([^)]*)\s*\))?$',
        re.IGNORECASE
    )
    _RATE_RE = re.compile(
        r'^(rate|increase)\s*\(\s*(.+?)\s*\[\s*(\d+)([smhd])\s*\]\s*\)$',
        re.IGNORECASE
    )
    _SIMPLE_RE = re.compile(
        r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{([^}]*)\})?$'
    )

    def _match_aggregation(self, expr: str) -> Optional[Dict]:
        m = self._AGG_RE.match(expr)
        if not m:
            return None
        agg_name = m.group(1).lower()
        inner = m.group(2).strip()
        group_by_str = m.group(3)
        group_by = []
        if group_by_str:
            group_by = [g.strip() for g in group_by_str.split(",") if g.strip()]
        return {"agg": agg_name, "inner": inner, "group_by": group_by}

    def _match_rate_increase(self, expr: str) -> Optional[Dict]:
        m = self._RATE_RE.match(expr)
        if not m:
            return None
        func = m.group(1).lower()
        inner = m.group(2).strip()
        amount = int(m.group(3))
        unit = m.group(4)
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        window = amount * multipliers.get(unit, 1)
        return {"func": func, "inner": inner, "window": window}

    def _parse_simple_selector(self, expr: str) -> Tuple[str, List[LabelMatcher]]:
        """解析 metric_name{label1="val1",...} 格式"""
        m = self._SIMPLE_RE.match(expr)
        if not m:
            raise ValueError(f"Invalid metric selector: {expr}")
        name = m.group(1)
        labels_str = m.group(2)

        matchers = []
        if labels_str:
            label_re = re.compile(
                r'(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*'
                r'(?P<op>=~|!~|!=|=)\s*'
                r'"(?P<value>(?:[^"\\]|\\.)*)"'
            )
            for lm in label_re.finditer(labels_str):
                matchers.append(LabelMatcher(
                    name=lm.group("name"),
                    value=lm.group("value").replace('\\"', '"').replace('\\\\', '\\'),
                    operator=lm.group("op")
                ))
        return name, matchers

    def _execute_simple_query(self, expr: str, at: Optional[float]) -> QueryResult:
        name, matchers = self._parse_simple_selector(expr)
        return self.instant_query(name, matchers, at)

    def _execute_rate_expr(self, parsed: Dict, at: Optional[float]) -> QueryResult:
        name, matchers = self._parse_simple_selector(parsed["inner"])
        if parsed["func"] == "rate":
            return self.rate(name, matchers, parsed["window"], at)
        else:
            return self.increase(name, matchers, parsed["window"], at)

    def _execute_agg_expr(self, parsed: Dict, at: Optional[float]) -> QueryResult:
        agg_name = parsed["agg"]
        inner = parsed["inner"]
        group_by = parsed["group_by"]

        rate_match = self._match_rate_increase(inner)
        if rate_match:
            rate_result = self._execute_rate_expr(rate_match, at)
            return self._aggregate_result_items(
                rate_result.items, agg_name, group_by, at
            )

        name, matchers = self._parse_simple_selector(inner)
        return self.aggregate(name, agg_name, matchers, group_by, at)

    def _aggregate_result_items(self, items: List[QueryResultItem],
                                agg_name: str,
                                group_by: Optional[List[str]],
                                at: Optional[float]) -> QueryResult:
        agg_func = AGGREGATORS.get(agg_name.lower())
        if agg_func is None:
            raise ValueError(f"Unknown aggregator: {agg_name}")
        at = at or time.time()

        groups: Dict[Tuple[str, ...], List[QueryResultItem]] = defaultdict(list)
        for item in items:
            if group_by:
                key = tuple(item.labels.get(g) or "" for g in group_by)
            else:
                key = ()
            groups[key].append(item)

        result_items = []
        for gkey, members in groups.items():
            values = [m.value for m in members]
            agg_value = agg_func(values)
            if group_by:
                labels_dict = {group_by[i]: gkey[i] for i in range(len(group_by))}
            else:
                labels_dict = {}
            result_items.append(QueryResultItem(
                labels=LabelSet(labels_dict),
                value=agg_value,
                timestamp=at
            ))

        return QueryResult(result_items)
