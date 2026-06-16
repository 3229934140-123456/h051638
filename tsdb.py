"""
时序存储模块 (TSDB): 内存型时序数据库
- 存储带标签的时间序列
- 倒排索引支持标签快速查询
- 数据保留策略
- 范围查询与样本获取
"""
from typing import Dict, List, Optional, Iterator, Tuple
from collections import defaultdict
import time
import threading

from config import LabelSet, Sample, Metric


class TimeSeries:
    """单个时间序列: 存储一组样本点"""

    def __init__(self, labels: LabelSet, max_samples: int = 100000):
        self.labels = labels
        self.samples: List[Sample] = []
        self._lock = threading.Lock()
        self._max_samples = max_samples

    def append(self, sample: Sample) -> None:
        with self._lock:
            if self.samples and sample.timestamp <= self.samples[-1].timestamp:
                return
            self.samples.append(sample)
            if len(self.samples) > self._max_samples:
                self.samples = self.samples[-self._max_samples:]

    def range(self, start: float, end: float) -> List[Sample]:
        with self._lock:
            result = []
            for s in self.samples:
                if start <= s.timestamp <= end:
                    result.append(s)
            return result

    def latest(self) -> Optional[Sample]:
        with self._lock:
            return self.samples[-1] if self.samples else None

    def cleanup(self, before: float) -> int:
        with self._lock:
            original_len = len(self.samples)
            while self.samples and self.samples[0].timestamp < before:
                self.samples.pop(0)
            return original_len - len(self.samples)

    def __len__(self) -> int:
        return len(self.samples)


class TSDB:
    """时序数据库: 管理所有时间序列"""

    def __init__(self, retention_period: float = 3600 * 24):
        self._retention = retention_period
        self._series: Dict[str, TimeSeries] = {}
        self._series_by_labels: Dict[LabelSet, str] = {}
        self._label_index: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
        self._lock = threading.RLock()
        self._last_cleanup = time.time()

    # ===== 写入接口 =====

    def append(self, metric: Metric) -> None:
        self._maybe_cleanup()
        key = metric.series_key

        with self._lock:
            ts = self._series.get(key)
            if ts is None:
                full_labels = metric.full_labels
                ts = TimeSeries(full_labels)
                self._series[key] = ts
                self._series_by_labels[full_labels] = key
                for ln, lv in full_labels.labels.items():
                    self._label_index[ln][lv].add(key)

        ts.append(metric.sample)

    def append_batch(self, metrics: List[Metric]) -> None:
        for m in metrics:
            self.append(m)

    # ===== 基础查询接口 =====

    def get_series(self, key: str) -> Optional[TimeSeries]:
        return self._series.get(key)

    def get_sample_range(self, key: str, start: float, end: float) -> List[Sample]:
        ts = self._series.get(key)
        return ts.range(start, end) if ts else []

    def get_latest(self, key: str) -> Optional[Sample]:
        ts = self._series.get(key)
        return ts.latest() if ts else None

    # ===== 标签匹配查询 =====

    def match_series(self, matchers: List) -> List[str]:
        """
        根据标签匹配器查询符合条件的序列 key 列表
        支持 =, !=, =~, !~ 四种操作符
        """
        with self._lock:
            if not matchers:
                return list(self._series.keys())

            result_sets: List[set] = []

            for matcher in matchers:
                name = matcher.name
                operator = matcher.operator
                value = matcher.value

                if operator == "=":
                    keys = self._label_index.get(name, {}).get(value, set()).copy()
                    result_sets.append(keys)
                elif operator == "!=":
                    all_keys = set(self._series.keys())
                    exclude = self._label_index.get(name, {}).get(value, set())
                    result_sets.append(all_keys - exclude)
                elif operator == "=~":
                    matched = set()
                    for lv, keys in self._label_index.get(name, {}).items():
                        if matcher.matches(lv):
                            matched.update(keys)
                    result_sets.append(matched)
                elif operator == "!~":
                    all_keys = set(self._series.keys())
                    matched = set()
                    for lv, keys in self._label_index.get(name, {}).items():
                        if matcher.matches(lv):
                            matched.update(keys)
                    result_sets.append(all_keys - matched)

            if not result_sets:
                return list(self._series.keys())

            final = result_sets[0]
            for s in result_sets[1:]:
                final = final & s

            return list(final)

    def query_labels(self, keys: List[str]) -> List[LabelSet]:
        result = []
        for k in keys:
            ts = self._series.get(k)
            if ts:
                result.append(ts.labels)
        return result

    def query_samples(self, keys: List[str], start: float,
                      end: float) -> Dict[str, List[Sample]]:
        result = {}
        for k in keys:
            samples = self.get_sample_range(k, start, end)
            if samples:
                result[k] = samples
        return result

    # ===== 元数据查询 =====

    def all_series_keys(self) -> List[str]:
        return list(self._series.keys())

    def all_metric_names(self) -> List[str]:
        return list({k for k in self._label_index.get("__name__", {}).keys()})

    def label_names(self) -> List[str]:
        return list(self._label_index.keys())

    def label_values(self, name: str) -> List[str]:
        return list(self._label_index.get(name, {}).keys())

    def series_count(self) -> int:
        return len(self._series)

    def total_samples(self) -> int:
        return sum(len(ts) for ts in self._series.values())

    # ===== 数据保留 =====

    def _maybe_cleanup(self) -> None:
        now = time.time()
        if now - self._last_cleanup < 60:
            return
        self.cleanup()
        self._last_cleanup = now

    def cleanup(self) -> int:
        cutoff = time.time() - self._retention
        removed_total = 0
        with self._lock:
            empty_keys = []
            for key, ts in self._series.items():
                removed = ts.cleanup(cutoff)
                removed_total += removed
                if len(ts) == 0:
                    empty_keys.append(key)

            for key in empty_keys:
                ts = self._series.pop(key)
                if ts.labels in self._series_by_labels:
                    del self._series_by_labels[ts.labels]
                for ln, lv in ts.labels.labels.items():
                    if ln in self._label_index and lv in self._label_index[ln]:
                        self._label_index[ln][lv].discard(key)
                        if not self._label_index[ln][lv]:
                            del self._label_index[ln][lv]
                    if ln in self._label_index and not self._label_index[ln]:
                        del self._label_index[ln]

        return removed_total
