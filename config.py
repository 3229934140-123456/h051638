"""
配置模块: 定义系统的数据模型、目标配置、告警规则和路由配置
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
import time
import re


@dataclass
class LabelSet:
    """标签集合: 用作时间序列的唯一标识"""
    labels: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        self._key = None

    @property
    def key(self) -> str:
        if self._key is None:
            sorted_items = sorted(self.labels.items())
            self._key = "|".join(f"{k}={v}" for k, v in sorted_items)
        return self._key

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if not isinstance(other, LabelSet):
            return False
        return self.labels == other.labels

    def get(self, name: str) -> Optional[str]:
        return self.labels.get(name)

    def copy(self) -> 'LabelSet':
        return LabelSet(dict(self.labels))


@dataclass
class Sample:
    """指标样本: 时间戳 + 数值"""
    timestamp: float
    value: float


@dataclass
class Metric:
    """带标签的指标样本"""
    name: str
    labels: LabelSet
    sample: Sample

    @property
    def full_labels(self) -> LabelSet:
        merged = dict(self.labels.labels)
        merged["__name__"] = self.name
        return LabelSet(merged)

    @property
    def series_key(self) -> str:
        return self.full_labels.key


@dataclass
class ScrapeTarget:
    """抓取目标配置"""
    job: str
    url: str
    scrape_interval: float = 15.0
    scrape_timeout: float = 10.0
    static_labels: Dict[str, str] = field(default_factory=dict)
    health: str = "unknown"
    last_scrape: Optional[float] = None
    last_scrape_duration: Optional[float] = None
    last_error: Optional[str] = None
    scrape_count: int = 0
    error_count: int = 0

    def mark_healthy(self, duration: float):
        self.health = "up"
        self.last_scrape = time.time()
        self.last_scrape_duration = duration
        self.last_error = None
        self.scrape_count += 1

    def mark_unhealthy(self, error: str):
        self.health = "down"
        self.last_scrape = time.time()
        self.last_error = error
        self.scrape_count += 1
        self.error_count += 1


@dataclass
class LabelMatcher:
    """标签匹配器: 用于查询和告警规则中的标签过滤"""
    name: str
    value: str
    operator: str = "="

    def matches(self, label_value: Optional[str]) -> bool:
        if self.operator == "=":
            return label_value == self.value
        elif self.operator == "!=":
            return label_value != self.value
        elif self.operator == "=~":
            try:
                return label_value is not None and bool(re.match(self.value, label_value))
            except re.error:
                return False
        elif self.operator == "!~":
            try:
                return label_value is None or not bool(re.match(self.value, label_value))
            except re.error:
                return True
        return False


@dataclass
class AlertRule:
    """告警规则配置"""
    name: str
    metric_name: str
    label_matchers: List[LabelMatcher]
    condition: str
    threshold: float
    for_duration: float = 60.0
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)

    def evaluate_condition(self, value: float) -> bool:
        if self.condition == ">":
            return value > self.threshold
        elif self.condition == ">=":
            return value >= self.threshold
        elif self.condition == "<":
            return value < self.threshold
        elif self.condition == "<=":
            return value <= self.threshold
        elif self.condition == "==":
            return value == self.threshold
        elif self.condition == "!=":
            return value != self.threshold
        return False


@dataclass
class NotificationRoute:
    """告警通知路由配置"""
    name: str
    label_matchers: List[LabelMatcher]
    channels: List[str]
    group_by: List[str] = field(default_factory=lambda: ["alertname"])
    group_wait: float = 10.0
    repeat_interval: float = 300.0


@dataclass
class Alert:
    """活动告警实例"""
    fingerprint: str
    rule_name: str
    labels: LabelSet
    annotations: Dict[str, str]
    value: float
    starts_at: float
    ends_at: Optional[float] = None
    status: str = "pending"
    last_sent: Optional[float] = None

    def duration(self, now: Optional[float] = None) -> float:
        now = now or time.time()
        end = self.ends_at or now
        return end - self.starts_at


@dataclass
class Config:
    """全局系统配置"""
    global_scrape_interval: float = 15.0
    global_scrape_timeout: float = 10.0
    evaluation_interval: float = 15.0
    tsdb_retention_period: float = 3600 * 24
    targets: List[ScrapeTarget] = field(default_factory=list)
    alert_rules: List[AlertRule] = field(default_factory=list)
    routes: List[NotificationRoute] = field(default_factory=list)
    discovery_interval: float = 60.0
