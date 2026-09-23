"""按路由维度汇总时延分布与错误计数。

维度约束：
- 标签使用路由模板（如 ``/items/{item_id}``），不使用原始路径，
  避免高基数路径参数撑爆维度；
- 无匹配路由（404）统一归入 ``__unknown__``；
- 路由数超过 ``max_routes`` 时，新路由确定性地归入 ``__other__``，
  已有路由不受影响。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

OVERFLOW_ROUTE = "__other__"
UNKNOWN_ROUTE = "__unknown__"

# 时延桶边界（毫秒），Prometheus 风格直方图。
DEFAULT_BUCKETS_MS: tuple[float, ...] = (5, 10, 25, 50, 100, 250, 500, 1000, 5000)


@dataclass(slots=True)
class RouteStats:
    """单条路由的累计统计。"""

    count: int = 0
    error_count: int = 0
    latency_sum_ms: float = 0.0
    latency_max_ms: float = 0.0
    buckets: dict[float, int] = field(default_factory=dict)

    @property
    def error_rate(self) -> float:
        return self.error_count / self.count if self.count else 0.0


class MetricsRegistry:
    """路由维度指标注册表，带维度上限与归并策略。"""

    def __init__(
        self,
        max_routes: int = 100,
        buckets_ms: tuple[float, ...] = DEFAULT_BUCKETS_MS,
    ) -> None:
        if max_routes < 1:
            raise ValueError("max_routes must be >= 1")
        self._max_routes = max_routes
        self._buckets_ms = buckets_ms
        self._stats: dict[str, RouteStats] = {}
        self._lock = threading.Lock()

    def resolve_route(self, route_template: str | None) -> str:
        """把路由模板归一化为指标维度标签；未知/超限时归并。"""
        with self._lock:
            if not route_template:
                return self._ensure(UNKNOWN_ROUTE)
            if route_template in self._stats:
                return route_template
            regular = sum(
                1
                for k in self._stats
                if k not in (UNKNOWN_ROUTE, OVERFLOW_ROUTE)
            )
            if regular >= self._max_routes:
                return self._ensure(OVERFLOW_ROUTE)
            self._stats[route_template] = RouteStats(
                buckets={b: 0 for b in self._buckets_ms}
            )
            return route_template

    def _ensure(self, label: str) -> str:
        """确保归并标签存在（归并桶不占 max_routes 配额）。调用方须持锁。"""
        if label not in self._stats:
            self._stats[label] = RouteStats(
                buckets={b: 0 for b in self._buckets_ms}
            )
        return label

    def record(self, route: str, latency_ms: float, is_error: bool) -> str:
        """记录一次请求（成功与失败各计一次，不重复）。

        返回实际使用的维度标签（可能被归并）。"""
        label = self.resolve_route(route)
        with self._lock:
            stats = self._stats[label]
            stats.count += 1
            if is_error:
                stats.error_count += 1
            stats.latency_sum_ms += latency_ms
            stats.latency_max_ms = max(stats.latency_max_ms, latency_ms)
            for bound in self._buckets_ms:
                if latency_ms <= bound:
                    stats.buckets[bound] += 1
        return label

    def snapshot(self) -> dict[str, RouteStats]:
        """返回当前统计快照（副本）。"""
        with self._lock:
            return {
                route: RouteStats(
                    count=s.count,
                    error_count=s.error_count,
                    latency_sum_ms=s.latency_sum_ms,
                    latency_max_ms=s.latency_max_ms,
                    buckets=dict(s.buckets),
                )
                for route, s in self._stats.items()
            }
