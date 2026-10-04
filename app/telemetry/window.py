"""滑动时间窗口指标：按路由聚合最近一段时间的请求情况。

语义：
- 每个路由维度维护一组固定时长的连续时间桶（环形复用），
  窗口 = 最近 ``bucket_count`` 个桶，随时间向前滚动；
- 请求在完成时（中间件唯一计数点）落入**完成时刻**对应的桶。
  一笔请求只被记录一次，因此必落在且仅落在一个桶里——
  不存在跨窗重复计数，也不存在遗漏；
- 窗口滚动只是“旧桶被新桶复用”，只影响窗口聚合的可见范围，
  不回写、不修正累计值；累计统计（MetricsRegistry）是唯一事实源，
  任意时刻：窗口内 total <= 累计 total，且未发生滚动时两者相等；
- 分位数由固定边界的时延直方图近似（p50/p95/p99），内存有界。
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field

from .metrics import INF, LATENCY_BOUNDS, _bucket_for

# 窗口内延迟分位数（直方图近似）
QUANTILES: tuple[float, ...] = (0.50, 0.95, 0.99)


@dataclass(slots=True)
class WindowRouteStats:
    """单路由在窗口内的聚合快照。"""

    total: int = 0
    success: int = 0
    client_errors: int = 0
    server_errors: int = 0
    error_kinds: dict[str, int] = field(default_factory=dict)
    quantiles: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "success": self.success,
            "client_errors": self.client_errors,
            "server_errors": self.server_errors,
            "error_kinds": dict(self.error_kinds),
            "quantiles": dict(self.quantiles),
        }


@dataclass(slots=True)
class _Bucket:
    """一个时间桶；index 标识其覆盖的时间段，复用时整体重置。"""

    index: int = -1
    total: int = 0
    success: int = 0
    client_errors: int = 0
    server_errors: int = 0
    error_kinds: dict[str, int] = field(default_factory=dict)
    latency_buckets: dict[float, int] = field(default_factory=dict)

    def reset(self, index: int) -> None:
        self.index = index
        self.total = 0
        self.success = 0
        self.client_errors = 0
        self.server_errors = 0
        self.error_kinds.clear()
        self.latency_buckets.clear()


class WindowedMetrics:
    """按路由的滑动窗口聚合。

    路由维度由调用方（中间件）先经 MetricsRegistry 归并，
    因此本类的键集合与累计指标一致，维度同样有界。
    """

    def __init__(self, window_seconds: float = 300.0, bucket_seconds: float = 10.0) -> None:
        if window_seconds <= 0 or bucket_seconds <= 0:
            raise ValueError("window_seconds and bucket_seconds must be positive")
        self._window_seconds = float(window_seconds)
        self._bucket_seconds = float(bucket_seconds)
        self._bucket_count = max(1, math.ceil(window_seconds / bucket_seconds))
        self._routes: dict[str, list[_Bucket]] = {}
        self._lock = threading.Lock()

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    @property
    def bucket_count(self) -> int:
        return self._bucket_count

    def _index_at(self, now: float) -> int:
        return int(now // self._bucket_seconds)

    def record(self, route: str, status_code: int, duration: float,
               error_kind: str | None = None, now: float | None = None) -> None:
        """把一次完成的请求计入其完成时刻对应的桶（唯一落点）。"""
        ts = time.monotonic() if now is None else now
        index = self._index_at(ts)
        with self._lock:
            ring = self._routes.get(route)
            if ring is None:
                ring = [_Bucket() for _ in range(self._bucket_count)]
                self._routes[route] = ring
            bucket = ring[index % self._bucket_count]
            if bucket.index != index:
                bucket.reset(index)
            bucket.total += 1
            if status_code < 400:
                bucket.success += 1
            elif status_code < 500:
                bucket.client_errors += 1
            else:
                bucket.server_errors += 1
            if error_kind is not None:
                bucket.error_kinds[error_kind] = bucket.error_kinds.get(error_kind, 0) + 1
            bound = _bucket_for(duration)
            bucket.latency_buckets[bound] = bucket.latency_buckets.get(bound, 0) + 1

    def snapshot(self, now: float | None = None) -> dict[str, WindowRouteStats]:
        """聚合当前窗口（最近 bucket_count 个桶）内的各路由统计。"""
        ts = time.monotonic() if now is None else now
        current = self._index_at(ts)
        oldest = current - self._bucket_count + 1
        with self._lock:
            return {
                route: self._aggregate(ring, oldest, current)
                for route, ring in self._routes.items()
            }

    def _aggregate(self, ring: list[_Bucket], oldest: int, current: int) -> WindowRouteStats:
        stats = WindowRouteStats()
        merged_hist: dict[float, int] = {}
        for bucket in ring:
            # 只聚合 [oldest, current] 内的桶：旧桶已滑出，未来桶不属于本窗口
            if bucket.index < oldest or bucket.index > current:
                continue
            stats.total += bucket.total
            stats.success += bucket.success
            stats.client_errors += bucket.client_errors
            stats.server_errors += bucket.server_errors
            for kind, n in bucket.error_kinds.items():
                stats.error_kinds[kind] = stats.error_kinds.get(kind, 0) + n
            for bound, n in bucket.latency_buckets.items():
                merged_hist[bound] = merged_hist.get(bound, 0) + n
        stats.quantiles = _quantiles_from_histogram(merged_hist, stats.total)
        return stats


def _quantiles_from_histogram(hist: dict[float, int], total: int) -> dict[str, float]:
    """从固定边界直方图近似分位数（桶内线性插值）。"""
    if total <= 0:
        return {}
    result: dict[str, float] = {}
    for q in QUANTILES:
        target = q * total
        cumulative = 0
        low = 0.0
        value = LATENCY_BOUNDS[-1]
        for bound in (*LATENCY_BOUNDS, INF):
            count = hist.get(bound, 0)
            if count <= 0:
                continue
            high = LATENCY_BOUNDS[-1] if bound == INF else bound
            if cumulative + count >= target:
                frac = (target - cumulative) / count
                value = low + frac * (high - low)
                break
            cumulative += count
            low = high
        result[f"p{int(q * 100)}"] = round(value, 6)
    return result
