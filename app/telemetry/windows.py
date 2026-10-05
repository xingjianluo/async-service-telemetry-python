"""滑动时间窗口聚合（每路由维度）。

语义：
- 时间被切分为定长 bucket（bucket_seconds），窗口 = 最近 num_buckets 个 bucket；
- 每笔请求在完成时记录一次，按完成时间戳落入且仅落入一个 bucket
  （bucket_index = floor(ts / bucket_seconds)），并发在途请求归属确定，
  不会双窗口重复计数、也不会漏计；
- 窗口随时间前滚，过期 bucket 被裁剪；裁剪只影响窗口视图，
  累计指标（MetricsRegistry）独立维护，二者可对账：
  窗口覆盖全部历史时，窗口聚合 == 累计值；
- 延迟分位数（p50/p95/p99）由窗口内留存的原始样本计算，
  采用线性插值（rank = q * (n-1)），结果确定；
- 失败原因（failure_reasons）与累计指标同口径：每次失败恰好落入
  一个原因桶，随所属 bucket 一起滚动过期，窗口内各原因之和恒等于
  client_errors + server_errors，窗口覆盖全部历史时与累计逐项对账一致。
"""
from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from .metrics import classify_failure_reason

# 窗口内延迟分位数
QUANTILES: tuple[float, ...] = (0.5, 0.95, 0.99)


@dataclass(slots=True)
class _Bucket:
    total: int = 0
    success: int = 0
    client_errors: int = 0
    server_errors: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)
    latencies: list[float] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class WindowSnapshot:
    """单一路由在当前窗口内的聚合快照。"""

    route: str
    window_start: float
    window_end: float
    total: int = 0
    success: int = 0
    client_errors: int = 0
    server_errors: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)
    quantiles: dict[float, float | None] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "route": self.route,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "total": self.total,
            "success": self.success,
            "client_errors": self.client_errors,
            "server_errors": self.server_errors,
            "failure_reasons": dict(self.failure_reasons),
            "quantiles": dict(self.quantiles),
        }


def _quantile(sorted_vals: list[float], q: float) -> float | None:
    """线性插值分位数；无样本返回 None（确定性规则）。"""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return sorted_vals[0]
    rank = q * (n - 1)
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


class SlidingWindowMetrics:
    """按路由的滑动窗口聚合器（线程安全）。

    路由维度由调用方负责归并（传入的必须是 canonical route），
    本类不扩张维度基数。
    """

    def __init__(
        self,
        window_seconds: float = 300.0,
        bucket_seconds: float = 10.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if window_seconds <= 0 or bucket_seconds <= 0:
            raise ValueError("window_seconds and bucket_seconds must be positive")
        self._window_seconds = window_seconds
        self._bucket_seconds = bucket_seconds
        self._num_buckets = max(1, math.ceil(window_seconds / bucket_seconds))
        self._clock = clock or time.time
        self._routes: dict[str, dict[int, _Bucket]] = {}
        self._lock = threading.Lock()

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    @property
    def bucket_seconds(self) -> float:
        return self._bucket_seconds

    def _bucket_index(self, ts: float) -> int:
        return int(ts // self._bucket_seconds)

    def _prune_locked(self, now_index: int) -> None:
        """裁剪窗口之外的过期 bucket（调用方须持锁）。"""
        oldest = now_index - self._num_buckets + 1
        empty_routes = []
        for route, buckets in self._routes.items():
            for idx in [i for i in buckets if i < oldest]:
                del buckets[idx]
            if not buckets:
                empty_routes.append(route)
        for route in empty_routes:
            del self._routes[route]

    def record(
        self,
        route: str,
        status_code: int,
        duration: float,
        ts: float | None = None,
        reason: str | None = None,
    ) -> None:
        """记录一次完成的请求；按 ts（默认当前时钟）落入唯一 bucket。

        reason 与 MetricsRegistry.record 同口径：失败且未提供时按状态码
        确定性推导，每次失败恰好计入一个原因桶，随 bucket 一起过期。
        """
        if ts is None:
            ts = self._clock()
        idx = self._bucket_index(ts)
        if status_code >= 400:
            reason = classify_failure_reason(status_code, reason)
        with self._lock:
            self._prune_locked(self._bucket_index(self._clock()))
            buckets = self._routes.setdefault(route, {})
            b = buckets.get(idx)
            if b is None:
                b = _Bucket()
                buckets[idx] = b
            b.total += 1
            if status_code < 400:
                b.success += 1
            elif status_code < 500:
                b.client_errors += 1
            else:
                b.server_errors += 1
            if reason is not None:
                b.failure_reasons[reason] = b.failure_reasons.get(reason, 0) + 1
            b.latencies.append(duration)

    def _window_bounds(self, now: float) -> tuple[float, float, int]:
        now_index = self._bucket_index(now)
        end = (now_index + 1) * self._bucket_seconds
        start = end - self._num_buckets * self._bucket_seconds
        return start, end, now_index

    def snapshot(self, route: str, now: float | None = None) -> WindowSnapshot:
        if now is None:
            now = self._clock()
        start, end, now_index = self._window_bounds(now)
        with self._lock:
            self._prune_locked(now_index)
            buckets = self._routes.get(route, {})
            return self._aggregate(route, start, end, buckets, now_index)

    def snapshot_all(self, now: float | None = None) -> dict[str, WindowSnapshot]:
        if now is None:
            now = self._clock()
        start, end, now_index = self._window_bounds(now)
        with self._lock:
            self._prune_locked(now_index)
            return {
                route: self._aggregate(route, start, end, buckets, now_index)
                for route, buckets in self._routes.items()
            }

    def _aggregate(
        self,
        route: str,
        start: float,
        end: float,
        buckets: dict[int, _Bucket],
        now_index: int,
    ) -> WindowSnapshot:
        oldest = now_index - self._num_buckets + 1
        total = success = client_errors = server_errors = 0
        failure_reasons: dict[str, int] = {}
        latencies: list[float] = []
        for idx, b in buckets.items():
            if idx < oldest or idx > now_index:
                continue  # 窗口外的 bucket 不参与（未来时间戳防御）
            total += b.total
            success += b.success
            client_errors += b.client_errors
            server_errors += b.server_errors
            for reason, count in b.failure_reasons.items():
                failure_reasons[reason] = failure_reasons.get(reason, 0) + count
            latencies.extend(b.latencies)
        latencies.sort()
        quantiles = {q: _quantile(latencies, q) for q in QUANTILES}
        return WindowSnapshot(
            route=route,
            window_start=start,
            window_end=end,
            total=total,
            success=success,
            client_errors=client_errors,
            server_errors=server_errors,
            failure_reasons=failure_reasons,
            quantiles=quantiles,
        )
