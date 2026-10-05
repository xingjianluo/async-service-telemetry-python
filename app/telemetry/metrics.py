"""按路由维度的时延分布与错误计数，基数受控。

计数语义（在 record() 内唯一完成，任何路径只计一次）：
- total：进入记录的全部请求；
- success：status_code < 400；
- client_errors：400 <= status_code < 500；
- server_errors：status_code >= 500。
三者互斥且 total = 三者之和。

失败原因（failure_reasons）：
- 每次失败（status_code >= 400）恰好归入一个原因桶，各原因之和恒等于
  client_errors + server_errors（失败总数），不重不漏；
- 原因词汇有界：中间件在异常路径传入归一化分类（ErrorKind 值），
  未提供时按状态码确定性推导（4xx -> client_error，5xx -> internal），
  绝不出现无原因的失败，也不扩张维度基数。

维度约束：
- 只有通过 register_route() 显式注册的路由模板才拥有独立维度；
- 已识别但未注册的模板统一归并到 __other__；
- 无匹配路由（404 任意路径）统一归并到 __unmatched__；
- 注册数量超过 max_routes 时拒绝新增（调用方配置错误应尽早暴露）。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

# 时延直方图边界（秒），+Inf 由 _bucket_for 用 None 表示
LATENCY_BOUNDS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)
INF = float("inf")

# 失败原因词汇（有界）：异常路径由中间件传入 ErrorKind 值，
# 未提供时按状态码确定性推导为下列两个兜底值之一
REASON_CLIENT_ERROR = "client_error"
REASON_INTERNAL = "internal"


def derive_failure_reason(status_code: int) -> str | None:
    """按状态码确定性推导失败原因；成功请求返回 None（无失败原因）。"""
    if status_code < 400:
        return None
    if status_code < 500:
        return REASON_CLIENT_ERROR
    return REASON_INTERNAL


@dataclass(slots=True)
class RouteMetrics:
    """单一路由维度的指标快照。"""

    total: int = 0
    success: int = 0
    client_errors: int = 0
    server_errors: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)
    latency_buckets: dict[float, int] = field(default_factory=dict)
    latency_sum_seconds: float = 0.0
    latency_count: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "success": self.success,
            "client_errors": self.client_errors,
            "server_errors": self.server_errors,
            "failure_reasons": dict(self.failure_reasons),
            "latency_buckets": dict(self.latency_buckets),
            "latency_sum_seconds": self.latency_sum_seconds,
            "latency_count": self.latency_count,
        }


def _bucket_for(duration: float) -> float:
    for bound in LATENCY_BOUNDS:
        if duration <= bound:
            return bound
    return INF


class MetricsRegistry:
    OVERFLOW_ROUTE = "__other__"
    UNMATCHED_ROUTE = "__unmatched__"

    def __init__(self, max_routes: int = 64) -> None:
        self._max_routes = max_routes
        self._known: set[str] = set()
        self._metrics: dict[str, RouteMetrics] = {}
        self._lock = threading.Lock()

    def register_route(self, route: str) -> None:
        with self._lock:
            if route in self._known:
                return
            if len(self._known) >= self._max_routes:
                raise RuntimeError(
                    f"metrics route cardinality limit {self._max_routes} reached; "
                    f"refuse to register {route!r}"
                )
            self._known.add(route)
            self._metrics[route] = RouteMetrics()

    def canonical_route(self, route: str | None) -> str:
        """把任意路由模板归并为受控维度。"""
        if route is None:
            return self.UNMATCHED_ROUTE
        with self._lock:
            if route in self._known:
                return route
        return self.OVERFLOW_ROUTE

    def _ensure(self, key: str) -> RouteMetrics:
        m = self._metrics.get(key)
        if m is None:
            m = RouteMetrics()
            self._metrics[key] = m
        return m

    def record(
        self,
        route: str | None,
        status_code: int,
        duration: float,
        reason: str | None = None,
    ) -> str:
        """记录一次请求的结果，成功/失败在此唯一计数。返回实际归并维度。

        reason：失败原因（有界词汇，异常路径由中间件传入归一化分类）；
        失败但未提供时按状态码确定性推导，成功请求忽略该参数。
        """
        key = self.canonical_route(route)
        if status_code >= 400 and reason is None:
            reason = derive_failure_reason(status_code)
        with self._lock:
            m = self._ensure(key)
            m.total += 1
            if status_code < 400:
                m.success += 1
            elif status_code < 500:
                m.client_errors += 1
            else:
                m.server_errors += 1
            if reason is not None and status_code >= 400:
                m.failure_reasons[reason] = m.failure_reasons.get(reason, 0) + 1
            m.latency_buckets[_bucket_for(duration)] = (
                m.latency_buckets.get(_bucket_for(duration), 0) + 1
            )
            m.latency_sum_seconds += duration
            m.latency_count += 1
        return key

    def snapshot(self) -> dict[str, RouteMetrics]:
        with self._lock:
            return {k: _copy_metrics(v) for k, v in self._metrics.items()}


def _copy_metrics(m: RouteMetrics) -> RouteMetrics:
    return RouteMetrics(
        total=m.total,
        success=m.success,
        client_errors=m.client_errors,
        server_errors=m.server_errors,
        failure_reasons=dict(m.failure_reasons),
        latency_buckets=dict(m.latency_buckets),
        latency_sum_seconds=m.latency_sum_seconds,
        latency_count=m.latency_count,
    )
