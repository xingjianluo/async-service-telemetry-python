"""对外遥测快照：只读、脱敏、限流、就绪门控。

语义：
- 输出窗口化路由聚合 + 累计对照；路由键全部是 canonical 模板
  （受 MetricsRegistry 基数上限约束，超限走既有 __other__/__unmatched__ 归并）；
- 窗口与累计两侧都带 failure_reasons（失败原因分布，有界词汇），
  各原因之和恒等于同侧 client_errors + server_errors，
  编排/告警侧可按原因分别设阈值并两侧对账；
- 脱敏边界：不输出异常内部细节、不输出依赖标识（健康检查名），
  仅输出整体就绪状态；内部关联信息（trace_id、路由数）只入服务端日志；
- 高频拉取受令牌桶限流，超限返回 429；
- 就绪门控：readiness 为 DOWN 时返回 503 与最小负载，
  绝不返回看似正常的数据；DEGRADED 仍返回 200 但负载中如实标注。
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .context import try_get_context
from .health import HealthReport, HealthState
from .metrics import MetricsRegistry
from .windows import SlidingWindowMetrics

logger = logging.getLogger("telemetry.snapshot")

ReadinessProbe = Callable[[], Awaitable[HealthReport]]


@dataclass(frozen=True, slots=True)
class SnapshotResponse:
    status_code: int
    payload: dict


class SnapshotRateLimiter:
    """令牌桶限流器（线程安全，时钟可注入以便测试）。"""

    def __init__(
        self,
        rate_per_second: float = 5.0,
        burst: int = 10,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if rate_per_second <= 0 or burst <= 0:
            raise ValueError("rate_per_second and burst must be positive")
        self._rate = rate_per_second
        self._burst = burst
        self._tokens = float(burst)
        self._last = 0.0
        self._clock = clock or time.monotonic
        self._started = False
        self._lock = threading.Lock()

    def allow(self) -> bool:
        now = self._clock()
        with self._lock:
            if self._started:
                self._tokens = min(
                    self._burst, self._tokens + (now - self._last) * self._rate
                )
            else:
                self._started = True
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class TelemetrySnapshotter:
    """组装对外快照负载。"""

    def __init__(
        self,
        metrics: MetricsRegistry,
        windows: SlidingWindowMetrics,
        readiness: ReadinessProbe,
        limiter: SnapshotRateLimiter,
    ) -> None:
        self._metrics = metrics
        self._windows = windows
        self._readiness = readiness
        self._limiter = limiter

    async def build(self) -> SnapshotResponse:
        trace_id = None
        ctx = try_get_context()
        if ctx is not None:
            trace_id = ctx.trace_id

        if not self._limiter.allow():
            logger.warning("snapshot rate limited trace_id=%s", trace_id)
            return SnapshotResponse(status_code=429, payload={
                "error": {"kind": "rate_limited",
                          "message": "snapshot polling rate exceeded"},
            })

        report = await self._readiness()
        if report.status is HealthState.DOWN:
            # 未就绪：明确拒绝，不返回看似正常的数据
            logger.warning("snapshot refused: not ready trace_id=%s", trace_id)
            return SnapshotResponse(status_code=503, payload={
                "error": {"kind": "not_ready",
                          "message": "service is not ready"},
                "status": report.status.value,
            })

        cumulative = self._metrics.snapshot()
        windowed = self._windows.snapshot_all()
        routes: dict[str, dict] = {}
        for route, cum in cumulative.items():
            win = windowed.get(route)
            routes[route] = {
                "window": win.to_dict() if win is not None else None,
                "cumulative": cum.to_dict(),
            }

        payload = {
            "status": report.status.value,
            "window": {
                "window_seconds": self._windows.window_seconds,
                "bucket_seconds": self._windows.bucket_seconds,
            },
            "routes": routes,
            "limits": {
                "route_count": len(routes),
            },
        }
        logger.info(
            "snapshot served trace_id=%s status=%s routes=%d",
            trace_id, report.status.value, len(routes),
        )
        return SnapshotResponse(status_code=200, payload=payload)
