"""健康与就绪判定：依赖检查、超时与降级语义。

判定语义：
- 所有依赖成功 -> healthy；
- 仅非关键依赖失败 -> degraded（部分降级）；
- 任一关键依赖失败 -> unhealthy；
- 单个依赖检查超时按失败处理，reason=timeout；
- 失败原因经 errors.normalize 归一化，对外不暴露内部异常细节。
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from app.telemetry.errors import normalize
from app.telemetry.tracing import current_context

log = logging.getLogger("telemetry.health")


class Status(enum.Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True, slots=True)
class DependencyResult:
    name: str
    status: Status
    reason: str  # 对外安全的归一化原因码；"ok" 表示成功
    latency_ms: float


@dataclass(frozen=True, slots=True)
class HealthReport:
    status: Status
    checks: tuple[DependencyResult, ...] = field(default_factory=tuple)


@dataclass(slots=True)
class Dependency:
    name: str
    check: Callable[[], Awaitable[None]]
    critical: bool = True  # 非关键依赖失败 -> degraded；关键依赖失败 -> unhealthy
    timeout_s: float = 1.0


class HealthChecker:
    def __init__(self) -> None:
        self._deps: list[Dependency] = []

    def register(self, dep: Dependency) -> None:
        self._deps.append(dep)

    async def check(self) -> HealthReport:
        """并发执行所有依赖检查并聚合总体状态。"""
        results = await asyncio.gather(
            *(self._check_one(dep) for dep in self._deps)
        )
        status = Status.HEALTHY
        for dep, result in zip(self._deps, results, strict=True):
            if result.status is Status.HEALTHY:
                continue
            if dep.critical:
                status = Status.UNHEALTHY
            elif status is Status.HEALTHY:
                status = Status.DEGRADED
        report = HealthReport(status=status, checks=tuple(results))
        ctx = current_context()
        log.info(
            "health decision request_id=%s status=%s basis=%s",
            ctx.request_id if ctx else "-",
            status.value,
            {r.name: r.reason for r in results},
        )
        return report

    async def _check_one(self, dep: Dependency) -> DependencyResult:
        start = time.perf_counter()
        try:
            async with asyncio.timeout(dep.timeout_s):
                await dep.check()
        except BaseException as exc:  # noqa: BLE001 - 含 TimeoutError；归一化后记录
            latency = (time.perf_counter() - start) * 1000
            err = normalize(exc)
            ctx = current_context()
            log.warning(
                "dependency check failed request_id=%s dep=%s reason=%s detail=%s",
                ctx.request_id if ctx else "-",
                dep.name,
                err.reason,
                err.log_detail,
            )
            return DependencyResult(
                name=dep.name,
                status=Status.UNHEALTHY,
                reason=err.reason,
                latency_ms=latency,
            )
        latency = (time.perf_counter() - start) * 1000
        return DependencyResult(
            name=dep.name, status=Status.HEALTHY, reason="ok", latency_ms=latency
        )
