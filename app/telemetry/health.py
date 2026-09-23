"""健康（liveness）与就绪（readiness）判定。

语义：
- liveness：进程存活即 UP（不探测依赖，避免级联重启）；
- readiness：并发执行依赖探针，单探针带超时；
  - 关键依赖 DOWN -> 整体 DOWN；
  - 仅非关键依赖异常 -> 整体 DEGRADED；
  - 全部正常 -> UP。
- 对外负载只含状态与稳定原因码；异常细节（detail）仅入服务端日志。
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("telemetry.health")


class HealthState(str, Enum):
    UP = "up"
    DEGRADED = "degraded"
    DOWN = "down"


Probe = Callable[[], Awaitable["ProbeResult"]]

REASON_OK = "ok"
REASON_TIMEOUT = "probe_timeout"
REASON_ERROR = "probe_error"


@dataclass(frozen=True, slots=True)
class ProbeResult:
    state: HealthState
    reason: str = REASON_OK  # 稳定的机器可读原因码
    detail: str | None = None  # 仅入日志的内部信息

    @classmethod
    def ok(cls) -> "ProbeResult":
        return cls(state=HealthState.UP, reason=REASON_OK)


@dataclass(frozen=True, slots=True)
class HealthReport:
    status: HealthState
    checks: dict[str, ProbeResult]


class HealthChecker:
    def __init__(self, probe_timeout: float = 1.0) -> None:
        self._probe_timeout = probe_timeout
        self._probes: dict[str, tuple[Probe, bool]] = {}

    def register(self, name: str, probe: Probe, critical: bool = True) -> None:
        self._probes[name] = (probe, critical)

    async def liveness(self) -> HealthReport:
        return HealthReport(status=HealthState.UP, checks={})

    async def readiness(self) -> HealthReport:
        names = list(self._probes)
        results = await asyncio.gather(
            *(self._run_probe(name) for name in names)
        )
        checks = dict(zip(names, results, strict=True))
        return HealthReport(status=self._aggregate(checks), checks=checks)

    async def _run_probe(self, name: str) -> ProbeResult:
        probe, _ = self._probes[name]
        try:
            return await asyncio.wait_for(probe(), timeout=self._probe_timeout)
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning("health probe timeout", extra={"probe": name})
            return ProbeResult(
                state=HealthState.DOWN,
                reason=REASON_TIMEOUT,
                detail=f"probe exceeded {self._probe_timeout}s",
            )
        except Exception as exc:  # noqa: BLE001 - 探针异常必须被归一化
            logger.warning(
                "health probe error", extra={"probe": name, "error": repr(exc)}
            )
            return ProbeResult(
                state=HealthState.DOWN,
                reason=REASON_ERROR,
                detail=f"{type(exc).__name__}: {exc}",
            )

    def _aggregate(self, checks: dict[str, ProbeResult]) -> HealthState:
        overall = HealthState.UP
        for name, result in checks.items():
            _, critical = self._probes[name]
            if result.state is HealthState.UP:
                continue
            if critical:
                return HealthState.DOWN
            overall = HealthState.DEGRADED
        return overall

    def public_payload(self, report: HealthReport) -> dict:
        """脱敏后的对外负载：仅状态与原因码，无内部细节。"""
        return {
            "status": report.status.value,
            "checks": {
                name: {"state": r.state.value, "reason": r.reason}
                for name, r in report.checks.items()
            },
        }
