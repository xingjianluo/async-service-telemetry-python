import asyncio
import logging

import pytest

from app.telemetry.errors import DependencyUnavailableError
from app.telemetry.health import Dependency, HealthChecker, Status

log = logging.getLogger("test.health")


async def _ok() -> None:
    await asyncio.sleep(0)


async def _slow() -> None:
    await asyncio.sleep(10)


async def _refused() -> None:
    raise DependencyUnavailableError("pg: connection refused on 10.0.0.1:5432")


@pytest.mark.asyncio
async def test_all_healthy():
    hc = HealthChecker()
    hc.register(Dependency("db", _ok))
    hc.register(Dependency("cache", _ok, critical=False))
    report = await hc.check()
    assert report.status is Status.HEALTHY
    assert all(r.reason == "ok" for r in report.checks)
    log.info("status=%s 判定依据=全部依赖 ok", report.status.value)


@pytest.mark.asyncio
async def test_timeout_is_distinguished():
    hc = HealthChecker()
    hc.register(Dependency("slow-db", _slow, timeout_s=0.05))
    report = await hc.check()
    assert report.status is Status.UNHEALTHY
    assert report.checks[0].reason == "timeout"
    assert report.checks[0].latency_ms < 1000  # 确实被超时截断
    log.info(
        "status=%s reason=%s 判定依据=超时截断",
        report.status.value,
        report.checks[0].reason,
    )


@pytest.mark.asyncio
async def test_non_critical_failure_degrades():
    hc = HealthChecker()
    hc.register(Dependency("db", _ok))
    hc.register(Dependency("cache", _refused, critical=False))
    report = await hc.check()
    assert report.status is Status.DEGRADED
    reasons = {r.name: r.reason for r in report.checks}
    assert reasons["cache"] == "dependency_unavailable"
    assert reasons["db"] == "ok"
    log.info("status=%s 判定依据=%s", report.status.value, reasons)


@pytest.mark.asyncio
async def test_critical_failure_unhealthy_and_no_internal_leak():
    hc = HealthChecker()
    hc.register(Dependency("db", _refused))
    report = await hc.check()
    assert report.status is Status.UNHEALTHY
    reason = report.checks[0].reason
    assert reason == "dependency_unavailable"
    # 对外原因码不泄露内部地址等细节
    assert "10.0.0.1" not in reason
    log.info("status=%s reason=%s 判定依据=关键依赖失败", report.status.value, reason)


@pytest.mark.asyncio
async def test_checks_run_concurrently():
    hc = HealthChecker()
    for i in range(5):
        hc.register(Dependency(f"dep{i}", _slow, timeout_s=0.1))
    report = await hc.check()
    # 并发执行：5 个 0.1s 超时检查总耗时应远小于串行的 0.5s
    total = sum(r.latency_ms for r in report.checks)
    assert total >= 500  # 每个都跑满超时
    assert report.status is Status.UNHEALTHY
