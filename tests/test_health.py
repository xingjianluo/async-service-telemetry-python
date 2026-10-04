"""健康/就绪判定单测：up / degraded / down / timeout 可区分，对外无内部细节。"""
from __future__ import annotations

import asyncio

from app.telemetry.health import (
    REASON_ERROR,
    REASON_OK,
    REASON_TIMEOUT,
    HealthChecker,
    HealthState,
    ProbeResult,
)


def ok_probe():
    async def p():
        return ProbeResult(HealthState.UP)
    return p


async def test_liveness_always_up():
    h = HealthChecker()
    h.register("db", _failing_probe, critical=True)
    report = await h.liveness()
    assert report.status is HealthState.UP
    print("[health] liveness 判定:up（进程存活与依赖解耦）")


async def _failing_probe():
    raise RuntimeError("internal dsn redis://user:pass@host must not leak")


async def test_readiness_all_up():
    h = HealthChecker()
    h.register("db", ok_probe(), critical=True)
    report = await h.readiness()
    assert report.status is HealthState.UP
    assert report.checks["db"].reason == REASON_OK
    payload = h.public_payload(report)
    print(f"[health] readiness 判定:{payload}")


async def test_readiness_critical_down():
    h = HealthChecker(probe_timeout=0.2)
    h.register("db", _failing_probe, critical=True)
    h.register("cache", ok_probe(), critical=False)
    report = await h.readiness()
    assert report.status is HealthState.DOWN
    assert report.checks["db"].reason == REASON_ERROR
    payload = h.public_payload(report)
    assert payload["status"] == "down"
    assert "must not leak" not in str(payload)
    assert "dsn" not in str(payload)
    print(f"[health] 关键依赖异常 判定:down, db.reason={REASON_ERROR}，对外负载无内部细节")


async def test_readiness_non_critical_degraded():
    async def cache_down():
        return ProbeResult(HealthState.DOWN, reason="cache_unavailable", detail="x")

    h = HealthChecker()
    h.register("db", ok_probe(), critical=True)
    h.register("cache", cache_down, critical=False)
    report = await h.readiness()
    assert report.status is HealthState.DEGRADED
    payload = h.public_payload(report)
    assert payload["checks"]["cache"]["reason"] == "cache_unavailable"
    print("[health] 非关键依赖异常 判定:degraded（原因可区分于 down）")


async def test_readiness_timeout_is_down():
    async def slow():
        await asyncio.sleep(1.0)

    h = HealthChecker(probe_timeout=0.05)
    h.register("db", slow, critical=True)
    report = await h.readiness()
    assert report.status is HealthState.DOWN
    assert report.checks["db"].reason == REASON_TIMEOUT
    print(f"[health] 探针超时 判定:down, db.reason={REASON_TIMEOUT}")


async def test_app_readiness_endpoint_states():
    """通过 debug 接口切换依赖状态，验证端到端三种判定。"""
    import httpx

    from app.service import create_app

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        for dep, state, expected in (
            ("primary", "down", "down"),
            ("primary", "up", "up"),
            ("cache", "down", "degraded"),
            ("cache", "up", "up"),
        ):
            await c.post(f"/debug/deps/{dep}?state={state}")
            r = await c.get("/health/ready")
            body = r.json()
            assert body["status"] == expected, (dep, state, body)
            print(f"[health] {dep}={state} 判定:{expected} 负载={body}")
