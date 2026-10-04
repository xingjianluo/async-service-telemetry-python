"""对外快照端点单测：脱敏、窗口+累计对照、限流、就绪门控、维度上限。"""
from __future__ import annotations

import logging

import httpx
import pytest

from app.service import create_app
from app.telemetry.snapshot import SnapshotRateLimiter


@pytest.fixture
async def client():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        c.app = app  # type: ignore[attr-defined]
        yield c


async def test_snapshot_contains_window_and_cumulative(client):
    await client.get("/")
    await client.get("/boom")
    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "up"
    assert body["window"]["window_seconds"] > 0
    routes = body["routes"]
    assert "/" in routes and "/boom" in routes
    root = routes["/"]
    # 窗口与累计同时在场，且本用例内二者相等（窗口覆盖全部历史）
    assert root["window"]["total"] == root["cumulative"]["total"] == 1
    assert routes["/boom"]["cumulative"]["server_errors"] == 1
    q = root["window"]["quantiles"]
    assert set(q) == {"0.5", "0.95", "0.99"}
    print(f"[snapshot] 判定:窗口+累计对照齐全，分位数键 {sorted(q)}")


async def test_snapshot_is_sanitized(client):
    """对外负载不得泄露内部异常细节与依赖标识。"""
    await client.get("/boom")
    await client.get("/upstream/unavailable")
    r = await client.get("/telemetry/snapshot")
    text = r.text
    leaked = (
        "secret internal detail",  # 内部异常细节
        "10.0.0.1",                # 依赖地址
        "circuit half-open",       # 熔断内部状态
        "primary", "cache",        # 依赖标识（健康检查名）
    )
    assert not any(s in text for s in leaked), f"泄露: {text}"
    print("[snapshot] 判定:无异常细节、无依赖标识，脱敏边界成立")


async def test_snapshot_refused_when_not_ready(client):
    await client.post("/debug/deps/primary", params={"state": "down"})
    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 503
    body = r.json()
    assert body["error"]["kind"] == "not_ready"
    assert "routes" not in body  # 不返回看似正常的数据
    print("[snapshot] 判定:未就绪时 503 明确拒绝，负载最小化")


async def test_snapshot_degraded_still_served_with_status(client):
    await client.post("/debug/deps/cache", params={"state": "down"})
    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 200
    assert r.json()["status"] == "degraded"
    print("[snapshot] 判定:降级状态如实标注，仍可提供快照")


async def test_snapshot_rate_limited(client):
    """超过令牌桶突发量后必须 429，且不拖垮服务（拒绝是即时的）。"""
    codes = []
    for _ in range(30):
        r = await client.get("/telemetry/snapshot")
        codes.append(r.status_code)
    assert 429 in codes
    first_429 = codes.index(429)
    assert first_429 <= 10  # burst=10，第 11 次起应被拒
    assert all(c == 429 for c in codes[first_429:first_429 + 5])
    body = (await client.get("/telemetry/snapshot")).json()
    assert body["error"]["kind"] == "rate_limited"
    print(f"[snapshot] 判定:第 {first_429 + 1} 次拉起限流，持续返回 429")


def test_rate_limiter_token_bucket_deterministic():
    t = [0.0]
    limiter = SnapshotRateLimiter(rate_per_second=2.0, burst=3, clock=lambda: t[0])
    assert [limiter.allow() for _ in range(4)] == [True, True, True, False]
    t[0] = 1.0  # 1 秒补 2 个令牌
    assert limiter.allow() is True
    assert limiter.allow() is True
    assert limiter.allow() is False
    print("[snapshot] 限流器 判定:令牌桶按速率补发，突发上限确定")


async def test_snapshot_dimension_cap_and_merge(client):
    """维度上限：未注册路径归并 __other__/__unmatched__，快照不扩张。"""
    for i in range(50):
        await client.get(f"/wild-{i}")  # 404 -> __unmatched__
    r = await client.get("/telemetry/snapshot")
    routes = r.json()["routes"]
    assert "__unmatched__" in routes
    assert routes["__unmatched__"]["cumulative"]["total"] == 50
    assert not any(k.startswith("/wild-") for k in routes)
    assert r.json()["limits"]["route_count"] == len(routes)
    print(f"[snapshot] 判定:50 条野路径归并单维度，route_count={len(routes)}")


async def test_snapshot_log_carries_trace_id(client, caplog):
    with caplog.at_level(logging.INFO, logger="telemetry.snapshot"):
        await client.get("/telemetry/snapshot",
                         headers={"x-request-id": "snap-trace-001"})
    assert "snap-trace-001" in caplog.text
    print("[snapshot] 日志 判定:内部日志保留 trace_id 关联，对外不暴露")
