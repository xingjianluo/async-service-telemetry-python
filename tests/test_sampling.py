"""采样决定单测：入口唯一决定、传播一致、确定性降级、不影响指标计数。"""
from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from app.service import create_app
from app.telemetry.context import (
    TraceContext,
    bind_context,
    get_context,
    propagation_headers,
    run_propagated,
)
from app.telemetry.sampling import (
    DECISION_DEFAULT,
    DECISION_INBOUND,
    decide_sampling,
    format_sampled_header,
    parse_sampled_header,
)


@pytest.fixture
async def client():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        c.app = app  # type: ignore[attr-defined]
        yield c


def test_parse_header_strict():
    assert parse_sampled_header("1") is True
    assert parse_sampled_header("true") is True
    assert parse_sampled_header("0") is False
    assert parse_sampled_header("false") is False
    assert parse_sampled_header(None) is None
    assert parse_sampled_header("yes") is None
    assert parse_sampled_header("") is None
    print("[sampling] 判定:仅接受 1/true/0/false，其余一律走降级")


def test_decide_sampling_deterministic_fallback():
    assert decide_sampling("1") == (True, DECISION_INBOUND)
    assert decide_sampling("0") == (False, DECISION_INBOUND)
    # 缺失/非法 -> 配置默认值，同一配置下结果确定
    assert decide_sampling(None, default=True) == (True, DECISION_DEFAULT)
    assert decide_sampling("garbage", default=False) == (False, DECISION_DEFAULT)
    assert decide_sampling(None, default=False) == (False, DECISION_DEFAULT)
    print("[sampling] 判定:头部合法遵从头部，非法/缺失确定性落到配置默认")


def test_decision_frozen_within_trace():
    """上下文冻结：链路中途无法把采样决定改劈叉。"""
    ctx = TraceContext(trace_id="frozen-trace-01", source="inbound", sampled=False)
    with bind_context(ctx):
        assert get_context().sampled is False
        # 传播到“后台任务”仍是同一决定
        observed = run_propagated(lambda: get_context().sampled)
        assert observed is False
        # frozen dataclass 不允许篡改
        with pytest.raises(AttributeError):
            ctx.sampled = True  # type: ignore[misc]
    print("[sampling] 判定:frozen 上下文，链路内决定不可拆分、不可篡改")


def test_propagation_headers_carry_decision():
    ctx = TraceContext(trace_id="prop-trace-0001", source="inbound", sampled=False)
    headers = propagation_headers(ctx)
    assert headers["x-request-id"] == "prop-trace-0001"
    assert headers["x-trace-sampled"] == "0"
    assert format_sampled_header(True) == "1"
    print(f"[sampling] 判定:下游传播头 {headers}")


async def test_inbound_header_honored_and_echoed(client):
    r1 = await client.get("/", headers={"x-trace-sampled": "0"})
    assert r1.headers["x-trace-sampled"] == "0"
    r2 = await client.get("/", headers={"x-trace-sampled": "1"})
    assert r2.headers["x-trace-sampled"] == "1"
    print("[sampling] 判定:入站采样头被遵从并回写响应头")


async def test_invalid_or_missing_header_falls_back_deterministically(client):
    # 默认 default_sampled=True：缺失与非法都确定性得到 1
    for headers in ({}, {"x-trace-sampled": "maybe"}, {"x-trace-sampled": "2"}):
        r = await client.get("/", headers=headers)
        assert r.headers["x-trace-sampled"] == "1"
    print("[sampling] 判定:缺失/非法头部确定性降级为默认（不随机、不时有时无）")


async def test_sampling_propagates_to_background_task(client):
    r = await client.post("/work", headers={
        "x-request-id": "bg-trace-00001",
        "x-trace-sampled": "0",
    })
    body = r.json()
    assert body["trace_id"] == "bg-trace-00001"
    assert body["sampled"] is False
    assert body["background_trace_id"] == "bg-trace-00001"
    assert body["background_sampled"] is False
    print("[sampling] 判定:采样决定随后台任务传播，trace_id 与 sampled 均一致")


async def test_mixed_sampling_concurrent_metrics_unaffected(client):
    """并发混合采样/不采样请求：指标与窗口都恰好计一次，与采样无关。"""
    async def one(i: int) -> None:
        sampled = "1" if i % 2 else "0"
        r = await client.get("/", headers={
            "x-request-id": f"mix-{i:04d}-{'b' * 16}",
            "x-trace-sampled": sampled,
        })
        assert r.headers["x-trace-sampled"] == sampled

    await asyncio.gather(*(one(i) for i in range(100)))
    m = client.app.state.metrics.snapshot()["/"]
    assert m.total == 100  # 采样不采样都计入，且只计一次
    w = client.app.state.windows.snapshot("/")
    assert w.total == m.total  # 窗口与累计对账一致
    print(f"[sampling] 并发混合 判定:100 笔（采样各半）metrics={m.total} "
          f"window={w.total}，不重不丢")


async def test_request_log_carries_sampling_verdict(client, caplog):
    with caplog.at_level(logging.INFO, logger="telemetry.request"):
        await client.get("/", headers={
            "x-request-id": "slog-00000001",
            "x-trace-sampled": "0",
        })
        await client.get("/", headers={"x-request-id": "slog-00000002"})
    text = caplog.text
    assert "slog-00000001" in text and "sampled=False" in text
    assert "sampled_source=inbound" in text
    assert "slog-00000002" in text and "sampled_source=default" in text
    print("[sampling] 日志 判定:包含 trace_id、采样值与判定依据来源")
