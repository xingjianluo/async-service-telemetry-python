"""采样决策单测：显式优先、确定性降级、传播一致、链路不拆分。"""
from __future__ import annotations

import asyncio

import pytest

from app.telemetry.context import (
    TraceContext,
    bind_context,
    get_context,
    run_propagated,
)
from app.telemetry.sampling import (
    BASIS_INBOUND,
    BASIS_POLICY,
    SamplingPolicy,
    decide_sampling,
    parse_sampled_header,
    propagation_headers,
)


def test_parse_header_values():
    assert parse_sampled_header("1") is True
    assert parse_sampled_header("true") is True
    assert parse_sampled_header("TRUE") is True
    assert parse_sampled_header("0") is False
    assert parse_sampled_header("false") is False
    for bad in (None, "", "yes", "2", "maybe", "1 "):
        # "1 " strip 后合法；其余非法
        expected = True if bad == "1 " else None
        assert parse_sampled_header(bad) is expected, bad
    print("[sampling] 判定:合法值解析正确，非法值返回 None 走降级")


def test_explicit_decision_wins():
    policy = SamplingPolicy(default_rate=0.0)
    assert decide_sampling(True, "any-trace", policy) == (True, BASIS_INBOUND)
    policy = SamplingPolicy(default_rate=1.0)
    assert decide_sampling(False, "any-trace", policy) == (False, BASIS_INBOUND)
    print("[sampling] 判定:显式决定优先于策略默认值")


def test_fallback_is_deterministic_per_trace():
    policy = SamplingPolicy(default_rate=0.5)
    tid = "deterministic-trace-id-01"
    first = decide_sampling(None, tid, policy)
    for _ in range(100):
        assert decide_sampling(None, tid, policy) == first
    assert first[1] == BASIS_POLICY
    # 不同 trace_id 的判定应近似按比例分布（确定性哈希，不随机漂移）
    samples = [decide_sampling(None, f"trace-{i:05d}", policy)[0] for i in range(2000)]
    ratio = sum(samples) / len(samples)
    assert 0.4 < ratio < 0.6, ratio
    print(f"[sampling] 判定:降级对同一 trace_id 恒定；2000 个 id 采样率={ratio:.3f}")


def test_policy_extremes():
    assert decide_sampling(None, "t", SamplingPolicy(1.0)) == (True, BASIS_POLICY)
    assert decide_sampling(None, "t", SamplingPolicy(0.0)) == (False, BASIS_POLICY)
    with pytest.raises(ValueError):
        SamplingPolicy(1.5)
    print("[sampling] 判定:rate=1 全采、rate=0 全不采、非法 rate 拒绝")


def test_propagation_headers_roundtrip():
    headers = propagation_headers("trace-abc-123", sampled=False)
    assert headers["x-request-id"] == "trace-abc-123"
    assert headers["x-trace-sampled"] == "0"
    # 下游解析回来必须得到同一决定（链路不拆分）
    assert parse_sampled_header(headers["x-trace-sampled"]) is False
    headers = propagation_headers("trace-abc-123", sampled=True)
    assert parse_sampled_header(headers["x-trace-sampled"]) is True
    print("[sampling] 判定:传播头往返一致，决定不被拆分")


async def test_sampled_flag_propagates_to_background():
    ctx = TraceContext(trace_id="bg-trace-0001", source="inbound", sampled=False)
    with bind_context(ctx):
        async def job():
            await asyncio.sleep(0)
            inner = get_context()
            return inner.trace_id, inner.sampled

        tid, sampled = await run_propagated(job)
    assert tid == "bg-trace-0001"
    assert sampled is False  # 后台任务继承同一采样决定
    print("[sampling] 判定:采样决定随上下文传入后台任务，保持一致")
