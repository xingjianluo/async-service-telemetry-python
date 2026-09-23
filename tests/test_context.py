"""追踪上下文单元测试：并发隔离、非法补全、向后台传播。"""
from __future__ import annotations

import asyncio
import re

import pytest

from app.telemetry.context import (
    bind_context,
    get_context,
    new_trace_id,
    parse_inbound,
    run_propagated,
    try_get_context,
    TraceContext,
)


def test_generate_when_missing_or_invalid(caplog):
    for raw in (None, "", "short", "bad id!!", "x" * 129):
        ctx = parse_inbound(raw)
        assert ctx.source == "generated"
        assert re.fullmatch(r"[0-9a-f]{32}", ctx.trace_id)
        print(f"[trace] raw={raw!r} -> 判定:补全 trace_id={ctx.trace_id}")

    ctx = parse_inbound("valid-id-1234")
    assert ctx.source == "inbound"
    assert ctx.trace_id == "valid-id-1234"
    print(f"[trace] 判定:透放入站 trace_id={ctx.trace_id}")


def test_generated_ids_unique():
    ids = {new_trace_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_missing_context_raises():
    assert try_get_context() is None
    with pytest.raises(LookupError):
        get_context()


async def test_concurrent_context_isolation(caplog):
    """N 个交错请求并发推进，每个协程内读到的标识必须始终是自己的。"""
    n = 200
    observations: dict[int, str] = {}

    async def request(idx: int, trace_id: str):
        ctx = TraceContext(trace_id=trace_id, source="inbound")
        with bind_context(ctx):
            # 多次让出事件循环，制造最大交错
            for _ in range(10):
                await asyncio.sleep(0)
                seen = get_context().trace_id
                assert seen == trace_id, f"串扰! 协程{idx} 读到 {seen}"
            observations[idx] = get_context().trace_id
        print(f"[trace] 并发协程{idx} trace_id={trace_id} 判定:全程一致")

    await asyncio.gather(
        *(request(i, f"trace-{i:04d}-abcdef") for i in range(n))
    )
    assert len(set(observations.values())) == n
    # 交错结束后上下文不得泄漏到请求作用域之外
    assert try_get_context() is None


async def test_propagate_to_background_task():
    with bind_context(TraceContext(trace_id="parent-trace-0001", source="inbound")):
        async def job():
            await asyncio.sleep(0)
            return get_context().trace_id

        result = await run_propagated(job)
        print(f"[trace] 后台任务读取 trace_id={result} 判定:与父请求一致")
        assert result == "parent-trace-0001"


async def test_propagate_to_thread_pool():
    import functools

    from app.telemetry.context import bind_context_run

    ctx = TraceContext(trace_id="thread-trace-0002", source="generated")
    loop = asyncio.get_running_loop()

    def sync_job():
        return get_context().trace_id

    with bind_context(ctx):
        result = await loop.run_in_executor(None, functools.partial(bind_context_run, ctx, sync_job))
    print(f"[trace] 线程池任务读取 trace_id={result} 判定:与请求一致")
    assert result == "thread-trace-0002"
