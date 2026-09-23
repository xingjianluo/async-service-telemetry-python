import asyncio
import logging

import pytest

from app.telemetry.tracing import (
    bind_context,
    current_context,
    ensure_context,
    parse_traceparent,
    reset_context,
)

log = logging.getLogger("test.tracing")


def test_parse_valid_traceparent():
    ctx = parse_traceparent(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    )
    assert ctx is not None
    assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert ctx.parent_span_id is None
    assert ctx.sampled is True


@pytest.mark.parametrize(
    "bad",
    [
        None,
        "",
        "garbage",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",  # 缺 flags
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",  # 全零 trace
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",  # 全零 span
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",  # 大写非法
    ],
)
def test_parse_invalid_returns_none(bad):
    assert parse_traceparent(bad) is None


def test_ensure_context_fills_deterministically():
    ctx, generated = ensure_context("not-a-traceparent")
    assert generated is True
    assert len(ctx.trace_id) == 32 and len(ctx.span_id) == 16
    # 非法输入绝不透传
    assert "not-a-traceparent" not in ctx.to_traceparent()
    log.info("request_id=%s generated=%s", ctx.request_id, generated)


def test_ensure_context_reuses_valid_trace_id():
    ctx, generated = ensure_context(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    )
    assert generated is False
    assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert ctx.parent_span_id == "00f067aa0ba902b7"


def test_child_keeps_trace_id():
    ctx, _ = ensure_context(None)
    child = ctx.child()
    assert child.trace_id == ctx.trace_id
    assert child.parent_span_id == ctx.span_id
    assert child.span_id != ctx.span_id


@pytest.mark.asyncio
async def test_no_cross_talk_between_concurrent_tasks():
    """并发协程各自绑定上下文，互不串扰。"""
    barrier = asyncio.Barrier(20)
    seen: dict[str, str] = {}

    async def worker(name: str, traceparent: str | None) -> None:
        ctx, generated = ensure_context(traceparent)
        token = bind_context(ctx)
        try:
            await barrier.wait()  # 让所有协程交错执行
            await asyncio.sleep(0)
            got = current_context()
            assert got is ctx
            seen[name] = got.trace_id
            log.info(
                "worker=%s request_id=%s generated=%s", name, got.request_id, generated
            )
        finally:
            reset_context(token)

    async with asyncio.TaskGroup() as tg:
        for i in range(20):
            tp = f"00-{i + 1:032x}-{i + 1:016x}-01" if i % 2 == 0 else None
            tg.create_task(worker(f"w{i}", tp))

    # 每个协程看到的都是自己绑定的 trace_id
    for i in range(20):
        expected = f"{i + 1:032x}" if i % 2 == 0 else seen[f"w{i}"]
        assert seen[f"w{i}"] == expected
    # 补全的 trace_id 互不相同
    generated_ids = [seen[f"w{i}"] for i in range(1, 20, 2)]
    assert len(set(generated_ids)) == len(generated_ids)
    assert current_context() is None
