"""请求级追踪上下文（基于 contextvars，asyncio 任务隔离）。

- 每个入站请求绑定一个 TraceContext；
- contextvars 保证并发/交错请求各自独立，不会串扰；
- 下游调用与后台任务通过 run_propagated() 显式继承当前快照；
- 入站标识缺失或非法时确定性补全（generated），绝不静默透传非法值。
"""
from __future__ import annotations

import contextvars
import re
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

TRACE_HEADER = "x-request-id"
TRACE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{8,128}$")

SOURCE_INBOUND = "inbound"
SOURCE_GENERATED = "generated"


@dataclass(frozen=True, slots=True)
class TraceContext:
    """单次请求的追踪上下文。"""

    trace_id: str
    source: str


_current: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "trace_context", default=None
)


def new_trace_id() -> str:
    """生成合规的新追踪标识（uuid4 hex，32 字符，确定性规则、随机性取值）。"""
    return uuid.uuid4().hex


def is_valid_trace_id(value: str | None) -> bool:
    return isinstance(value, str) and bool(TRACE_ID_PATTERN.fullmatch(value))


def parse_inbound(raw: str | None) -> TraceContext:
    """解析入站标识；缺失或非法时补全为 generated 上下文。"""
    if is_valid_trace_id(raw):
        return TraceContext(trace_id=raw, source=SOURCE_INBOUND)
    return TraceContext(trace_id=new_trace_id(), source=SOURCE_GENERATED)


def get_context() -> TraceContext:
    """读取当前上下文；未绑定时抛出明确异常（防止后台任务裸跑丢上下文）。"""
    ctx = _current.get()
    if ctx is None:
        raise LookupError("trace context is not bound to the current execution context")
    return ctx


def try_get_context() -> TraceContext | None:
    return _current.get()


def set_context(ctx: TraceContext) -> contextvars.Token[TraceContext | None]:
    return _current.set(ctx)


@contextmanager
def bind_context(ctx: TraceContext) -> Iterator[None]:
    """在作用域内绑定上下文，退出时恢复（含异常路径）。"""
    token = _current.set(ctx)
    try:
        yield
    finally:
        _current.reset(token)


def run_propagated(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """以当前上下文快照运行函数（供后台任务/线程池使用）。

    未绑定上下文时不伪造 id，直接抛出——调用方应先绑定。
    """
    ctx = get_context()
    return bind_context_run(ctx, func, *args, **kwargs)


def bind_context_run(ctx: TraceContext, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    with bind_context(ctx):
        return func(*args, **kwargs)
