"""请求级追踪上下文：解析、生成与传播。

传播模型：
- 入站请求携带 W3C ``traceparent`` 头时解析复用其 trace_id；
- 缺失或非法时确定性地生成新 trace_id（不静默透传非法值）；
- 上下文通过 contextvars 在协程间传播，并发请求互不串扰；
- 下游调用 / 后台任务通过 ``child()`` 派生 span，保持同一 trace_id。
"""

from __future__ import annotations

import contextvars
import re
import secrets
from dataclasses import dataclass

TRACEPARENT_HEADER = "traceparent"
REQUEST_ID_HEADER = "x-request-id"

_TRACEPARENT_RE = re.compile(
    r"^[\da-f]{2}-([\da-f]{32})-([\da-f]{16})-([\da-f]{2})$"
)


@dataclass(frozen=True, slots=True)
class TraceContext:
    """单个请求链路的追踪标识。"""

    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    sampled: bool = True

    @property
    def request_id(self) -> str:
        """对外暴露的请求标识（取 trace_id 前 16 位）。"""
        return self.trace_id[:16]

    def child(self) -> TraceContext:
        """派生下游调用/后台任务使用的子上下文。"""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=_new_span_id(),
            parent_span_id=self.span_id,
            sampled=self.sampled,
        )

    def to_traceparent(self) -> str:
        """序列化为 W3C traceparent 头。"""
        flags = "01" if self.sampled else "00"
        return f"00-{self.trace_id}-{self.span_id}-{flags}"


def _new_trace_id() -> str:
    return secrets.token_hex(16)


def _new_span_id() -> str:
    return secrets.token_hex(8)


def parse_traceparent(value: str | None) -> TraceContext | None:
    """解析 W3C traceparent；非法输入返回 None 而不是抛出。

    全零 trace_id / span_id 按规范视为非法。
    """
    if not value:
        return None
    m = _TRACEPARENT_RE.match(value.strip())
    if not m:
        return None
    trace_id, span_id, flags = m.groups()
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None
    return TraceContext(
        trace_id=trace_id,
        span_id=span_id,
        sampled=bool(int(flags, 16) & 1),
    )


def ensure_context(traceparent: str | None) -> tuple[TraceContext, bool]:
    """解析入站 traceparent；缺失或非法时确定性地补全。

    返回 (上下文, 是否由本服务补全)。补全策略：生成全新 trace_id 与
    span_id，绝不原样透传非法标识。
    """
    parsed = parse_traceparent(traceparent)
    if parsed is not None:
        # 入站 span 作为父 span，本服务派生自己的 span。
        return (
            TraceContext(
                trace_id=parsed.trace_id,
                span_id=_new_span_id(),
                parent_span_id=parsed.span_id,
                sampled=parsed.sampled,
            ),
            False,
        )
    return TraceContext(trace_id=_new_trace_id(), span_id=_new_span_id()), True


_context: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "telemetry_trace_context", default=None
)


def current_context() -> TraceContext | None:
    """读取当前协程上下文的追踪上下文。"""
    return _context.get()


def bind_context(ctx: TraceContext) -> contextvars.Token[TraceContext | None]:
    """将上下文绑定到当前协程上下文，返回可复位的 token。"""
    return _context.set(ctx)


def reset_context(token: contextvars.Token[TraceContext | None]) -> None:
    """复位上下文绑定。"""
    _context.reset(token)
