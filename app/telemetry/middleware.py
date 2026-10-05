"""入站遥测中间件（纯 ASGI）。

职责（每个请求恰好一次）：
1. 解析/补全追踪标识、做出采样决定并绑定上下文（contextvars，任务级隔离）；
2. 统一记录指标：正常响应与异常路径都只在 finally 中记录一次，
   累计指标与滑动窗口在同一点位、以同一失败原因记录，保证按原因对账一致；
3. 异常归一化：对外返回安全分类 + trace_id，内部细节仅入日志；
   异常路径的失败原因取归一化分类（ErrorKind 值，有界词汇）；
4. 响应头回写 x-request-id 与 x-trace-sampled：正常与异常（含超时、
   下游不可用、内部异常）路径都回写，采样标记与入站决定始终一致。

采样只影响追踪详细度，不影响指标计数；窗口按请求完成时间落桶，
在途请求归属确定，不重不丢。

路由维度由注入的 route_resolver 解析（starlette 不写 scope["route"]）。
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .context import TraceContext, bind_context, parse_inbound
from .errors import normalize_error
from .metrics import MetricsRegistry
from .sampling import SAMPLED_BYTES
from .windows import SlidingWindowMetrics

logger = logging.getLogger("telemetry.request")

RouteResolver = Callable[[Scope], str | None]

_TRACE_HEADER_BYTES = b"x-request-id"


class TelemetryMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        metrics: MetricsRegistry,
        route_resolver: RouteResolver,
        windows: SlidingWindowMetrics | None = None,
        default_sampled: bool = True,
    ) -> None:
        self.app = app
        self._metrics = metrics
        self._route_resolver = route_resolver
        self._windows = windows
        self._default_sampled = default_sampled

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        raw_id = _extract_header(scope, _TRACE_HEADER_BYTES)
        raw_sampled = _extract_header(scope, SAMPLED_BYTES)
        ctx = parse_inbound(raw_id, raw_sampled, default_sampled=self._default_sampled)
        start = time.perf_counter()
        status_code = 500  # 异常且未发响应时的兜底
        reason: str | None = None  # 失败原因；成功请求保持 None
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                headers = message.setdefault("headers", [])
                headers.append((_TRACE_HEADER_BYTES, ctx.trace_id.encode()))
                headers.append((SAMPLED_BYTES, b"1" if ctx.sampled else b"0"))
            await send(message)

        with bind_context(ctx):
            try:
                await self.app(scope, receive, send_wrapper)
            except Exception as exc:
                err = normalize_error(exc)
                status_code = err.public_status
                reason = err.kind.value  # 失败原因：归一化分类（有界词汇）
                logger.error(
                    "request failed trace_id=%s kind=%s detail=%s",
                    ctx.trace_id, err.kind.value, err.internal_detail,
                )
                if response_started:
                    raise  # 响应已流出，无法改写，只能上抛
                await _send_error_json(send, status_code, ctx, {
                    "error": {
                        "kind": err.kind.value,
                        "message": err.public_message,
                        "trace_id": ctx.trace_id,
                    }
                })
            finally:
                duration = time.perf_counter() - start
                route = self._route_resolver(scope)
                canonical = self._metrics.record(route, status_code, duration,
                                                 reason=reason)
                if self._windows is not None:
                    # 与累计指标同一点位、同一原因、按完成时间落桶：恰好一次
                    self._windows.record(canonical, status_code, duration,
                                         reason=reason)
                logger.info(
                    "request done trace_id=%s source=%s route=%s canonical=%s "
                    "status=%d duration=%.4fs sampled=%s sampled_source=%s",
                    ctx.trace_id, ctx.source, route, canonical,
                    status_code, duration, ctx.sampled, ctx.sampled_source,
                )


def _extract_header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return None


async def _send_error_json(send: Send, status: int, ctx: TraceContext, payload: dict) -> None:
    """异常路径的统一错误响应。

    与正常响应一样回写 x-request-id 与 x-trace-sampled：采样标记在所有
    路径（含超时/下游不可用/内部异常）都与入站决定保持一致，不时有时无。
    """
    body = json.dumps(payload).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (_TRACE_HEADER_BYTES, ctx.trace_id.encode()),
            (SAMPLED_BYTES, b"1" if ctx.sampled else b"0"),
        ],
    })
    await send({"type": "http.response.body", "body": body})
