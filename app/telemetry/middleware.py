"""入站遥测中间件（纯 ASGI）。

职责（每个请求恰好一次）：
1. 解析/补全追踪标识并绑定上下文（contextvars，任务级隔离）；
2. 统一记录指标：正常响应与异常路径都只在 finally 中记录一次；
3. 异常归一化：对外返回安全分类 + trace_id，内部细节仅入日志；
4. 响应头回写 x-request-id，便于客户端关联。

路由维度由注入的 route_resolver 解析（starlette 不写 scope["route"]）。
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .context import bind_context, parse_inbound
from .errors import normalize_error
from .metrics import MetricsRegistry

logger = logging.getLogger("telemetry.request")

RouteResolver = Callable[[Scope], str | None]

_TRACE_HEADER_BYTES = b"x-request-id"


class TelemetryMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        metrics: MetricsRegistry,
        route_resolver: RouteResolver,
    ) -> None:
        self.app = app
        self._metrics = metrics
        self._route_resolver = route_resolver

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        raw_id = _extract_header(scope, _TRACE_HEADER_BYTES)
        ctx = parse_inbound(raw_id)
        start = time.perf_counter()
        status_code = 500  # 异常且未发响应时的兜底
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                headers = message.setdefault("headers", [])
                headers.append((_TRACE_HEADER_BYTES, ctx.trace_id.encode()))
            await send(message)

        with bind_context(ctx):
            try:
                await self.app(scope, receive, send_wrapper)
            except Exception as exc:  # noqa: BLE001 - 统一归一化
                err = normalize_error(exc)
                status_code = err.public_status
                logger.error(
                    "request failed trace_id=%s kind=%s detail=%s",
                    ctx.trace_id, err.kind.value, err.internal_detail,
                )
                if response_started:
                    raise  # 响应已流出，无法改写，只能上抛
                await _send_error_json(send, status_code, ctx.trace_id, {
                    "error": {
                        "kind": err.kind.value,
                        "message": err.public_message,
                        "trace_id": ctx.trace_id,
                    }
                })
            finally:
                duration = time.perf_counter() - start
                route = self._route_resolver(scope)
                canonical = self._metrics.record(route, status_code, duration)
                logger.info(
                    "request done trace_id=%s source=%s route=%s canonical=%s "
                    "status=%d duration=%.4fs",
                    ctx.trace_id, ctx.source, route, canonical,
                    status_code, duration,
                )


def _extract_header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return None


async def _send_error_json(send: Send, status: int, trace_id: str, payload: dict) -> None:
    body = json.dumps(payload).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (_TRACE_HEADER_BYTES, trace_id.encode()),
        ],
    })
    await send({"type": "http.response.body", "body": body})
