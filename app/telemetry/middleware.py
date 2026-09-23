"""ASGI 中间件：绑定追踪上下文、记录指标、归一化异常。

每个入站请求：
1. 解析/补全 traceparent，绑定到 contextvar（并发请求互不串扰）；
2. 响应头回写 x-request-id 与 traceparent，供客户端关联；
3. 请求结束后按路由模板记录一次时延与错误（成功/失败各一次）；
4. 未捕获异常归一化后返回对外安全的错误体，内部细节仅进日志。
"""

from __future__ import annotations

import logging
import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.telemetry.errors import normalize
from app.telemetry.metrics import MetricsRegistry
from app.telemetry.tracing import (
    REQUEST_ID_HEADER,
    TRACEPARENT_HEADER,
    bind_context,
    ensure_context,
    reset_context,
)

log = logging.getLogger("telemetry.middleware")


class TelemetryMiddleware:
    def __init__(self, app: ASGIApp, metrics: MetricsRegistry) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = {k.decode(): v.decode() for k, v in scope.get("headers", [])}
        ctx, generated = ensure_context(headers.get(TRACEPARENT_HEADER))
        token = bind_context(ctx)
        start = time.perf_counter()
        status_code = 500
        response_started = False

        async def send_with_trace(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                hdrs = MutableHeaders(scope=message)
                hdrs.append(REQUEST_ID_HEADER, ctx.request_id)
                hdrs.append(TRACEPARENT_HEADER, ctx.to_traceparent())
            await send(message)

        try:
            await self.app(scope, receive, send_with_trace)
        except BaseException as exc:
            err = normalize(exc)
            status_code = err.http_status
            log.error(
                "request failed request_id=%s reason=%s detail=%s",
                ctx.request_id,
                err.reason,
                err.log_detail,
            )
            if response_started:
                raise  # 响应已开始，无法替换为错误体
            await self._send_error(send, ctx.request_id, err.reason, err.http_status)
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            route = self._route_template(scope)
            label = self.metrics.record(
                route, latency_ms, is_error=status_code >= 500
            )
            log.info(
                "request request_id=%s method=%s path=%s route=%s label=%s "
                "status=%d latency_ms=%.2f trace_generated=%s",
                ctx.request_id,
                scope.get("method", "-"),
                scope.get("path", "-"),
                route or "-",
                label,
                status_code,
                latency_ms,
                generated,
            )
            reset_context(token)

    @staticmethod
    def _route_template(scope: Scope) -> str | None:
        """从 scope 提取路由模板；未匹配路由返回 None（归入 __unknown__）。"""
        route = scope.get("route")
        path = getattr(route, "path", None)
        return path if isinstance(path, str) else None

    @staticmethod
    async def _send_error(
        send: Send, request_id: str, reason: str, status: int
    ) -> None:
        # 对外只暴露原因码与请求标识，不含内部异常细节
        body = (
            f'{{"error": "{reason}", "request_id": "{request_id}"}}'
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send(
            {"type": "http.response.body", "body": body, "more_body": False}
        )
