"""入站遥测中间件（纯 ASGI）。

职责（每个请求恰好一次）：
1. 解析/补全追踪标识并绑定上下文（contextvars，任务级隔离）；
2. 采样决定在入站时一次性做出并写入上下文，随响应头回写、
   随传播头带往下游/后台，整条链路一致；
3. 统一记录指标：累计（MetricsRegistry）与窗口（WindowedMetrics）
   都在 finally 中恰好记录一次，成功与异常路径不重不漏；
4. 异常归一化：对外返回安全分类 + trace_id，内部细节仅入日志；
5. 响应头回写 x-request-id 与 x-trace-sampled，便于客户端关联。

路由维度由注入的 route_resolver 解析（starlette 不写 scope["route"]）。
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import replace

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .context import bind_context, parse_inbound
from .errors import normalize_error
from .metrics import MetricsRegistry
from .sampling import (
    SamplingPolicy,
    decide_sampling,
    parse_sampled_header,
)
from .window import WindowedMetrics

logger = logging.getLogger("telemetry.request")

RouteResolver = Callable[[Scope], str | None]

_TRACE_HEADER_BYTES = b"x-request-id"
_SAMPLED_HEADER_BYTES = b"x-trace-sampled"


class TelemetryMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        metrics: MetricsRegistry,
        route_resolver: RouteResolver,
        windowed: WindowedMetrics | None = None,
        sampling_policy: SamplingPolicy | None = None,
    ) -> None:
        self.app = app
        self._metrics = metrics
        self._route_resolver = route_resolver
        self._windowed = windowed
        self._policy = sampling_policy or SamplingPolicy()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        raw_id = _extract_header(scope, _TRACE_HEADER_BYTES)
        ctx = parse_inbound(raw_id)
        # 采样：入站头优先；缺失/非法走确定性降级，一次性决定、链路恒定
        raw_sampled = _extract_header(scope, _SAMPLED_HEADER_BYTES)
        explicit = parse_sampled_header(raw_sampled)
        sampled, basis = decide_sampling(explicit, ctx.trace_id, self._policy)
        ctx = replace(ctx, sampled=sampled)

        start = time.perf_counter()
        status_code = 500  # 异常且未发响应时的兜底
        error_kind: str | None = None
        response_started = False
        sampled_value = b"1" if ctx.sampled else b"0"

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                headers = message.setdefault("headers", [])
                headers.append((_TRACE_HEADER_BYTES, ctx.trace_id.encode()))
                headers.append((_SAMPLED_HEADER_BYTES, sampled_value))
            await send(message)

        with bind_context(ctx):
            try:
                await self.app(scope, receive, send_wrapper)
            except Exception as exc:
                err = normalize_error(exc)
                status_code = err.public_status
                error_kind = err.kind.value
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
                canonical = self._metrics.record(route, status_code, duration)
                if error_kind is None and status_code >= 400:
                    error_kind = "http_4xx" if status_code < 500 else "http_5xx"
                if self._windowed is not None:
                    self._windowed.record(
                        canonical, status_code, duration, error_kind=error_kind
                    )
                logger.info(
                    "request done trace_id=%s source=%s route=%s canonical=%s "
                    "status=%d duration=%.4fs sampled=%s sampling_basis=%s",
                    ctx.trace_id, ctx.source, route, canonical,
                    status_code, duration, ctx.sampled, basis,
                )
                if ctx.sampled:
                    # 详细追踪（span 级）仅对采样链路开启；指标不受采样影响
                    logger.debug(
                        "trace span trace_id=%s route=%s status=%d duration=%.4fs",
                        ctx.trace_id, route, status_code, duration,
                    )


def _extract_header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return None


async def _send_error_json(send: Send, status: int, ctx, payload: dict) -> None:
    body = json.dumps(payload).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (_TRACE_HEADER_BYTES, ctx.trace_id.encode()),
            (_SAMPLED_HEADER_BYTES, b"1" if ctx.sampled else b"0"),
        ],
    })
    await send({"type": "http.response.body", "body": body})
