"""错误归一化：内部异常 -> 对外安全的原因码与关联信息。

原则：
- 对外只暴露稳定的原因码（reason），不泄露异常消息、堆栈、内部标识；
- 内部细节（异常类型与消息）只进服务端日志（log_detail），
  日志同时携带 request_id 以便关联定位。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


class UpstreamError(Exception):
    """下游依赖调用失败。"""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class DependencyUnavailableError(Exception):
    """依赖不可用（连接拒绝、DNS 失败等）。"""


@dataclass(frozen=True, slots=True)
class NormalizedError:
    reason: str          # 稳定的原因码，如 "timeout" / "upstream_5xx" / "internal"
    log_detail: str      # 仅用于服务端日志的定位信息
    http_status: int = 500


def normalize(exc: BaseException) -> NormalizedError:
    """把任意异常归一化为对外安全的原因码，内部细节只进日志。"""
    detail = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return NormalizedError(reason="timeout", log_detail=detail, http_status=504)
    if isinstance(exc, DependencyUnavailableError):
        return NormalizedError(
            reason="dependency_unavailable", log_detail=detail, http_status=503
        )
    if isinstance(exc, UpstreamError):
        if exc.status_code is not None and exc.status_code >= 500:
            return NormalizedError(
                reason="upstream_5xx", log_detail=detail, http_status=502
            )
        return NormalizedError(
            reason="upstream_error", log_detail=detail, http_status=502
        )
    if isinstance(exc, ConnectionError):
        return NormalizedError(
            reason="connection_failed", log_detail=detail, http_status=503
        )
    return NormalizedError(reason="internal", log_detail=detail, http_status=500)
