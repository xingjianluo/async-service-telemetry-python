"""异常归一化：内部细节仅入日志，对外只暴露安全分类与关联标识。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum


class ErrorKind(str, Enum):
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    DEPENDENCY_DEGRADED = "dependency_degraded"
    VALIDATION = "validation"
    INTERNAL = "internal"


_PUBLIC_MESSAGES: dict[ErrorKind, str] = {
    ErrorKind.UPSTREAM_TIMEOUT: "upstream request timed out",
    ErrorKind.UPSTREAM_UNAVAILABLE: "upstream service unavailable",
    ErrorKind.DEPENDENCY_DEGRADED: "dependency degraded",
    ErrorKind.VALIDATION: "invalid request",
    ErrorKind.INTERNAL: "internal error",
}

_PUBLIC_STATUS: dict[ErrorKind, int] = {
    ErrorKind.UPSTREAM_TIMEOUT: 504,
    ErrorKind.UPSTREAM_UNAVAILABLE: 503,
    ErrorKind.DEPENDENCY_DEGRADED: 503,
    ErrorKind.VALIDATION: 400,
    ErrorKind.INTERNAL: 500,
}


class DependencyError(Exception):
    """依赖调用失败基类。kind 决定对外分类；message 视为内部细节，不对外暴露。"""

    kind: ErrorKind = ErrorKind.INTERNAL


class UpstreamTimeoutError(DependencyError):
    kind = ErrorKind.UPSTREAM_TIMEOUT


class UpstreamUnavailableError(DependencyError):
    kind = ErrorKind.UPSTREAM_UNAVAILABLE


class DependencyDegradedError(DependencyError):
    kind = ErrorKind.DEPENDENCY_DEGRADED


@dataclass(frozen=True, slots=True)
class NormalizedError:
    kind: ErrorKind
    public_status: int
    public_message: str
    internal_detail: str  # 仅用于服务端日志


def normalize_error(exc: BaseException) -> NormalizedError:
    """把任意异常映射为可区分的安全分类。

    分类规则确定性：
    1. 业务声明的 DependencyError 子类 -> 其 kind；
    2. asyncio/内置超时 -> upstream_timeout；
    3. 其余一律 internal（细节不外泄）。
    """
    if isinstance(exc, DependencyError):
        kind = exc.kind
    elif isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        kind = ErrorKind.UPSTREAM_TIMEOUT
    else:
        kind = ErrorKind.INTERNAL
    return NormalizedError(
        kind=kind,
        public_status=_PUBLIC_STATUS[kind],
        public_message=_PUBLIC_MESSAGES[kind],
        internal_detail=f"{type(exc).__name__}: {exc}",
    )
