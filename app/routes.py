"""演示路由：下游调用、后台任务、受控失败、健康与指标端点。"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Request, Response

from app.telemetry.errors import DependencyUnavailableError, UpstreamError
from app.telemetry.health import Status
from app.telemetry.tracing import current_context

log = logging.getLogger("app.routes")
router = APIRouter()


def _ctx_info() -> dict[str, str | None]:
    ctx = current_context()
    if ctx is None:
        return {"request_id": None, "traceparent": None}
    return {"request_id": ctx.request_id, "traceparent": ctx.to_traceparent()}


async def _call_downstream() -> dict[str, str | None]:
    """模拟下游调用：派生子上下文并携带 traceparent 传播。"""
    ctx = current_context()
    child = ctx.child() if ctx else None
    await asyncio.sleep(0)  # 模拟 IO
    return {
        "downstream_traceparent": child.to_traceparent() if child else None,
    }


@router.get("/")
async def read_root() -> dict[str, object]:
    return {"Hello": "World", **_ctx_info()}


@router.get("/items/{item_id}")
async def read_item(item_id: int) -> dict[str, object]:
    return {"item_id": item_id, **_ctx_info()}


@router.get("/work")
async def do_work() -> dict[str, object]:
    """触发一次下游调用与一个后台任务，验证上下文传播。"""
    downstream = await _call_downstream()

    done = asyncio.Event()
    background: dict[str, str | None] = {}

    async def background_task() -> None:
        # 后台任务继承创建时的协程上下文（contextvars 拷贝语义）
        info = _ctx_info()
        background.update(info)
        log.info("background task request_id=%s", info["request_id"])
        done.set()

    task = asyncio.create_task(background_task())
    await done.wait()
    await task
    return {"downstream": downstream, "background": background, **_ctx_info()}


@router.get("/fail/{kind}")
async def fail(kind: str) -> dict[str, object]:
    """按 kind 触发受控失败：timeout / upstream / unavailable / internal。"""
    if kind == "timeout":
        await asyncio.sleep(0)
        raise TimeoutError("simulated downstream timeout after 1.0s")
    if kind == "upstream":
        raise UpstreamError("billing returned 503", status_code=503)
    if kind == "unavailable":
        raise DependencyUnavailableError("pg: connection refused 10.0.0.1:5432")
    raise RuntimeError("unexpected internal state: shard=7 secret=abc123")


@router.get("/healthz")
async def healthz(request: Request) -> dict[str, object]:
    report = await request.app.state.health.check()
    return {
        "status": report.status.value,
        "checks": [
            {"name": c.name, "status": c.status.value, "reason": c.reason}
            for c in report.checks
        ],
    }


@router.get("/readyz")
async def readyz(request: Request, response: Response) -> dict[str, object]:
    report = await request.app.state.health.check()
    if report.status is Status.UNHEALTHY:
        response.status_code = 503
    return {"status": report.status.value}


@router.get("/metrics")
async def metrics(request: Request) -> dict[str, object]:
    snap = request.app.state.metrics.snapshot()
    return {
        route: {
            "count": s.count,
            "error_count": s.error_count,
            "error_rate": round(s.error_rate, 4),
            "latency_sum_ms": round(s.latency_sum_ms, 3),
            "latency_max_ms": round(s.latency_max_ms, 3),
            "buckets": {str(k): v for k, v in sorted(s.buckets.items())},
        }
        for route, s in sorted(snap.items())
    }
