"""组装 FastAPI 应用：路由、遥测中间件、健康探针。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fastapi import FastAPI

from .telemetry.context import bind_context_run, get_context
from .telemetry.errors import (
    DependencyDegradedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from .telemetry.health import HealthChecker, HealthState, ProbeResult
from .telemetry.middleware import TelemetryMiddleware
from .telemetry.metrics import MetricsRegistry

PROBE_TIMEOUT = 0.2


@dataclass
class DepState:
    name: str
    state: str = "up"  # up / down / slow
    latency: float = 0.0


def create_app() -> FastAPI:
    app = FastAPI(title="async-service-telemetry")

    metrics = MetricsRegistry(max_routes=32)
    health = HealthChecker(probe_timeout=PROBE_TIMEOUT)
    primary = DepState(name="primary")
    cache = DepState(name="cache")
    app.state.metrics = metrics
    app.state.deps = {"primary": primary, "cache": cache}

    @app.get("/")
    async def root() -> dict:
        ctx = get_context()
        return {"hello": "world", "trace_id": ctx.trace_id}

    @app.get("/items/{item_id}")
    async def get_item(item_id: str) -> dict:
        ctx = get_context()
        return {"item_id": item_id, "trace_id": ctx.trace_id}

    @app.post("/work")
    async def background_work() -> dict:
        """演示追踪上下文向线程池后台任务传播。"""
        ctx = get_context()
        loop = asyncio.get_running_loop()

        def job() -> str:
            # 工作线程内读取上下文：必须与请求一致
            return get_context().trace_id

        observed = await loop.run_in_executor(
            None, bind_context_run, ctx, job
        )
        return {"trace_id": ctx.trace_id, "background_trace_id": observed}

    @app.get("/upstream/timeout")
    async def upstream_timeout() -> dict:
        # 真实超时路径：wait_for 触发 asyncio.TimeoutError
        await asyncio.wait_for(asyncio.sleep(5), timeout=0.05)
        return {"ok": True}

    @app.get("/upstream/unavailable")
    async def upstream_unavailable() -> dict:
        raise UpstreamUnavailableError("connection refused 10.0.0.1:6379")

    @app.get("/upstream/degraded")
    async def upstream_degraded() -> dict:
        raise DependencyDegradedError("cache miss storm; circuit half-open")

    @app.get("/boom")
    async def boom() -> dict:
        raise RuntimeError("secret internal detail must not leak")

    @app.get("/health/live")
    async def liveness() -> dict:
        report = await health.liveness()
        return health.public_payload(report)

    @app.get("/health/ready")
    async def readiness() -> dict:
        report = await health.readiness()
        return health.public_payload(report)

    @app.post("/debug/deps/{name}")
    async def set_dep(name: str, state: str) -> dict:
        dep = app.state.deps.get(name)
        if dep is None:
            return {"ok": False}
        dep.state = state
        return {"ok": True, "name": name, "state": state}

    async def probe_primary() -> ProbeResult:
        if primary.state == "down":
            raise UpstreamUnavailableError("primary down")
        if primary.state == "slow":
            await asyncio.sleep(PROBE_TIMEOUT * 5)
        return ProbeResult(HealthState.UP)

    async def probe_cache() -> ProbeResult:
        if cache.state == "down":
            # 非关键依赖降级：degraded 而非 down
            return ProbeResult(
                HealthState.DOWN, reason="cache_unavailable", detail="cache down"
            )
        return ProbeResult(HealthState.UP)

    health.register("primary", probe_primary, critical=True)
    health.register("cache", probe_cache, critical=False)

    # 注册已知路由维度后再挂中间件，保证维度受控
    for route in app.routes:
        path = getattr(route, "path", None)
        if path is not None:
            metrics.register_route(path)

    resolver = _make_route_resolver(app)
    app.add_middleware(TelemetryMiddleware, metrics=metrics, route_resolver=resolver)
    return app


def _make_route_resolver(app: FastAPI):
    from starlette.routing import Match

    routes = [r for r in app.routes if hasattr(r, "matches")]

    def resolve(scope) -> str | None:
        for route in routes:
            try:
                match, _ = route.matches(scope)
            except Exception:  # noqa: BLE001 - 解析失败按未知路由归并
                continue
            if match == Match.FULL:
                return getattr(route, "path", None)
        return None

    return resolve
