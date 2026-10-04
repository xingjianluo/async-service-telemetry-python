"""组装 FastAPI 应用：路由、遥测中间件、健康探针。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .telemetry.context import bind_context_run, get_context
from .telemetry.errors import (
    DependencyDegradedError,
    UpstreamUnavailableError,
)
from .telemetry.health import HealthChecker, HealthState, ProbeResult
from .telemetry.metrics import MetricsRegistry
from .telemetry.middleware import TelemetryMiddleware
from .telemetry.snapshot import SnapshotRateLimiter, TelemetrySnapshotter
from .telemetry.windows import SlidingWindowMetrics

PROBE_TIMEOUT = 0.2
WINDOW_SECONDS = 300.0
BUCKET_SECONDS = 10.0
SNAPSHOT_RATE_PER_SECOND = 5.0
SNAPSHOT_BURST = 10


@dataclass
class DepState:
    name: str
    state: str = "up"  # up / down / slow
    latency: float = 0.0


def create_app() -> FastAPI:
    app = FastAPI(title="async-service-telemetry")

    metrics = MetricsRegistry(max_routes=32)
    windows = SlidingWindowMetrics(
        window_seconds=WINDOW_SECONDS, bucket_seconds=BUCKET_SECONDS
    )
    health = HealthChecker(probe_timeout=PROBE_TIMEOUT)
    primary = DepState(name="primary")
    cache = DepState(name="cache")
    app.state.metrics = metrics
    app.state.windows = windows
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
        """演示追踪上下文（含采样决定）向线程池后台任务传播。"""
        ctx = get_context()
        loop = asyncio.get_running_loop()

        def job() -> tuple[str, bool]:
            # 工作线程内读取上下文：必须与请求一致（含采样决定）
            inner = get_context()
            return inner.trace_id, inner.sampled

        observed_id, observed_sampled = await loop.run_in_executor(
            None, bind_context_run, ctx, job
        )
        return {
            "trace_id": ctx.trace_id,
            "sampled": ctx.sampled,
            "background_trace_id": observed_id,
            "background_sampled": observed_sampled,
        }

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

    snapshotter = TelemetrySnapshotter(
        metrics=metrics,
        windows=windows,
        readiness=health.readiness,
        limiter=SnapshotRateLimiter(
            rate_per_second=SNAPSHOT_RATE_PER_SECOND, burst=SNAPSHOT_BURST
        ),
    )
    app.state.snapshotter = snapshotter

    @app.get("/telemetry/snapshot")
    async def telemetry_snapshot() -> JSONResponse:
        """对外只读遥测快照：脱敏、限流、就绪门控。"""
        resp = await snapshotter.build()
        return JSONResponse(resp.payload, status_code=resp.status_code)

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
    app.add_middleware(
        TelemetryMiddleware,
        metrics=metrics,
        route_resolver=resolver,
        windows=windows,
    )
    return app


def _make_route_resolver(app: FastAPI):
    from starlette.routing import Match

    routes = [r for r in app.routes if hasattr(r, "matches")]

    def resolve(scope) -> str | None:
        for route in routes:
            try:
                match, _ = route.matches(scope)
            except Exception:  # noqa: BLE001, S112 - 解析失败按未知路由归并
                continue
            if match == Match.FULL:
                return getattr(route, "path", None)
        return None

    return resolve
