"""应用装配：中间件、路由、健康检查。"""

from __future__ import annotations

import asyncio

from fastapi import FastAPI

from app.routes import router
from app.telemetry.health import Dependency, HealthChecker
from app.telemetry.metrics import MetricsRegistry
from app.telemetry.middleware import TelemetryMiddleware


async def _ping() -> None:
    await asyncio.sleep(0)


def create_app() -> FastAPI:
    app = FastAPI(title="async-service-telemetry")
    metrics = MetricsRegistry(max_routes=100)
    health = HealthChecker()
    health.register(Dependency("self", _ping, critical=True, timeout_s=1.0))
    app.state.metrics = metrics
    app.state.health = health
    app.add_middleware(TelemetryMiddleware, metrics=metrics)
    app.include_router(router)
    return app


app = create_app()
