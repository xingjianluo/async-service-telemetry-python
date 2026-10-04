"""对外快照单测：内容对照、脱敏、限流、缓存、就绪门控。"""
from __future__ import annotations

import pytest

from app.telemetry.health import HealthChecker, HealthState, ProbeResult
from app.telemetry.metrics import MetricsRegistry
from app.telemetry.snapshot import (
    SnapshotConfig,
    SnapshotNotReady,
    SnapshotRateLimited,
    SnapshotService,
)
from app.telemetry.window import WindowedMetrics


def _ok_probe():
    async def p():
        return ProbeResult(HealthState.UP)
    return p


def _down_probe():
    async def p():
        return ProbeResult(HealthState.DOWN, reason="boom", detail="dsn://secret")
    return p


def _service(rate=100.0, burst=100, ttl=0.0, critical_down=False):
    metrics = MetricsRegistry()
    metrics.register_route("/a")
    windowed = WindowedMetrics(window_seconds=60, bucket_seconds=10)
    health = HealthChecker()
    health.register("db", _down_probe() if critical_down else _ok_probe(), critical=True)
    svc = SnapshotService(
        metrics, windowed, health,
        SnapshotConfig(cache_ttl_seconds=ttl, rate_limit_per_second=rate, rate_burst=burst),
    )
    return svc, metrics, windowed


async def test_snapshot_contains_window_and_cumulative():
    svc, metrics, windowed = _service()
    metrics.record("/a", 200, 0.01)
    metrics.record("/a", 500, 0.02)
    windowed.record("/a", 200, 0.01, now=1000.0)
    windowed.record("/a", 500, 0.02, error_kind="internal", now=1000.0)
    payload = await svc.build(now=1000.0)
    route = payload["routes"]["/a"]
    assert route["cumulative"]["total"] == 2
    assert route["window"]["total"] == 2
    assert route["window"]["error_kinds"] == {"internal": 1}
    assert set(route["window"]["quantiles"]) == {"p50", "p95", "p99"}
    assert payload["status"] == "up"
    assert payload["window_seconds"] == 60
    assert payload["snapshot_id"]
    print(f"[snapshot] 判定:窗口与累计对照齐全 {route}")


async def test_snapshot_is_sanitized():
    svc, metrics, windowed = _service()
    metrics.record("/a", 500, 0.02)
    windowed.record("/a", 500, 0.02, error_kind="internal", now=1.0)
    payload = await svc.build(now=1.0)
    text = str(payload)
    # 不得出现内部异常细节、连接信息
    for leaked in ("dsn", "secret", "Traceback", "10.0.0"):
        assert leaked not in text, leaked
    # 依赖标识不得作为字段出现（snapshot_id 是随机 hex，用键匹配避免误判）
    assert "db" not in payload
    assert "checks" not in payload
    assert set(payload["routes"]["/a"]["window"]) == {
        "total", "success", "client_errors", "server_errors",
        "error_kinds", "quantiles",
    }
    print("[snapshot] 判定:对外负载无内部细节/依赖标识")


async def test_snapshot_rejected_when_not_ready():
    svc, _, _ = _service(critical_down=True)
    with pytest.raises(SnapshotNotReady):
        await svc.build(now=1.0)
    print("[snapshot] 判定:readiness=down 时拒绝生成快照")


async def test_rate_limit_enforced():
    svc, _, _ = _service(rate=1.0, burst=3)
    for i in range(3):
        await svc.build(now=100.0 + i * 0.01)  # 耗尽突发额度
    with pytest.raises(SnapshotRateLimited) as exc:
        await svc.build(now=100.03)
    assert exc.value.retry_after > 0
    # 等待补充后恢复
    await svc.build(now=101.5)
    print("[snapshot] 判定:令牌桶限流生效，补充后恢复")


async def test_metrics_aggregation_cached_within_ttl():
    svc, metrics, _ = _service(ttl=10.0)
    metrics.record("/a", 200, 0.01)
    await svc.build(now=100.0)
    metrics.record("/a", 200, 0.01)  # 缓存期内的新请求
    second = await svc.build(now=105.0)
    assert second["routes"]["/a"]["cumulative"]["total"] == 1  # 缓存未刷新
    third = await svc.build(now=111.0)  # 超过 TTL
    assert third["routes"]["/a"]["cumulative"]["total"] == 2
    print("[snapshot] 判定:TTL 内复用聚合结果，过期后刷新")
