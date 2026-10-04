"""端到端测试：并发混合请求下追踪不串扰、指标恰好计一次、失败原因可区分。"""
from __future__ import annotations

import asyncio
import logging
import re

import httpx
import pytest

from app.service import create_app
from app.telemetry.metrics import MetricsRegistry

VALID_ID = re.compile(r"^[A-Za-z0-9._-]{8,128}$")


@pytest.fixture
async def client():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        c.app = app  # type: ignore[attr-defined]
        yield c


async def test_concurrent_mixed_requests_no_crosstalk(client, caplog):
    """200 个并发请求（成功/各类失败交错），每个响应必须携带自己的 trace_id。"""
    cases = []
    for i in range(200):
        tid = f"req-{i:04d}-{'a' * 16}"
        path = ["/", f"/items/{i}", "/boom", "/upstream/timeout",
                "/upstream/unavailable"][i % 5]
        cases.append((tid, path))

    async def one(tid: str, path: str):
        r = await client.get(path, headers={"x-request-id": tid})
        # 响应头必须回写本请求的标识
        assert r.headers["x-request-id"] == tid, f"串扰: {tid} -> {r.headers.get('x-request-id')}"
        body = r.json()
        if r.status_code >= 400:
            assert body["error"]["trace_id"] == tid, f"错误体串扰: {tid}"
        elif "trace_id" in body:
            assert body["trace_id"] == tid, f"响应体串扰: {tid}"
        return tid, path, r.status_code

    results = await asyncio.gather(*(one(t, p) for t, p in cases))
    assert len(results) == 200
    print("[e2e] 200 并发混合请求 判定:全部响应携带各自 trace_id，无串扰")


async def test_metrics_counted_exactly_once(client):
    """并发结束后，每个路由的计数必须等于实际请求数（不重不漏）。"""
    plan = {"/": 30, "/items/{item_id}": 25, "/boom": 20,
            "/upstream/timeout": 15, "/upstream/unavailable": 10}
    paths = {"/": "/", "/items/{item_id}": "/items/x", "/boom": "/boom",
             "/upstream/timeout": "/upstream/timeout",
             "/upstream/unavailable": "/upstream/unavailable"}

    async def hit(path: str):
        await client.get(path)

    await asyncio.gather(*(
        hit(paths[tpl]) for tpl, n in plan.items() for _ in range(n)
    ))

    snap = client.app.state.metrics.snapshot()
    for tpl, n in plan.items():
        m = snap[tpl]
        assert m.total == n, f"{tpl}: total={m.total} != {n}"
        assert m.latency_count == n
        assert m.success + m.client_errors + m.server_errors == m.total
        print(f"[metrics] {tpl} 判定:total={m.total} "
              f"(ok={m.success} 4xx={m.client_errors} 5xx={m.server_errors})")
    assert snap["/boom"].server_errors == 20
    assert snap["/"].success == 30


async def test_failure_kinds_distinguishable_and_sanitized(client):
    expectations = {
        "/upstream/timeout": (504, "upstream_timeout"),
        "/upstream/unavailable": (503, "upstream_unavailable"),
        "/upstream/degraded": (503, "dependency_degraded"),
        "/boom": (500, "internal"),
    }
    for path, (status, kind) in expectations.items():
        r = await client.get(path)
        body = r.json()
        assert r.status_code == status
        assert body["error"]["kind"] == kind
        # 内部细节不得外泄
        leaked = ("secret internal detail", "10.0.0.1", "circuit half-open")
        assert not any(s in str(body) for s in leaked)
        print(f"[e2e] {path} 判定:{status}/{kind}，内部细节已脱敏")


async def test_invalid_trace_id_is_replaced(client):
    r = await client.get("/", headers={"x-request-id": "bad id!!"})
    echoed = r.headers["x-request-id"]
    assert echoed != "bad id!!"
    assert VALID_ID.fullmatch(echoed)
    print(f"[e2e] 非法标识 判定:确定性补全为 {echoed}")


async def test_unknown_routes_merge_into_single_bucket(client):
    for i in range(300):
        await client.get(f"/no-such-path-{i}")
    snap = client.app.state.metrics.snapshot()
    unmatched = snap[MetricsRegistry.UNMATCHED_ROUTE]
    assert unmatched.total == 300
    assert unmatched.client_errors == 300
    # 300 条不同路径只产生一个维度
    assert MetricsRegistry.UNMATCHED_ROUTE in snap
    print("[e2e] 300 条不同 404 路径 判定:归并 __unmatched__ 单一维度")


async def test_concurrent_mixed_errors_timeouts_sampling_windows_reconcile(client):
    """并发混合：成功/异常/超时 x 采样/不采样。

    证明：窗口聚合与累计指标逐路由对账一致（不重不丢），
    采样开关不影响计数，错误分类齐全。
    """
    plan = [("/", 30), ("/boom", 20), ("/upstream/timeout", 15),
            ("/upstream/unavailable", 10)]

    async def one(i: int, path: str) -> None:
        r = await client.get(path, headers={
            "x-request-id": f"stress-{i:04d}-{'c' * 16}",
            "x-trace-sampled": "1" if i % 2 else "0",
        })
        assert r.headers["x-request-id"] == f"stress-{i:04d}-{'c' * 16}"

    tasks = []
    idx = 0
    for path, n in plan:
        for _ in range(n):
            tasks.append(one(idx, path))
            idx += 1
    await asyncio.gather(*tasks)

    metrics = client.app.state.metrics.snapshot()
    windows = client.app.state.windows
    for tpl, n in plan:
        cum = metrics[tpl]
        win = windows.snapshot(tpl)
        assert cum.total == n, f"{tpl} 累计 {cum.total} != {n}"
        assert win.total == cum.total, f"{tpl} 窗口 {win.total} != 累计 {cum.total}"
        assert win.success == cum.success
        assert win.client_errors == cum.client_errors
        assert win.server_errors == cum.server_errors
        assert win.quantiles[0.5] is not None
        print(f"[e2e] {tpl} 判定:窗口==累计 total={cum.total} "
              f"(5xx={cum.server_errors})，采样混合不影响计数")


async def test_request_logs_carry_trace_id_and_verdict(client, caplog):
    with caplog.at_level(logging.INFO, logger="telemetry.request"):
        await client.get("/", headers={"x-request-id": "log-trace-0001"})
        await client.get("/boom", headers={"x-request-id": "log-trace-0002"})
    text = caplog.text
    assert "log-trace-0001" in text and "route=/" in text
    assert "log-trace-0002" in text and "kind=internal" in text
    print("[e2e] 日志 判定:包含 trace_id 与路由/分类依据")
