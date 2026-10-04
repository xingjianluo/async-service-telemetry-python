"""端到端：并发+异常+超时+采样混合场景下的窗口聚合、采样传播与对外快照。"""
from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from app.service import create_app


@pytest.fixture
async def client():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        c.app = app  # type: ignore[attr-defined]
        yield c


async def test_window_matches_cumulative_under_mixed_concurrency(client):
    """并发混合（成功/异常/超时/采样开/采样关）后：窗口与累计对账一致，不重不漏。"""
    plan = [("/", 30), ("/items/{item_id}", 20), ("/boom", 15),
            ("/upstream/timeout", 10), ("/upstream/unavailable", 10)]
    paths = {"/": "/", "/items/{item_id}": "/items/x", "/boom": "/boom",
             "/upstream/timeout": "/upstream/timeout",
             "/upstream/unavailable": "/upstream/unavailable"}

    async def hit(tpl: str, i: int):
        # 采样显式开/关/缺头三种混合
        headers = {}
        if i % 3 == 0:
            headers["x-trace-sampled"] = "1"
        elif i % 3 == 1:
            headers["x-trace-sampled"] = "0"
        return await client.get(paths[tpl], headers=headers)

    await asyncio.gather(*(
        hit(tpl, i) for tpl, n in plan for i in range(n)
    ))

    cumulative = client.app.state.metrics.snapshot()
    windowed = client.app.state.windowed.snapshot()
    total_sent = sum(n for _, n in plan)
    cum_total = sum(m.total for m in cumulative.values())
    win_total = sum(s.total for s in windowed.values())
    assert cum_total == total_sent, f"累计 {cum_total} != 实发 {total_sent}"
    # 测试窗口（300s）内未发生滚动：窗口合计必须等于累计合计
    assert win_total == cum_total, f"窗口 {win_total} != 累计 {cum_total}"
    for tpl, n in plan:
        assert windowed[tpl].total == n == cumulative[tpl].total
        w = windowed[tpl]
        assert w.success + w.client_errors + w.server_errors == w.total
        print(f"[e2e] {tpl} 判定:窗口==累计=={n}，失败分类={w.error_kinds}")
    # 失败分类与路由语义吻合
    assert windowed["/boom"].error_kinds == {"internal": 15}
    assert windowed["/upstream/timeout"].error_kinds == {"upstream_timeout": 10}
    assert windowed["/upstream/unavailable"].error_kinds == {"upstream_unavailable": 10}
    # 分位数存在
    assert set(windowed["/"].quantiles) == {"p50", "p95", "p99"}


async def test_sampling_decision_consistent_across_response_and_background(client):
    """采样决定：响应头回写、后台任务继承、下游传播头一致，链路不拆分。"""
    # 显式关采样
    r = await client.post("/work", headers={
        "x-request-id": "sampling-off-0001", "x-trace-sampled": "0",
    })
    body = r.json()
    assert r.headers["x-trace-sampled"] == "0"
    assert body["sampled"] is False
    assert body["background_sampled"] is False          # 后台任务同一决定
    assert body["downstream_headers"]["x-trace-sampled"] == "0"  # 下游同一决定
    assert body["downstream_headers"]["x-request-id"] == "sampling-off-0001"

    # 显式开采样
    r = await client.post("/work", headers={
        "x-request-id": "sampling-on-00001", "x-trace-sampled": "1",
    })
    body = r.json()
    assert r.headers["x-trace-sampled"] == "1"
    assert body["sampled"] is True and body["background_sampled"] is True
    assert body["downstream_headers"]["x-trace-sampled"] == "1"
    print("[e2e] 采样 判定:显式开/关在响应头、后台任务、下游传播头三处一致")


async def test_sampling_fallback_is_deterministic(client):
    """下游/上游不识别（缺头或非法值）时：降级确定，同一 trace 多次请求结果一致。"""
    tid = "fallback-trace-0001"
    decisions = set()
    for headers in ({}, {"x-trace-sampled": "garbage!!"}):
        for _ in range(3):
            r = await client.get("/", headers={**headers, "x-request-id": tid})
            decisions.add(r.headers["x-trace-sampled"])
    # 缺头与非法头都走同一确定性降级 -> 全部一致，不时有时无
    assert len(decisions) == 1, decisions
    print(f"[e2e] 采样降级 判定:缺头/非法头对同一 trace 决定恒定={decisions.pop()}")


async def test_snapshot_endpoint_sanitized_and_gated(client):
    """对外快照：内容脱敏、窗口+累计对照、未就绪时 503 拒绝。"""
    for path in ("/", "/boom", "/upstream/unavailable"):
        await client.get(path)
    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "up"
    assert body["snapshot_id"]
    routes = body["routes"]
    assert routes["/"]["window"]["total"] == routes["/"]["cumulative"]["total"] == 1
    assert routes["/boom"]["window"]["error_kinds"] == {"internal": 1}
    # 脱敏：内部细节与依赖标识不外泄
    text = str(body)
    for leaked in ("secret internal detail", "10.0.0.1", "circuit half-open",
                   "primary", "cache", "dsn"):
        assert leaked not in text, leaked
    print(f"[e2e] 快照 判定:窗口+累计对照齐全且脱敏 snapshot_id={body['snapshot_id']}")

    # 未就绪 -> 明确 503，不返回看似正常的数据
    await client.post("/debug/deps/primary?state=down")
    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 503
    assert r.json()["error"]["kind"] == "not_ready"
    await client.post("/debug/deps/primary?state=up")
    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 200
    print("[e2e] 快照门控 判定:未就绪 503，恢复后 200")


async def test_snapshot_endpoint_rate_limited(client):
    """高频拉取触发限流：突发额度耗尽后返回 429 + Retry-After。"""
    statuses = []
    for _ in range(30):
        r = await client.get("/telemetry/snapshot")
        statuses.append(r.status_code)
        if len(statuses) > 11:
            break
    assert 429 in statuses, statuses
    r = await client.get("/telemetry/snapshot")
    if r.status_code == 429:
        assert r.headers["Retry-After"]
        assert r.json()["error"]["kind"] == "rate_limited"
    print(f"[e2e] 限流 判定:高频拉取出现 429，状态序列={statuses}")


async def test_request_logs_carry_sampling_verdict(client, caplog):
    with caplog.at_level(logging.INFO, logger="telemetry.request"):
        await client.get("/", headers={
            "x-request-id": "log-sampled-0001", "x-trace-sampled": "0",
        })
        await client.get("/boom", headers={"x-request-id": "log-sampled-0002"})
    text = caplog.text
    assert "log-sampled-0001" in text and "sampled=False" in text
    assert "sampling_basis=inbound_header" in text
    assert "log-sampled-0002" in text and "kind=internal" in text
    print("[e2e] 日志 判定:含 trace_id、采样决定与判定依据")
