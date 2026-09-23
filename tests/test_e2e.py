import asyncio
import logging
import re

import httpx
import pytest
import pytest_asyncio

from main import create_app

log = logging.getLogger("test.e2e")

TRACEPARENT_RE = re.compile(r"^00-[\da-f]{32}-[\da-f]{16}-0[01]$")


@pytest.fixture
def app():
    return create_app()


@pytest_asyncio.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_response_carries_request_id_and_traceparent(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    rid = resp.headers["x-request-id"]
    tp = resp.headers["traceparent"]
    assert TRACEPARENT_RE.match(tp)
    assert rid in tp.replace("-", "")
    log.info("request_id=%s traceparent=%s 判定依据=响应头回写", rid, tp)


@pytest.mark.asyncio
async def test_inbound_traceparent_reused(client):
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    resp = await client.get("/", headers={"traceparent": tp})
    out = resp.headers["traceparent"]
    assert out.split("-")[1] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert out != tp  # 本服务派生了自己的 span


@pytest.mark.asyncio
async def test_invalid_traceparent_not_passed_through(client):
    resp = await client.get("/", headers={"traceparent": "garbage-value"})
    out = resp.headers["traceparent"]
    assert TRACEPARENT_RE.match(out)
    assert "garbage" not in out
    log.info("非法标识已补全 traceparent=%s", out)


@pytest.mark.asyncio
async def test_context_propagates_to_downstream_and_background(client):
    resp = await client.get("/work")
    body = resp.json()
    trace_id = resp.headers["traceparent"].split("-")[1]
    assert body["downstream"]["downstream_traceparent"].split("-")[1] == trace_id
    assert body["background"]["request_id"] == resp.headers["x-request-id"]
    log.info(
        "request_id=%s 下游与后台任务 trace_id 一致=%s",
        resp.headers["x-request-id"],
        trace_id,
    )


@pytest.mark.asyncio
async def test_concurrent_requests_no_cross_talk(client):
    n = 30
    trace_ids = [f"{i + 1:032x}" for i in range(n)]

    async def hit(i: int) -> tuple[str, str]:
        tp = f"00-{trace_ids[i]}-{i + 1:016x}-01"
        resp = await client.get("/work", headers={"traceparent": tp})
        return resp.headers["x-request-id"], resp.json()["background"]["traceparent"]

    results = await asyncio.gather(*(hit(i) for i in range(n)))
    for i, (rid, bg_tp) in enumerate(results):
        assert bg_tp.split("-")[1] == trace_ids[i], f"请求 {i} 上下文串扰"
        assert rid == trace_ids[i][:16]
    log.info("并发 %d 请求无串扰 判定依据=每个请求 background trace_id 与入站一致", n)


@pytest.mark.asyncio
async def test_metrics_no_double_count_and_route_templates(client, app):
    for i in range(10):
        await client.get(f"/items/{i}")  # 路径参数不应撑爆维度
    await client.get("/no-such-route")
    resp = await client.get("/metrics")
    data = resp.json()
    assert data["/items/{item_id}"]["count"] == 10
    assert data["__unknown__"]["count"] == 1
    assert "/items/1" not in data
    log.info("指标维度=%s 判定依据=路由模板归一化", sorted(data))


@pytest.mark.asyncio
async def test_failure_reasons_distinguishable(client, app):
    cases = {
        "timeout": ("timeout", 504),
        "upstream": ("upstream_5xx", 502),
        "unavailable": ("dependency_unavailable", 503),
        "boom": ("internal", 500),
    }
    for kind, (reason, status) in cases.items():
        resp = await client.get(f"/fail/{kind}")
        assert resp.status_code == status
        body = resp.json()
        assert body["error"] == reason
        assert "request_id" in body
        # 不泄露内部细节
        assert "abc123" not in resp.text and "10.0.0.1" not in resp.text
        log.info(
            "kind=%s request_id=%s reason=%s 判定依据=归一化原因码",
            kind,
            body["request_id"],
            reason,
        )
    # 失败路径也被指标覆盖，且只计一次
    data = (await client.get("/metrics")).json()
    stats = data["/fail/{kind}"]
    assert stats["count"] == len(cases)
    assert stats["error_count"] == len(cases)


@pytest.mark.asyncio
async def test_health_endpoints(client):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"
    assert (await client.get("/readyz")).status_code == 200
