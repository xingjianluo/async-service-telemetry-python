"""失败原因归类：窗口/累计按原因对账、快照暴露原因分布且脱敏、异常路径采样标记。"""
from __future__ import annotations

import asyncio
import threading

import httpx
import pytest

from app.service import create_app
from app.telemetry.metrics import (
    FAILURE_REASONS,
    MetricsRegistry,
    classify_failure_reason,
)
from app.telemetry.windows import SlidingWindowMetrics


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
async def client():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        c.app = app  # type: ignore[attr-defined]
        yield c


def _failures(m) -> int:
    return m.client_errors + m.server_errors


# ---------- 分类规则 ----------

def test_classify_failure_reason_deterministic():
    # 成功不归类
    assert classify_failure_reason(200) is None
    assert classify_failure_reason(302) is None
    # 无 kind 时按状态码推导
    assert classify_failure_reason(404) == "client_error"
    assert classify_failure_reason(422) == "client_error"
    assert classify_failure_reason(500) == "internal"
    assert classify_failure_reason(502) == "internal"
    assert classify_failure_reason(503) == "upstream_unavailable"
    assert classify_failure_reason(504) == "upstream_timeout"
    # 有归一化 kind 时以 kind 为准（含 4xx 的 validation）
    assert classify_failure_reason(400, "validation") == "client_error"
    assert classify_failure_reason(503, "dependency_degraded") == "dependency_degraded"
    assert classify_failure_reason(500, "internal") == "internal"
    # 未知 kind 按状态码兜底，绝不产出自由文本原因
    assert classify_failure_reason(500, "weird") == "internal"
    assert classify_failure_reason(400, "weird") == "client_error"
    print(f"[reasons] 判定:分类规则确定，取值域固定 {list(FAILURE_REASONS)}")


# ---------- 窗口内原因归类 ----------

def test_window_reasons_sum_to_failures_and_roll():
    """窗口内各原因之和恒等于失败总数；窗口前滚后原因随 bucket 一起过期。"""
    clock = FakeClock()
    win = SlidingWindowMetrics(window_seconds=30.0, bucket_seconds=10.0, clock=clock)
    win.record("/a", 504, 0.1)   # upstream_timeout
    win.record("/a", 503, 0.1)   # upstream_unavailable
    win.record("/a", 500, 0.1)   # internal
    win.record("/a", 404, 0.1)   # client_error
    win.record("/a", 200, 0.1)

    snap = win.snapshot("/a")
    assert snap.failure_reasons == {
        "upstream_timeout": 1,
        "upstream_unavailable": 1,
        "internal": 1,
        "client_error": 1,
    }
    assert sum(snap.failure_reasons.values()) == (
        snap.client_errors + snap.server_errors
    )
    # 窗口前滚，旧 bucket 过期后原因计数同步消失，不漏不残留
    clock.t = 1031.0
    snap = win.snapshot("/a")
    assert snap.total == 0
    assert snap.failure_reasons == {}
    print("[reasons] 判定:窗口原因和==失败总数；前滚后原因随 bucket 过期清零")


def test_window_reasons_reconcile_with_cumulative():
    """窗口覆盖全部历史时，原因分布必须与累计逐项一致。"""
    clock = FakeClock()
    reg = MetricsRegistry(max_routes=4)
    reg.register_route("/a")
    win = SlidingWindowMetrics(window_seconds=3600.0, bucket_seconds=60.0, clock=clock)

    plan = (
        [(200, None)] * 7
        + [(404, None)] * 2
        + [(504, "upstream_timeout")] * 3
        + [(503, "upstream_unavailable")] * 2
        + [(503, "dependency_degraded")] * 1
        + [(500, "internal")] * 4
    )
    for i, (status, kind) in enumerate(plan):
        clock.t = 1000.0 + i * 0.5
        reason = classify_failure_reason(status, kind)
        reg.record("/a", status, 0.01, reason=reason)
        win.record("/a", status, 0.01, reason=reason)

    cum = reg.snapshot()["/a"]
    snap = win.snapshot("/a")
    assert snap.failure_reasons == cum.failure_reasons
    assert sum(snap.failure_reasons.values()) == _failures(snap) == _failures(cum)
    print(f"[reasons] 对账 判定:窗口原因 {snap.failure_reasons} == 累计，逐项一致")


def test_concurrent_mixed_reasons_no_dup_no_loss():
    """并发混合超时/不可用/异常/正常：每类原因不重不丢，总和精确。"""
    clock = FakeClock()
    reg = MetricsRegistry(max_routes=4)
    reg.register_route("/a")
    win = SlidingWindowMetrics(window_seconds=300.0, bucket_seconds=10.0, clock=clock)
    n_threads, n_each = 8, 250
    statuses = [200, 504, 503, 500, 404]

    def worker(base: int) -> None:
        for i in range(n_each):
            status = statuses[(base + i) % len(statuses)]
            reason = classify_failure_reason(status)
            reg.record("/a", status, 0.01, reason=reason)
            win.record("/a", status, 0.01, reason=reason)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    cum = reg.snapshot()["/a"]
    snap = win.snapshot("/a")
    expected_each = n_threads * n_each // len(statuses)
    assert cum.total == n_threads * n_each
    assert cum.failure_reasons == {
        "upstream_timeout": expected_each,
        "upstream_unavailable": expected_each,
        "internal": expected_each,
        "client_error": expected_each,
    }
    assert snap.failure_reasons == cum.failure_reasons
    assert sum(snap.failure_reasons.values()) == _failures(snap)
    print(f"[reasons] 并发 判定:{n_threads}x{n_each} 混合，"
          f"窗口==累计 {snap.failure_reasons}，不重不丢")


# ---------- 端到端：并发混合 + 快照 + 采样标记 ----------

async def test_e2e_concurrent_mixed_reasons_reconcile_and_sampled_headers(client):
    """并发混合（正常/超时/下游不可用/降级/内部异常 x 采样开关交错）。

    断言：
    1. 每个响应（含全部异常路径）的 x-trace-sampled 与入站决定一致；
    2. 窗口与累计按原因逐项对账一致，各原因之和 == 失败总数。
    """
    plan = [("/", 30), ("/upstream/timeout", 15), ("/upstream/unavailable", 10),
            ("/upstream/degraded", 5), ("/boom", 20)]

    async def one(i: int, path: str) -> None:
        sampled = "1" if i % 2 else "0"
        r = await client.get(path, headers={
            "x-request-id": f"fr-{i:04d}-{'d' * 16}",
            "x-trace-sampled": sampled,
        })
        # 成功与异常路径的采样标记都必须与入站决定一致
        assert r.headers["x-trace-sampled"] == sampled, (
            f"{path} 采样标记缺失或不一致: {dict(r.headers)}"
        )

    tasks = []
    idx = 0
    for path, n in plan:
        for _ in range(n):
            tasks.append(one(idx, path))
            idx += 1
    await asyncio.gather(*tasks)

    expected_reasons = {
        "/": {},
        "/upstream/timeout": {"upstream_timeout": 15},
        "/upstream/unavailable": {"upstream_unavailable": 10},
        "/upstream/degraded": {"dependency_degraded": 5},
        "/boom": {"internal": 20},
    }
    metrics = client.app.state.metrics.snapshot()
    windows = client.app.state.windows
    for tpl, n in plan:
        cum = metrics[tpl]
        win = windows.snapshot(tpl)
        assert cum.total == n
        assert cum.failure_reasons == expected_reasons[tpl]
        assert win.failure_reasons == cum.failure_reasons, (
            f"{tpl} 窗口 {win.failure_reasons} != 累计 {cum.failure_reasons}"
        )
        assert sum(win.failure_reasons.values()) == _failures(win)
        print(f"[reasons] {tpl} 判定:窗口==累计 原因={cum.failure_reasons}，"
              f"异常路径采样标记与入站一致")


async def test_snapshot_exposes_reasons_on_both_sides_and_sanitized(client):
    """快照的窗口与累计两侧都带失败原因分布，且仍不泄露内部细节。"""
    await client.get("/upstream/timeout")
    await client.get("/upstream/unavailable")
    await client.get("/boom")
    await client.get("/no-such-page")  # 404 -> client_error

    r = await client.get("/telemetry/snapshot")
    assert r.status_code == 200
    routes = r.json()["routes"]

    win = routes["/upstream/timeout"]["window"]
    cum = routes["/upstream/timeout"]["cumulative"]
    assert win["failure_reasons"] == {"upstream_timeout": 1}
    assert cum["failure_reasons"] == {"upstream_timeout": 1}
    assert routes["/boom"]["cumulative"]["failure_reasons"] == {"internal": 1}
    unmatched = routes["__unmatched__"]["cumulative"]
    assert unmatched["failure_reasons"] == {"client_error": 1}
    # 两侧原因之和各自等于两侧失败总数
    for side in (win, cum):
        assert sum(side["failure_reasons"].values()) == (
            side["client_errors"] + side["server_errors"]
        )
    # 脱敏：原因只是固定枚举，负载中无内部细节与依赖标识
    text = r.text
    leaked = ("secret internal detail", "10.0.0.1", "circuit half-open",
              "primary", "cache", "RuntimeError", "TimeoutError")
    assert not any(s in text for s in leaked), f"泄露: {text}"
    assert set(win["failure_reasons"]) <= set(FAILURE_REASONS)
    print("[reasons] 快照 判定:窗口+累计两侧原因分布齐全，"
          "取值为固定枚举，无内部细节/依赖标识")


async def test_error_paths_sampled_header_fallback_deterministic(client):
    """异常路径上，缺失/非法采样头按既定规则降级，与正常路径同一行为。"""
    for path in ("/boom", "/upstream/timeout", "/upstream/unavailable"):
        # 缺失 -> 默认 True
        r = await client.get(path)
        assert r.headers["x-trace-sampled"] == "1"
        # 非法 -> 默认 True
        r = await client.get(path, headers={"x-trace-sampled": "maybe"})
        assert r.headers["x-trace-sampled"] == "1"
        # 合法 -> 遵从
        r = await client.get(path, headers={"x-trace-sampled": "0"})
        assert r.headers["x-trace-sampled"] == "0"
    print("[reasons] 采样 判定:异常路径缺失/非法头确定性降级，"
          "合法头遵从，与正常路径一致")
