"""指标注册表单元测试：唯一计数、互斥分类、基数归并、超限拒绝。"""
from __future__ import annotations

import pytest

from app.telemetry.metrics import LATENCY_BOUNDS, MetricsRegistry


def test_disjoint_counting_no_double_count(caplog):
    reg = MetricsRegistry(max_routes=4)
    reg.register_route("/a")

    reg.record("/a", 200, 0.01)
    reg.record("/a", 204, 0.1)
    reg.record("/a", 404, 0.2)
    reg.record("/a", 422, 0.3)
    reg.record("/a", 500, 0.4)
    reg.record("/a", 503, 0.5)

    m = reg.snapshot()["/a"]
    assert m.total == 6
    assert (m.success, m.client_errors, m.server_errors) == (2, 2, 2)
    assert m.success + m.client_errors + m.server_errors == m.total
    assert m.latency_count == m.total
    assert sum(m.latency_buckets.values()) == m.total
    print(f"[metrics] /a 判定:total={m.total} 且 2+2+2 互斥不重复")


def test_latency_buckets_are_bounded():
    reg = MetricsRegistry()
    reg.register_route("/a")
    for d in (0.0, 0.004, 0.1, 1.0, 99.0):
        reg.record("/a", 200, d)
    m = reg.snapshot()["/a"]
    # 维度固定：边界桶 + +Inf
    assert all(b in (*LATENCY_BOUNDS, float("inf")) for b in m.latency_buckets)
    print(f"[metrics] 时延桶判定:固定边界 {sorted(m.latency_buckets, key=lambda x: x)}")


def test_unregistered_and_unmatched_are_merged(caplog):
    reg = MetricsRegistry(max_routes=4)
    reg.register_route("/known")

    # 大量不同的“已识别但未注册”路径不得扩张维度
    for i in range(500):
        reg.record(f"/wild-{i}", 200, 0.01)
    for i in range(500):
        reg.record(None, 404, 0.01)  # 未知路由 404

    snap = reg.snapshot()
    assert set(snap) == {
        "/known", MetricsRegistry.OVERFLOW_ROUTE, MetricsRegistry.UNMATCHED_ROUTE,
    }
    assert snap[MetricsRegistry.OVERFLOW_ROUTE].total == 500
    assert snap[MetricsRegistry.UNMATCHED_ROUTE].client_errors == 500
    print("[metrics] 判定:500 条高频未注册路径归并 __other__，500 条 404 归并 __unmatched__")


def test_cardinality_limit_is_enforced():
    reg = MetricsRegistry(max_routes=2)
    reg.register_route("/a")
    reg.register_route("/b")
    with pytest.raises(RuntimeError):
        reg.register_route("/c")
    print("[metrics] 判定:超出 max_routes 注册被拒绝（确定性失败，不静默扩张）")
