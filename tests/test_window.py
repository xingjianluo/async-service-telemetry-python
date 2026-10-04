"""滑动窗口单测：落桶唯一、滚动不丢不重、分位数、与累计对账。"""
from __future__ import annotations

from app.telemetry.metrics import MetricsRegistry
from app.telemetry.window import WindowedMetrics


def test_request_lands_in_exactly_one_bucket():
    w = WindowedMetrics(window_seconds=60, bucket_seconds=10)
    w.record("/a", 200, 0.01, now=100.0)
    # 同一时刻快照：窗口内恰有 1 次
    snap = w.snapshot(now=100.0)
    assert snap["/a"].total == 1
    # 窗口内任意后续时刻看，仍是 1 次（不会重复出现）
    assert w.snapshot(now=105.0)["/a"].total == 1
    print("[window] 判定:一笔请求在窗口内只计一次，不重复")


def test_window_rolls_and_drops_old_buckets():
    w = WindowedMetrics(window_seconds=30, bucket_seconds=10)
    w.record("/a", 200, 0.01, now=0.0)     # 桶 0
    w.record("/a", 200, 0.01, now=15.0)    # 桶 1
    w.record("/a", 500, 0.02, error_kind="internal", now=25.0)  # 桶 2
    snap = w.snapshot(now=29.9)
    assert snap["/a"].total == 3
    assert snap["/a"].server_errors == 1
    # 滚动：now=30 后桶 0（含 1 次成功）滑出窗口
    snap = w.snapshot(now=30.0)
    assert snap["/a"].total == 2
    assert snap["/a"].success == 1
    assert snap["/a"].server_errors == 1
    # 再滚过整个窗口，全部滑出
    snap = w.snapshot(now=61.0)
    assert snap["/a"].total == 0
    print("[window] 判定:窗口滚动后旧桶退出，剩余统计正确")


def test_inflight_request_counted_at_completion_time():
    """并发处理中的请求在完成时刻落桶：开始于旧窗、完成于新窗，只算进新窗。"""
    w = WindowedMetrics(window_seconds=30, bucket_seconds=10)
    # 请求在 t=9（旧窗）开始，t=31（新窗）才完成 -> 只出现在新窗
    w.record("/a", 200, 22.0, now=31.0)
    assert w.snapshot(now=29.0).get("/a") is None or w.snapshot(now=29.0)["/a"].total == 0
    snap = w.snapshot(now=31.0)
    assert snap["/a"].total == 1
    print("[window] 判定:并发在途请求按完成时刻落入唯一窗口")


def test_window_reconciles_with_cumulative():
    """窗口滚动不影响累计值；未滚动时窗口与累计相等。"""
    reg = MetricsRegistry()
    reg.register_route("/a")
    w = WindowedMetrics(window_seconds=30, bucket_seconds=10)
    events = [(0.0, 200), (5.0, 500), (15.0, 404), (25.0, 200), (70.0, 200)]
    for ts, status in events:
        reg.record("/a", status, 0.01)
        w.record("/a", status, 0.01, now=ts)
    cum = reg.snapshot()["/a"]
    win = w.snapshot(now=70.0)["/a"]
    # 累计是全量 5 次；窗口（30s）只含 t=70 的 1 次，且窗口 <= 累计
    assert cum.total == 5
    assert win.total == 1
    assert win.total <= cum.total
    # 未发生滚动的对照组
    w2 = WindowedMetrics(window_seconds=300, bucket_seconds=10)
    reg2 = MetricsRegistry()
    reg2.register_route("/a")
    for ts, status in events:
        reg2.record("/a", status, 0.01)
        w2.record("/a", status, 0.01, now=ts)
    assert w2.snapshot(now=70.0)["/a"].total == reg2.snapshot()["/a"].total == 5
    print("[window] 判定:滚动后累计不变(5)，窗口=1；未滚动时窗口==累计")


def test_error_kinds_classified_in_window():
    w = WindowedMetrics(window_seconds=60, bucket_seconds=10)
    w.record("/a", 504, 0.1, error_kind="upstream_timeout", now=0.0)
    w.record("/a", 503, 0.1, error_kind="upstream_unavailable", now=1.0)
    w.record("/a", 500, 0.1, error_kind="internal", now=2.0)
    w.record("/a", 404, 0.1, error_kind="http_4xx", now=3.0)
    w.record("/a", 200, 0.1, now=4.0)
    kinds = w.snapshot(now=5.0)["/a"].error_kinds
    assert kinds == {
        "upstream_timeout": 1, "upstream_unavailable": 1,
        "internal": 1, "http_4xx": 1,
    }
    print(f"[window] 判定:失败分类 {kinds}")


def test_quantiles_from_histogram():
    w = WindowedMetrics(window_seconds=60, bucket_seconds=10)
    # 100 个样本：50 个落在 <=0.05 桶，45 个落在 <=0.25，5 个落在 <=0.5
    for _ in range(50):
        w.record("/a", 200, 0.05, now=0.0)
    for _ in range(45):
        w.record("/a", 200, 0.25, now=0.0)
    for _ in range(5):
        w.record("/a", 200, 0.5, now=0.0)
    q = w.snapshot(now=1.0)["/a"].quantiles
    assert set(q) == {"p50", "p95", "p99"}
    assert 0.0 < q["p50"] <= 0.05
    assert 0.05 < q["p95"] <= 0.25
    assert 0.25 < q["p99"] <= 0.5
    print(f"[window] 判定:分位数 {q}")
