"""滑动窗口聚合单测：唯一归属、滚动不丢不重、分位数、与累计对账、并发。"""
from __future__ import annotations

import threading

from app.telemetry.metrics import MetricsRegistry
from app.telemetry.windows import SlidingWindowMetrics


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def test_window_aggregation_and_quantiles():
    clock = FakeClock()
    win = SlidingWindowMetrics(window_seconds=60.0, bucket_seconds=10.0, clock=clock)
    # 100 个成功请求，延迟 0.01..1.00 秒
    for i in range(1, 101):
        win.record("/a", 200, i / 100.0)
    win.record("/a", 404, 0.5)
    win.record("/a", 503, 0.5)

    snap = win.snapshot("/a")
    assert snap.total == 102
    assert (snap.success, snap.client_errors, snap.server_errors) == (100, 1, 1)
    assert snap.success + snap.client_errors + snap.server_errors == snap.total
    q = snap.quantiles
    # 102 个样本（含两个 0.5），线性插值确定性结果
    assert q[0.5] is not None and abs(q[0.5] - 0.51) < 0.02
    assert q[0.95] is not None and abs(q[0.95] - 0.95) < 0.01
    assert q[0.99] is not None and abs(q[0.99] - 0.99) < 0.01
    assert q[0.5] <= q[0.95] <= q[0.99]
    print(f"[windows] 判定:total={snap.total} p50={q[0.5]:.4f} "
          f"p95={q[0.95]:.4f} p99={q[0.99]:.4f}")


def test_window_rolls_and_expires_old_buckets():
    clock = FakeClock()
    win = SlidingWindowMetrics(window_seconds=30.0, bucket_seconds=10.0, clock=clock)
    win.record("/a", 200, 0.1)  # t=1000 -> bucket 100
    assert win.snapshot("/a").total == 1

    clock.t = 1025.0  # bucket 102，窗口覆盖 100..102
    assert win.snapshot("/a").total == 1

    clock.t = 1031.0  # bucket 103，窗口覆盖 101..103，bucket 100 已过期
    snap = win.snapshot("/a")
    assert snap.total == 0
    assert all(v is None for v in snap.quantiles.values())
    print("[windows] 判定:窗口前滚后过期 bucket 被裁剪，旧请求不再计入")


def test_request_lands_in_exactly_one_bucket():
    """在途请求按完成时间戳落桶：边界两侧各记一次，总和恒为 1，不重不丢。"""
    clock = FakeClock()
    win = SlidingWindowMetrics(window_seconds=30.0, bucket_seconds=10.0, clock=clock)
    # 请求在 t=1009.95 完成（bucket 100 末尾）
    win.record("/a", 200, 0.1, ts=1009.95)
    for t in (1009.96, 1010.0, 1010.05, 1019.9, 1029.9, 1039.9):
        clock.t = t
        snap = win.snapshot("/a")
        assert snap.total in (0, 1)
        if t <= 1039.9 - 10.0:  # 仍在窗口内
            assert snap.total == 1, f"t={t} 丢计"
    clock.t = 1040.1  # bucket 104，窗口 102..104，bucket 100 过期
    assert win.snapshot("/a").total == 0
    print("[windows] 判定:跨 bucket 边界滚动，请求始终恰好归属一个窗口，不重不丢")


def test_window_reconciles_with_cumulative():
    """窗口覆盖全部历史时，窗口聚合必须与累计指标一致。"""
    clock = FakeClock()
    reg = MetricsRegistry(max_routes=4)
    reg.register_route("/a")
    win = SlidingWindowMetrics(window_seconds=3600.0, bucket_seconds=60.0, clock=clock)

    plan = [(200, 0.01), (200, 0.2), (404, 0.3), (500, 0.4), (503, 0.5)] * 20
    for i, (status, dur) in enumerate(plan):
        clock.t = 1000.0 + i * 0.5  # 分散到多个 bucket
        reg.record("/a", status, dur)
        win.record("/a", status, dur)

    cum = reg.snapshot()["/a"]
    snap = win.snapshot("/a")
    assert snap.total == cum.total
    assert snap.success == cum.success
    assert snap.client_errors == cum.client_errors
    assert snap.server_errors == cum.server_errors
    print(f"[windows] 对账 判定:窗口 total={snap.total} == 累计 total={cum.total}，"
          f"分类逐项一致")


def test_concurrent_records_no_loss_no_dup():
    clock = FakeClock()
    win = SlidingWindowMetrics(window_seconds=300.0, bucket_seconds=10.0, clock=clock)
    n_threads, n_each = 8, 500

    def worker(base: int) -> None:
        for i in range(n_each):
            status = 200 if (base + i) % 3 else 500
            win.record("/a", status, 0.01)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    snap = win.snapshot("/a")
    assert snap.total == n_threads * n_each
    assert snap.success + snap.server_errors == snap.total
    print(f"[windows] 并发 判定:{n_threads} 线程 x {n_each} 笔，"
          f"total={snap.total} 不重不丢")


def test_snapshot_all_and_empty_route():
    clock = FakeClock()
    win = SlidingWindowMetrics(window_seconds=60.0, bucket_seconds=10.0, clock=clock)
    win.record("/a", 200, 0.1)
    win.record("/b", 500, 0.2)
    all_snaps = win.snapshot_all()
    assert set(all_snaps) == {"/a", "/b"}
    empty = win.snapshot("/never-seen")
    assert empty.total == 0
    assert empty.window_end > empty.window_start
    print("[windows] 判定:snapshot_all 覆盖全部路由，未知路由返回空窗口")
