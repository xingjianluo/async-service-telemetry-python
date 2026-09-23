import asyncio
import logging

import pytest

from app.telemetry.metrics import OVERFLOW_ROUTE, UNKNOWN_ROUTE, MetricsRegistry

log = logging.getLogger("test.metrics")


def test_record_success_and_error_once_each():
    reg = MetricsRegistry()
    reg.record("/items/{item_id}", 12.0, is_error=False)
    reg.record("/items/{item_id}", 30.0, is_error=True)
    stats = reg.snapshot()["/items/{item_id}"]
    assert stats.count == 2
    assert stats.error_count == 1
    assert stats.error_rate == 0.5
    assert stats.latency_sum_ms == 42.0
    assert stats.latency_max_ms == 30.0
    log.info(
        "route=/items/{item_id} count=%d errors=%d 判定依据=每次请求只 record 一次",
        stats.count,
        stats.error_count,
    )


def test_histogram_buckets_cumulative():
    reg = MetricsRegistry(buckets_ms=(10, 100))
    reg.record("/a", 5.0, is_error=False)
    reg.record("/a", 50.0, is_error=False)
    reg.record("/a", 500.0, is_error=False)
    buckets = reg.snapshot()["/a"].buckets
    assert buckets[10] == 1
    assert buckets[100] == 2  # 累计式：<=100 含 <=10


def test_unknown_route_merged():
    reg = MetricsRegistry()
    assert reg.record(None, 1.0, is_error=True) == UNKNOWN_ROUTE  # type: ignore[arg-type]
    assert UNKNOWN_ROUTE in reg.snapshot()


def test_overflow_route_merged_deterministically():
    reg = MetricsRegistry(max_routes=2)
    reg.record("/a", 1.0, is_error=False)
    reg.record("/b", 1.0, is_error=False)
    # 超限后新路由归并到 __other__，且可预期
    assert reg.record("/c", 1.0, is_error=False) == OVERFLOW_ROUTE
    assert reg.record("/d", 1.0, is_error=False) == OVERFLOW_ROUTE
    # 已有路由不受影响
    assert reg.record("/a", 1.0, is_error=False) == "/a"
    snap = reg.snapshot()
    assert set(snap) == {"/a", "/b", OVERFLOW_ROUTE}
    assert snap[OVERFLOW_ROUTE].count == 2
    log.info(
        "overflow 归并判定依据=max_routes=%d, routes=%s", 2, sorted(snap)
    )


@pytest.mark.asyncio
async def test_concurrent_records_no_double_count():
    reg = MetricsRegistry()
    n = 200

    async def hit(i: int) -> None:
        reg.record("/work", 1.0, is_error=(i % 3 == 0))

    async with asyncio.TaskGroup() as tg:
        for i in range(n):
            tg.create_task(hit(i))

    stats = reg.snapshot()["/work"]
    assert stats.count == n  # 不重复计数、不丢失
    assert stats.error_count == len([i for i in range(n) if i % 3 == 0])
    log.info("并发 %d 次请求后 count=%d 判定依据=锁保护原子自增", n, stats.count)
