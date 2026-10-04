"""对外遥测快照：只读入口，供编排/告警系统轮询。

语义：
- 内容：窗口聚合（WindowedMetrics）+ 累计对照（MetricsRegistry），
  两者同源于中间件唯一计数点，可对账；
- 容量保护：
  - 维度上限沿用 MetricsRegistry 的注册/归并策略，快照不新增维度；
  - 令牌桶限流（默认 5/s、突发 10），超限返回明确信号；
  - 指标聚合结果短 TTL 缓存，高频拉取不重复计算；
- 脱敏：只含路由模板、计数、失败分类码与分位数；
  不含内部异常细节、依赖标识、连接信息。每次生成带 snapshot_id
  写入服务端日志，外部凭它关联内部记录定位问题；
- 就绪门控：readiness 为 down 时拒绝生成（SnapshotNotReady），
  由路由层映射为明确状态码，绝不返回看似正常的陈旧数据；
  degraded 仍提供服务，但 status 字段如实标注。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass

from .health import HealthChecker, HealthState
from .metrics import MetricsRegistry
from .window import WindowedMetrics

logger = logging.getLogger("telemetry.snapshot")


@dataclass(frozen=True, slots=True)
class SnapshotConfig:
    cache_ttl_seconds: float = 1.0
    rate_limit_per_second: float = 5.0
    rate_burst: int = 10


class SnapshotRateLimited(Exception):
    """拉取超出限流。retry_after 提示下次可重试的秒数。"""

    def __init__(self, retry_after: float) -> None:
        super().__init__(f"snapshot rate limited, retry after {retry_after:.3f}s")
        self.retry_after = retry_after


class SnapshotNotReady(Exception):
    """服务未就绪，拒绝提供快照。"""


class SnapshotService:
    def __init__(
        self,
        metrics: MetricsRegistry,
        windowed: WindowedMetrics,
        health: HealthChecker,
        config: SnapshotConfig | None = None,
    ) -> None:
        self._metrics = metrics
        self._windowed = windowed
        self._health = health
        self._config = config or SnapshotConfig()
        self._tokens = float(self._config.rate_burst)
        self._last_refill: float | None = None
        self._metrics_cache: tuple[float, dict] | None = None
        self._lock = threading.Lock()

    async def build(self, now: float | None = None) -> dict:
        """构建一份脱敏快照；限流/未就绪时抛对应异常。"""
        ts = time.monotonic() if now is None else now
        self._consume_token(ts)

        # 就绪门控每次都新鲜判定（不缓存），保证编排侧门控语义正确
        report = await self._health.readiness()
        if report.status is HealthState.DOWN:
            logger.warning("snapshot rejected: readiness down")
            raise SnapshotNotReady("service is not ready")

        routes = self._routes_payload(ts)
        snapshot_id = uuid.uuid4().hex
        payload = {
            "snapshot_id": snapshot_id,
            "generated_at": round(time.time(), 3),
            "status": report.status.value,
            "window_seconds": self._windowed.window_seconds,
            "routes": routes,
        }
        logger.info(
            "snapshot generated snapshot_id=%s status=%s routes=%d",
            snapshot_id, report.status.value, len(routes),
        )
        return payload

    def _consume_token(self, ts: float) -> None:
        cfg = self._config
        with self._lock:
            if self._last_refill is None:
                self._last_refill = ts
            elapsed = max(0.0, ts - self._last_refill)
            self._tokens = min(
                float(cfg.rate_burst),
                self._tokens + elapsed * cfg.rate_limit_per_second,
            )
            self._last_refill = ts
            if self._tokens < 1.0:
                retry = (1.0 - self._tokens) / cfg.rate_limit_per_second
                logger.warning("snapshot rate limited retry_after=%.3fs", retry)
                raise SnapshotRateLimited(retry)
            self._tokens -= 1.0

    def _routes_payload(self, ts: float) -> dict:
        """窗口 + 累计对照；指标聚合部分按 TTL 缓存。"""
        cfg = self._config
        with self._lock:
            cache = self._metrics_cache
            if cache is not None and ts - cache[0] < cfg.cache_ttl_seconds:
                return cache[1]
        window_stats = self._windowed.snapshot(now=ts)
        cumulative = self._metrics.snapshot()
        routes: dict[str, dict] = {}
        for route, cum in cumulative.items():
            win = window_stats.get(route)
            routes[route] = {
                "window": win.to_dict() if win is not None else _empty_window(),
                "cumulative": cum.to_dict(),
            }
        with self._lock:
            self._metrics_cache = (ts, routes)
        return routes


def _empty_window() -> dict:
    return {
        "total": 0,
        "success": 0,
        "client_errors": 0,
        "server_errors": 0,
        "error_kinds": {},
        "quantiles": {},
    }
