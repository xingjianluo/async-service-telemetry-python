"""追踪采样决策与传播。

语义：
- 采样决定在入站时**一次性**做出，存入不可变 TraceContext，
  整条链路（含下游调用、后台任务）携带同一份决定，不会被拆成
  一半采样一半不采样；
- 决定通过请求头 ``x-trace-sampled``（"1"/"0"）传播；
- 降级行为是确定性的：下游/上游不识别或给出非法值时，
  按策略默认比例对 trace_id 做确定性哈希判定——同一 trace_id
  永远得到同一结果，不会时有时无；
- 采样只控制详细追踪（span 级日志）的开关，**不影响指标计数**，
  指标在任何采样决定下都恰好记录一次。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .context import TRACE_HEADER

SAMPLED_HEADER = "x-trace-sampled"

_TRUE_VALUES = {"1", "true"}
_FALSE_VALUES = {"0", "false"}

# 判定依据（稳定、机器可读，写入日志）
BASIS_INBOUND = "inbound_header"
BASIS_FALLBACK_INVALID = "invalid_header_fallback"
BASIS_POLICY = "policy_default"


@dataclass(frozen=True, slots=True)
class SamplingPolicy:
    """采样策略：无显式决定时按 default_rate 对 trace_id 确定性采样。"""

    default_rate: float = 1.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.default_rate <= 1.0:
            raise ValueError("default_rate must be within [0.0, 1.0]")


def parse_sampled_header(raw: str | None) -> bool | None:
    """解析入站采样头；缺失或非法返回 None（调用方走确定性降级）。"""
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return None


def decide_sampling(explicit: bool | None, trace_id: str,
                    policy: SamplingPolicy) -> tuple[bool, str]:
    """做出一次性采样决定，返回 (sampled, basis)。

    - explicit 非 None：显式决定优先（来自入站头）；
    - explicit 为 None：按策略对 trace_id 做确定性哈希，
      同一 trace_id 结果恒定（重试/跨跳一致）。
    """
    if explicit is not None:
        return explicit, BASIS_INBOUND
    rate = policy.default_rate
    if rate >= 1.0:
        return True, BASIS_POLICY
    if rate <= 0.0:
        return False, BASIS_POLICY
    digest = hashlib.sha256(trace_id.encode("utf-8")).digest()
    fraction = int.from_bytes(digest[:8], "big") / 2**64
    return fraction < rate, BASIS_POLICY


def propagation_headers(trace_id: str, sampled: bool) -> dict[str, str]:
    """生成向下游/后台传播用的请求头（含采样决定，保证链路一致）。"""
    return {
        TRACE_HEADER: trace_id,
        SAMPLED_HEADER: "1" if sampled else "0",
    }
