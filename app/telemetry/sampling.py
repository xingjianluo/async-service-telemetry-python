"""追踪采样决策与传播。

语义：
- 每条链路的采样决定在入口唯一做出，随后冻结在 TraceContext 中，
  链路中途不可变更（frozen dataclass），不会出现半采样半不采样；
- 决定随下游调用（显式头部 x-trace-sampled: 1/0）与
  后台任务（contextvars 快照）传播，保持一致；
- 头部缺失或非法时走确定性降级：使用服务配置的默认值，
  同一部署内行为确定，绝不时有时无；
- 采样只影响追踪详细度，不影响指标计数与窗口聚合，
  开关变化不会造成重复计数或窗口错乱。
"""
from __future__ import annotations

SAMPLED_HEADER = "x-trace-sampled"
SAMPLED_BYTES = b"x-trace-sampled"

# 采样决定来源（用于日志中的判定依据）
DECISION_INBOUND = "inbound"
DECISION_DEFAULT = "default"

_TRUE_VALUES = {"1", "true"}
_FALSE_VALUES = {"0", "false"}


def parse_sampled_header(raw: str | None) -> bool | None:
    """解析采样头；缺失或非法返回 None（交由确定性降级）。"""
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return None


def decide_sampling(raw: str | None, default: bool = True) -> tuple[bool, str]:
    """在入口唯一做出采样决定，返回 (sampled, 判定依据)。

    规则确定：头部合法 -> 遵从头部；否则 -> 配置默认值。
    """
    parsed = parse_sampled_header(raw)
    if parsed is not None:
        return parsed, DECISION_INBOUND
    return default, DECISION_DEFAULT


def format_sampled_header(sampled: bool) -> str:
    """向下游传播时的规范头部取值。"""
    return "1" if sampled else "0"
