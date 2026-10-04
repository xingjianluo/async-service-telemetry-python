# async-service-telemetry

基于 FastAPI（ASGI）的异步 Web 服务，内置端到端请求遥测：请求级追踪上下文、
按路由维度的时延/错误指标、健康与就绪判定。所有路径（成功、失败、并发交错）
均保持一致的计数与关联语义。

## 目录结构

```
app/
  service.py              # 应用组装：路由、探针、指标注册、中间件接线
  telemetry/
    context.py            # 追踪上下文（contextvars）
    metrics.py            # 指标注册表（基数受控）
    errors.py             # 异常归一化（安全分类）
    health.py             # liveness/readiness 判定
    middleware.py         # 入站遥测中间件（纯 ASGI）
tests/                    # 单测与并发端到端测试
```

## 1. 追踪传播模型

- **标识来源**：入站请求头 `X-Request-Id`，合法格式为
  `^[A-Za-z0-9._-]{8,128}$`。缺失或非法时，中间件确定性补全一个 uuid4 hex
  （32 字符），上下文 `source` 标记为 `inbound` / `generated`，
  **绝不静默透传非法值**。
- **隔离机制**：使用 `contextvars.ContextVar` 绑定 `TraceContext`，
  每个 ASGI 请求在 `bind_context()` 作用域内运行。asyncio 每个 Task 持有
  独立的 context 拷贝，因此并发或交错请求之间不会串扰，作用域退出即恢复，
  不向后续请求泄漏。
- **向下游/后台传播**：
  - `asyncio.create_task` / `asyncio.gather` 自动复制当前 context；
  - 线程池场景（`run_in_executor`）不会自动复制，必须用
    `bind_context_run(ctx, func)` 显式携带，见 `POST /work` 示例；
  - 未绑定上下文时调用 `get_context()` 抛出 `LookupError`，不伪造标识。
- **关联回写**：所有响应（含错误响应）都带 `X-Request-Id` 响应头；
  错误 JSON 中包含 `trace_id`，但不含任何内部异常文本。

## 2. 指标命名与维度约束

指标按**路由模板**（如 `/items/{item_id}`，而非具体路径）汇总，
每个维度包含：

| 字段 | 含义 |
| --- | --- |
| `total` | 该路由请求总数 |
| `success` / `client_errors` / `server_errors` | 互斥三类（<400 / 4xx / 5xx），三者之和恒等于 `total` |
| `latency_buckets` | 时延直方图，桶边界固定：0.005…5.0 秒 + `+Inf` |
| `latency_sum_seconds` / `latency_count` | 时延总和与样本数，可求均值 |

**唯一计数点**：中间件 `finally` 块中恰好调用一次 `record()`，成功与异常
路径都经过它，因此不重不漏。

**基数约束（可预期归并）**：

- 只有显式 `register_route()` 注册的模板拥有独立维度（应用启动时从
  FastAPI 路由表批量注册）；
- 已识别但未注册的模板 → 固定归并桶 `__other__`；
- 无匹配路由（任意 404 路径）→ 固定归并桶 `__unmatched__`；
- 注册数量超过 `max_routes`（默认 32）时直接抛错，属配置期问题，
  不允许维度静默扩张。

测试用 500 条高频未注册路径与 300 条不同 404 路径验证了维度不膨胀。

## 3. 健康判定语义

| 端点 | 语义 |
| --- | --- |
| `GET /health/live` | 进程存活即 `up`，不探测依赖，避免依赖抖动引发级联重启 |
| `GET /health/ready` | 并发执行依赖探针，单探针超时 0.2s，按下表聚合 |

就绪聚合规则：

- 任一**关键**（critical）探针 `down` / 超时 / 抛异常 → 整体 **down**；
- 仅**非关键**探针异常 → 整体 **degraded**（部分降级，与 down 可区分）；
- 全部正常 → **up**。

可区分的原因码（`reason`，稳定、机器可读）：`ok`、`probe_timeout`、
`probe_error`，探针也可自定义（如 `cache_unavailable`）。

**信息隔离**：对外负载只含 `status` 与每个检查的 `state/reason`；
异常堆栈、DSN、连接地址等内部细节仅写入服务端日志。请求错误同理，
对外只返回归一化分类：

| 异常情形 | HTTP | kind |
| --- | --- | --- |
| 下游超时（`asyncio.TimeoutError`） | 504 | `upstream_timeout` |
| 下游不可用（连接失败等） | 503 | `upstream_unavailable` |
| 部分降级 | 503 | `dependency_degraded` |
| 其余未预期异常 | 500 | `internal` |

错误日志记录 `trace_id + kind + 内部细节`，可凭响应中的 `trace_id`
在日志中定位完整链路。

## 4. 演示路由

- `GET /`、`GET /items/{item_id}`：正常路径，回显 trace_id；
- `POST /work`：追踪上下文传播到线程池后台任务；
- `GET /upstream/timeout|unavailable|degraded`：三类依赖故障；
- `GET /boom`：未预期异常（验证内部细节不外泄）；
- `POST /debug/deps/{name}?state=up|down|slow`：切换 primary（关键）/
  cache（非关键）探针状态，用于演示就绪判定。

## 5. 本地验证

```bash
uv sync
uv run pytest                 # 全部单测 + 并发端到端测试（日志打印 trace_id 与判定依据）
uv run uvicorn main:app       # 启动服务

# 正常请求（传入追踪标识）
curl -i -H 'X-Request-Id: demo-trace-0001' http://127.0.0.1:8000/
# 非法标识 → 被确定性补全
curl -i -H 'X-Request-Id: bad id!!' http://127.0.0.1:8000/
# 故障分类
curl -i http://127.0.0.1:8000/upstream/timeout
# 健康/就绪
curl http://127.0.0.1:8000/health/live
curl -X POST 'http://127.0.0.1:8000/debug/deps/cache?state=down'
curl http://127.0.0.1:8000/health/ready   # degraded
```

测试覆盖重点：

- 200 个并发混合请求（成功 + 三类失败交错）下追踪标识不串扰、不丢失；
- 并发压测后指标 total 与请求数精确相等，成功/失败互斥不重复计数；
- 504/503/500 及对应 kind 可区分，响应体无内部异常细节；
- 关键依赖 down、非关键依赖 degraded、探针超时 down 三类就绪判定；
- 高频/未知路径归并到固定桶，指标维度有上界。

## 6. 滑动时间窗口指标

在累计指标之外，`WindowedMetrics`（`app/telemetry/window.py`）按路由维护
**滑动窗口**聚合（默认窗口 300s、桶 10s，见 `service.py` 常量）：

- **落桶唯一**：请求在完成时（中间件 `finally` 中唯一计数点）落入
  *完成时刻*对应的桶。一笔请求只被记录一次，因此必落在且仅落在一个桶里——
  正在并发处理的请求不会因为窗口滚动被两个窗口各算一次，也不会漏算；
- **滚动语义**：窗口 = 最近 `window_seconds / bucket_seconds` 个桶，
  旧桶随时间被复用重置。滚动只改变窗口聚合的可见范围，**不回写累计值**；
  任意时刻 `窗口 total <= 累计 total`，未发生滚动时两者相等（有单测对账）；
- **窗口内容**：`total / success / client_errors / server_errors`、
  失败分类 `error_kinds`（归一化 kind 或 `http_4xx`/`http_5xx`，取值有界）、
  由固定边界直方图近似的分位数 `p50 / p95 / p99`（内存有界）。

## 7. 对外遥测快照（编排/告警拉取）

`GET /telemetry/snapshot`：只读入口，供外部系统轮询。

**字段**（全部脱敏）：

| 字段 | 含义 |
| --- | --- |
| `snapshot_id` | 本次快照标识，已写入服务端日志，可凭它关联内部记录定位 |
| `generated_at` / `window_seconds` | 生成时间与窗口长度 |
| `status` | 就绪判定（`up` / `degraded`） |
| `routes.<模板>.window` | 窗口聚合：计数、失败分类、p50/p95/p99 |
| `routes.<模板>.cumulative` | 累计对照：计数与时延直方图/总和 |

**脱敏边界**：只含路由模板、计数、稳定分类码与分位数；不含内部异常细节、
依赖标识、连接信息（这些只出现在服务端日志，用 `snapshot_id`/`trace_id` 关联）。

**容量保护**：

- 维度上限沿用 `MetricsRegistry` 的注册/归并策略，快照不新增维度；
- 令牌桶限流（默认 5/s、突发 10），超限返回 **429** + `Retry-After`；
- 指标聚合结果短 TTL（1s）缓存，高频拉取不重复计算；
  就绪判定每次新鲜执行，不被缓存。

**就绪门控**：readiness 为 `down` 时返回 **503** `{"error": {"kind": "not_ready"}}`，
绝不返回看似正常的陈旧数据；`degraded` 仍返回 200 并在 `status` 如实标注。

## 8. 追踪采样

- **决定时机**：入站时一次性做出，写入不可变 `TraceContext.sampled`，
  整条链路（含下游调用、后台任务）恒定不变——采样开关变化只影响新请求，
  不会把同一条链路拆成一半采样一半不采样；
- **传播**：请求头 `X-Trace-Sampled: 1|0`（兼容 `true/false`）；
  响应头回写该决定；`propagation_headers()` 生成下游调用头
  （`POST /work` 演示了 trace_id + 采样决定传入线程池后台任务与下游头）；
- **降级**：下游/上游不识别（缺头或非法值）时，按 `SamplingPolicy.default_rate`
  对 trace_id 做**确定性哈希**判定——同一 trace_id 结果恒定，不会时有时无；
- **与指标的关系**：采样只控制详细追踪（span 级日志）的开关，
  **指标与窗口统计在任何采样决定下都恰好记录一次**；
- 日志中 `sampling_basis`（`inbound_header` / `policy_default`）记录判定依据。

## 9. 本地验证（新增能力）

```bash
uv run pytest                 # 含窗口/采样/快照单测与混合并发端到端测试

uv run uvicorn main:app       # 启动服务

# 采样：显式决定随响应头回写，并传入后台任务与下游头
curl -i -H 'X-Request-Id: demo-trace-0001' -H 'X-Trace-Sampled: 0' \
     -X POST http://127.0.0.1:8000/work
# 缺头/非法头 → 确定性降级（同一 trace_id 多次请求结果一致）
curl -i -H 'X-Request-Id: demo-trace-0001' http://127.0.0.1:8000/

# 造一些流量后拉取对外快照（窗口 + 累计对照）
curl http://127.0.0.1:8000/boom; curl http://127.0.0.1:8000/upstream/timeout
curl http://127.0.0.1:8000/telemetry/snapshot | jq

# 就绪门控：依赖 down 时快照返回 503
curl -X POST 'http://127.0.0.1:8000/debug/deps/primary?state=down'
curl -i http://127.0.0.1:8000/telemetry/snapshot   # 503 not_ready
curl -X POST 'http://127.0.0.1:8000/debug/deps/primary?state=up'

# 限流：快速连续拉取超过突发额度后出现 429 + Retry-After
for i in $(seq 1 15); do curl -s -o /dev/null -w '%{http_code}\n' \
     http://127.0.0.1:8000/telemetry/snapshot; done
```

新增测试覆盖重点：

- 窗口：一笔请求只落一个桶、滚动不丢不重、在途请求按完成时刻落窗、
  窗口与累计对账、失败分类与 p50/p95/p99；
- 采样：显式决定优先、缺头/非法头确定性降级且不漂移、决定随响应头/
  后台任务/下游头一致传播、采样混合下指标仍恰好计一次；
- 快照：窗口+累计对照、脱敏（无内部细节/依赖标识）、限流 429、
  TTL 缓存、未就绪 503 门控；
- 端到端：并发混合（成功/异常/超时 × 采样开/关/缺头）下窗口合计
  与累计合计精确相等。
