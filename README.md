# async-service-telemetry

基于 FastAPI（ASGI）的异步 Web 服务，内置端到端请求遥测：请求级追踪上下文、
按路由维度的时延/错误指标、健康与就绪判定。所有路径（成功、失败、并发交错）
均保持一致的计数与关联语义。

## 目录结构

```
app/
  service.py              # 应用组装：路由、探针、指标注册、中间件接线
  telemetry/
    context.py            # 追踪上下文（contextvars，含冻结的采样决定）
    metrics.py            # 指标注册表（基数受控，累计值）
    windows.py            # 滑动时间窗口聚合（p50/p95/p99）
    sampling.py           # 采样决策与传播
    snapshot.py           # 对外遥测快照（脱敏、限流、就绪门控）
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
| `failure_reasons` | 失败原因分布（见下），各原因之和恒等于 `client_errors + server_errors` |
| `latency_buckets` | 时延直方图，桶边界固定：0.005…5.0 秒 + `+Inf` |
| `latency_sum_seconds` / `latency_count` | 时延总和与样本数，可求均值 |

**失败原因归类（`failure_reasons`）**：每次失败（status ≥ 400）恰好归入
一个原因桶，取值固定为以下枚举（基数有界，可安全对外暴露）：

| 原因 | 覆盖情形 |
| --- | --- |
| `client_error` | 4xx（含参数校验失败） |
| `upstream_timeout` | 下游超时（504 / `asyncio.TimeoutError`） |
| `upstream_unavailable` | 下游不可用（503 连接失败等） |
| `dependency_degraded` | 依赖部分降级（503，熔断半开等） |
| `internal` | 服务自身内部异常（500 及其余 5xx） |

归类规则确定：异常路径以归一化的 `ErrorKind` 为准；无异常信息时按状态码
推导（4xx→`client_error`，504→`upstream_timeout`，503→`upstream_unavailable`，
其余 5xx→`internal`）。中间件在唯一计数点把**同一份**原因判定同时喂给
累计指标与滑动窗口，因此两侧按原因逐项对账一致，不会某一类重复统计或
整类漏计。

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

## 3. 滑动时间窗口

在累计指标之外，`SlidingWindowMetrics` 提供每个路由**最近一段窗口**的聚合
（默认窗口 300s、bucket 10s，共 30 个 bucket）：

- **唯一归属**：每笔请求在完成时记录一次，按完成时间戳落入且仅落入一个
  bucket（`index = floor(ts / bucket_seconds)`）。窗口前滚时，在途并发请求
  的归属由完成时刻唯一确定，不会被两个窗口重复计入、也不会漏计；
- **滚动裁剪**：过期 bucket 被裁剪，只影响窗口视图；累计指标独立维护，
  二者可对账——窗口覆盖全部历史时，窗口聚合（含 `failure_reasons`
  各原因计数）与累计值逐项相等
  （`test_window_reconciles_with_cumulative`、
  `test_window_reasons_reconcile_with_cumulative`、
  `test_concurrent_mixed_errors_timeouts_sampling_windows_reconcile`、
  `test_e2e_concurrent_mixed_reasons_reconcile_and_sampled_headers`）；
- **窗口内容**：`total / success / client_errors / server_errors`
  （与累计同口径、互斥）+ `failure_reasons`（窗口内失败原因分布，
  随所属 bucket 一起滚动过期，窗口内各原因之和恒等于窗口失败总数）
  + 延迟分位数 `p50 / p95 / p99`
  （窗口内原始样本线性插值，`rank = q*(n-1)`，无样本时为 `null`）；
- **维度一致**：窗口复用中间件归并后的 canonical 路由键，不引入新维度，
  基数约束与累计指标完全相同。

## 4. 对外遥测快照 `GET /telemetry/snapshot`

供外部编排/告警系统轮询的**只读**入口，一次返回窗口聚合与累计对照：

```json
{
  "status": "up",
  "window": {"window_seconds": 300.0, "bucket_seconds": 10.0},
  "routes": {
    "/items/{item_id}": {
      "window":     {"total": 12, "success": 10, "client_errors": 1,
                     "server_errors": 1,
                     "failure_reasons": {"client_error": 1, "upstream_timeout": 1},
                     "quantiles": {"0.5": 0.01, "0.95": 0.4, "0.99": 0.5},
                     "window_start": 1791074800.0, "window_end": 1791075100.0},
      "cumulative": {"total": 128, "success": 120,
                     "failure_reasons": {"upstream_timeout": 3, "internal": 2},
                     "...": "..."}
    }
  },
  "limits": {"route_count": 9}
}
```

窗口与累计两侧都带 `failure_reasons`，编排/告警可直接按原因分别设阈值；
同一侧内各原因之和恒等于该侧 `client_errors + server_errors`，
窗口覆盖全部历史时两侧逐项相等。

**脱敏边界**：

- 路由键全部是 canonical 模板（注册模板 / `__other__` / `__unmatched__`），
  不含具体 URL 与参数；
- 不输出任何异常内部细节、依赖地址、健康检查名等依赖标识，
  健康部分只输出整体 `status`；`failure_reasons` 的键是固定枚举
  （见第 2 节），不含异常文本，可安全对外；
- 内部关联信息（`trace_id`、路由数、限流事件）只写服务端日志，
  可凭请求头 `X-Request-Id` 在日志中定位。

**容量保护**：

- 维度上限沿用 `MetricsRegistry` 的 `max_routes`，超限走既有归并策略，
  快照不会扩张维度；
- 令牌桶限流（默认 5 req/s、突发 10），超限返回 **429**
  `{"error": {"kind": "rate_limited"}}`，拒绝是即时的，不会拖垮服务。

**就绪门控**：readiness 为 **down** 时返回 503**
`{"error": {"kind": "not_ready"}}`，不返回看似正常的数据；
**degraded** 仍返回 200 并在 `status` 字段如实标注，编排侧可据此门控。

## 5. 追踪采样

- **入口唯一决定**：中间件解析入站头 `X-Trace-Sampled: 1|0`
  （也接受 `true/false`），合法则遵从；缺失或非法时**确定性降级**为服务
  配置的默认值（`default_sampled`，默认 `true`）——同一部署内行为确定，
  不会时有时无。判定依据（`inbound` / `default`）记入请求日志；
- **冻结传播**：决定存入 frozen 的 `TraceContext.sampled`，链路中途不可
  变更，不会出现半采样半不采样。下游调用用
  `propagation_headers(ctx)` 显式携带 `X-Request-Id` + `X-Trace-Sampled`；
  后台任务经 `bind_context_run` 继承同一快照（见 `POST /work`，
  响应可对比 `sampled` 与 `background_sampled`）；
- **与指标解耦**：采样只影响追踪详细度，指标计数与窗口聚合对所有请求
  照常恰好记录一次，开关变化不会造成重复计数或窗口错乱；
- **全路径回写**：所有响应都回写 `X-Trace-Sampled` 头——包括异常路径
  （下游超时 504、下游不可用 503、内部异常 500 等由中间件统一生成的
  错误响应），标记值与入站决定严格一致；入站头缺失或非法时按同一
  确定性规则降级为配置默认值，不会时有时无。下游与后台任务据此总能
  拿到完整的采样决定。

## 6. 健康判定语义

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

## 7. 演示路由

- `GET /`、`GET /items/{item_id}`：正常路径，回显 trace_id；
- `POST /work`：追踪上下文传播到线程池后台任务；
- `GET /upstream/timeout|unavailable|degraded`：三类依赖故障；
- `GET /boom`：未预期异常（验证内部细节不外泄）；
- `POST /debug/deps/{name}?state=up|down|slow`：切换 primary（关键）/
  cache（非关键）探针状态，用于演示就绪判定。

## 8. 本地验证

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

# 采样：显式关闭 / 缺省走默认
curl -i -H 'X-Trace-Sampled: 0' http://127.0.0.1:8000/
curl -i -X POST -H 'X-Trace-Sampled: 0' http://127.0.0.1:8000/work  # 后台任务继承
# 异常路径同样回写采样标记（与入站决定一致）
curl -i -H 'X-Trace-Sampled: 0' http://127.0.0.1:8000/boom
curl -i -H 'X-Trace-Sampled: 1' http://127.0.0.1:8000/upstream/timeout

# 对外快照：窗口 + 累计对照（两侧均含 failure_reasons）
curl http://127.0.0.1:8000/upstream/timeout >/dev/null
curl http://127.0.0.1:8000/boom >/dev/null
curl -s http://127.0.0.1:8000/telemetry/snapshot \
  | jq '.routes["/boom"].cumulative.failure_reasons,
        .routes["/upstream/timeout"].window.failure_reasons'

# 对外快照：窗口 + 累计对照
curl http://127.0.0.1:8000/telemetry/snapshot | jq
# 未就绪时门控
curl -X POST 'http://127.0.0.1:8000/debug/deps/primary?state=down'
curl -i http://127.0.0.1:8000/telemetry/snapshot   # 503 not_ready
# 高频轮询触发限流
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}\n' \
  http://127.0.0.1:8000/telemetry/snapshot; done   # 后续出现 429
```

测试覆盖重点：

- 200 个并发混合请求（成功 + 三类失败交错）下追踪标识不串扰、不丢失；
- 并发压测后指标 total 与请求数精确相等，成功/失败互斥不重复计数；
- 窗口滚动时请求恰好归属一个 bucket（不重不丢），窗口聚合与累计逐路由对账一致；
- 失败原因归类：窗口内各原因之和恒等于失败总数，随 bucket 滚动过期，
  并发混合（超时/不可用/降级/内部异常/正常）下窗口与累计按原因逐项对账一致；
- 快照窗口与累计两侧均暴露 `failure_reasons`，取值为固定枚举且负载仍脱敏；
- 异常路径（504/503/500）响应与正常响应一样回写 `X-Trace-Sampled`，
  缺失/非法入站头按确定性规则降级，与正常路径行为一致；
- 并发混合（异常 + 超时 + 采样/不采样各半）下窗口与累计仍一致，采样不影响计数；
- 采样头合法遵从、非法/缺失确定性降级，决定随后台任务传播且上下文冻结不可篡改；
- 快照脱敏（无异常细节、无依赖标识）、维度上限归并、令牌桶限流 429、
  未就绪 503 门控；
- 504/503/500 及对应 kind 可区分，响应体无内部异常细节；
- 关键依赖 down、非关键依赖 degraded、探针超时 down 三类就绪判定；
- 高频/未知路径归并到固定桶，指标维度有上界。
