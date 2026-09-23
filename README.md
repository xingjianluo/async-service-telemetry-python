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
