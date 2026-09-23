# async-service-telemetry

基于 FastAPI 的异步 Web 服务，内置端到端请求遥测与可观测性能力：
请求级追踪上下文传播、按路由维度的时延/错误指标、健康与就绪判定、
错误归一化。

## 追踪传播模型

- 入站请求可携带 W3C `traceparent` 头（`version-trace_id-span_id-flags`）。
  合法时复用其 `trace_id` 并以其 `span_id` 为父 span，本服务派生自己的
  span；**缺失或非法（含全零 ID、大写、格式错误）时确定性地生成全新
  trace_id/span_id，绝不静默透传非法标识**。
- 上下文通过 `contextvars` 绑定到协程上下文（`app/telemetry/tracing.py`），
  并发/交错请求互不串扰；中间件在请求结束时复位 token，不泄漏到下一请求。
- 下游调用与后台任务通过 `TraceContext.child()` 派生子 span（同一
  `trace_id`，新 `span_id`）；`asyncio.create_task` 会拷贝当前上下文，
  后台任务自动继承请求上下文。
- 响应头回写 `x-request-id`（`trace_id` 前 16 位）与 `traceparent`，
  供客户端与日志关联。

## 指标命名与维度约束

`MetricsRegistry`（`app/telemetry/metrics.py`）按路由维度累计：

| 字段 | 含义 |
| --- | --- |
| `count` | 请求总数（成功与失败各计一次，不重复计数） |
| `error_count` / `error_rate` | 5xx 或异常路径计为错误 |
| `latency_sum_ms` / `latency_max_ms` | 时延汇总 |
| `buckets` | 累计式直方图（5/10/25/50/100/250/500/1000/5000ms） |

维度约束与归并策略：

- 标签只使用**路由模板**（如 `/items/{item_id}`），不使用原始路径，
  路径参数不会撑爆维度；
- 未匹配路由（404）统一归入 `__unknown__`；
- 路由数超过 `max_routes`（默认 100）后，新路由确定性地归入
  `__other__`，已有路由不受影响；归并桶不占配额。

通过 `GET /metrics` 查看当前快照。

## 健康判定语义

`GET /healthz` / `GET /readyz` 并发执行所有依赖检查
（`app/telemetry/health.py`）：

| 总体状态 | 条件 |
| --- | --- |
| `healthy` | 所有依赖检查通过 |
| `degraded` | 仅非关键依赖（`critical=False`）失败 |
| `unhealthy` | 任一关键依赖失败或检查超时（readyz 返回 503） |

- 单个检查由 `asyncio.timeout` 截断，超时记为 `reason=timeout`；
- 失败原因经 `errors.normalize` 归一化为稳定原因码（`timeout` /
  `dependency_unavailable` / `upstream_5xx` / `upstream_error` /
  `connection_failed` / `internal`），**对外不暴露异常消息、堆栈、
  内部地址等细节**；内部细节仅随 `request_id` 写入服务端日志用于定位。

## 错误归一化

未捕获异常由 `TelemetryMiddleware` 归一化后返回
`{"error": <reason>, "request_id": <id>}` 与对应状态码
（timeout→504，upstream→502，unavailable→503，internal→500），
失败路径同样计入指标且只计一次。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest           # 全部测试（33 个，含并发与异常路径）
uv run ruff check .     # 静态检查
uv run uvicorn main:app --reload   # 本地启动
```

测试覆盖要点（日志中会打印 `request_id` 与判定依据）：

- `tests/test_tracing.py`：traceparent 解析/补全、并发协程上下文不串扰；
- `tests/test_metrics.py`：成功/失败各计一次、桶累计、超限归并、
  并发记录不重复计数；
- `tests/test_health.py`：超时/不可用/部分降级原因可区分；
- `tests/test_e2e.py`：30 路并发请求无串扰、下游与后台任务传播、
  失败原因码区分、指标维度归一化。

手动验证：

```bash
curl -i -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' localhost:8000/work
curl -i localhost:8000/fail/timeout
curl -s localhost:8000/metrics | jq
curl -s localhost:8000/healthz | jq
```
