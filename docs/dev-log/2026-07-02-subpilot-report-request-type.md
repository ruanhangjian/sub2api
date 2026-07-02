# SubPilot report 请求类型上报

## 修改背景

SubPilot 需要区分真实请求中的 stream 与 sync。sync/非流式请求没有首 token 时间，如果只上报总耗时，SubPilot 容易把总耗时误判为首字慢请求。

## 修改文件

- `backend/internal/service/subpilot_report.go`
- `backend/internal/service/subpilot_report_test.go`

## 数据口径变化

- success report 新增：
  - `request_type`: 使用 `UsageLog.EffectiveRequestType().String()`，例如 `sync`、`stream`、`ws_v2`。
  - `stream`: 使用 `UsageLog.Stream`。
- failure report 结构体也预留 `request_type` 与 `stream` 字段，方便后续失败路径接入。
- 非流式 success 不会伪造 `first_token_ms`，仍然只在 `UsageLog.FirstTokenMs > 0` 时上报。

## 测试结果

- `docker run --rm -v "$PWD":/src -w /src/backend golang:1.26 go test ./internal/service -run SubPilot -count=1` 通过。

## 未完成项/风险点

- OpenAI failure report 当前没有完整请求类型上下文，暂未给失败路径实际填充 request_type/stream。
- SubPilot 已通过 usage_logs 反查和开关兜底处理历史/缺字段 report，新字段主要提升新请求的准确性。

## 数据库和配置

无需 Sub2API 数据库 migration。无需新增环境变量。
