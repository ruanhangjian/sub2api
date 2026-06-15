# 阶段8 验收修复（Sub2API 侧）

日期：2026-06-15
分支：`feature/subpilot-report-integration`（已合并 `feature/subpilot-lease-report-fix` + `feature/subpilot-probe-auth`）

## 背景

阶段5-7 已接入 SubPilot select / report / probe endpoint，但验收发现：
- lease_id 从 select 拿到后丢失，导致所有 report 被 SubPilot validateReport 拒绝。
- OpenAI success 完全不上报 SubPilot；failure report 缺关键字段。
- official_usd_used 固定传 0，24h 成本不准。
- 内部 probe endpoint 无鉴权。

## 分支与合并

| 分支 | 问题 | commit |
|---|---|---|
| `feature/subpilot-lease-report-fix` | 1/5/7 | `2c85fc6` |
| `feature/subpilot-probe-auth` | 6 | `e338ff5` |
| 合并到 `feature/subpilot-report-integration` | — | 两个 `--no-ff` merge |

两个分支基于同一基线，改不同文件，合并无冲突。

## 改动文件

### lease-report（问题1/5/7）
| 文件 | 改动 |
|---|---|
| `service/gateway_service.go` | 问题1：AccountSelectionResult 加 SubPilotLeaseID；问题7：official_usd_used=TotalCost |
| `service/subpilot_integration.go` | 问题1：两个 try 函数设置 selection.SubPilotLeaseID |
| `service/subpilot_report.go` | 问题1：导出 WithSubPilotLeaseID；问题5：reportSuccessFromUsageLog 提包级 + reportFailureForOpenAI 扩宽 |
| `service/openai_gateway_service.go` | 问题5：RecordUsage 补 success report + reportSuccessFromUsageLogForOpenAI |
| `service/openai_account_scheduler.go` | 问题5：ReportOpenAIAccountScheduleResult 签名加 SubPilotFailContext |
| `handler/gateway_handler.go` | 问题1：2 处 select 点合并 lease |
| `handler/gateway_handler_responses.go` / `_chat_completions.go` | 问题1：select 点合并 lease |
| `handler/gemini_v1beta_handler.go` | 问题1：select 点合并 lease |
| `handler/openai_gateway_handler.go` / `_chat_completions.go` / `_images.go` / `_embeddings.go` | 问题1：6 处 select 合并 lease；问题5：23 处 report 补 SubPilotFailContext |
| `handler/ops_error_logger.go` | 问题1：applySubPilotLease helper |
| `service/subpilot_report_test.go` | 4 个新测试 |
| `service/openai_account_scheduler_test.go` | 签名更新 |

### probe-auth（问题6）
| 文件 | 改动 |
|---|---|
| `config/config.go` | SubPilotConfig 加 ProbeSecret |
| `server/router.go` | 传 cfg 给 RegisterAdminRoutes |
| `server/routes/admin.go` | 加 subPilotProbeSecretMiddleware（默认拒绝） |
| `handler/admin/subpilot_probe_handler.go` | 更新注释 |
| `server/routes/probe_auth_test.go` | 4 个鉴权测试 |

## 各问题实现

### 问题1：lease_id 丢失
- `AccountSelectionResult` 新增 `SubPilotLeaseID` 字段。
- 两个 try 函数把 `recommend.LeaseID` 写入 selection。
- 导出 `WithSubPilotLeaseID` / `SubPilotLeaseIDFromContext`。
- 新增 `applySubPilotLease(c, leaseID)` handler helper，在全部 11 个 select
  调用点（Claude/Gemini 5 + OpenAI 6）合并 lease_id 到 `c.Request.Context()`。
- 后续 report-success/report-failure 从 ctx 读 lease_id 并释放 lease。

### 问题5：OpenAI success/failure report
- `reportSuccessFromUsageLog` 提取为包级函数；新增
  `reportSuccessFromUsageLogForOpenAI` 供 OpenAI service 调用。
- OpenAI `RecordUsage` 落库后补齐 success report
  （request_id/lease_id/group_id/model/latency_ms/first_token_ms/official_usd_used）。
- `reportFailureForOpenAI` 扩宽为 `SubPilotFailContext{Ctx, GroupID, Model}`，
  补齐 group_id/model/lease_id/request_id。
- `ReportOpenAIAccountScheduleResult` 签名加 `SubPilotFailContext`，23 个调用点更新。
- **分级处理**：无 lease_id（非 SubPilot 选的请求）时跳过失败 report，不假装闭环。

### 问题7：24h 成本统计
- Claude `recordUsageCore` 两处 official_usd_used 从固定 0 改为 `usageLog.TotalCost`
  （官方美元额度，pre-markup，由 billing_service 按官方 per-token 费率算出）。
- OpenAI success report 同样传 `usageLog.TotalCost`。
- **说明**：TotalCost 是基于 token 数 × LiteLLM 官方费率的估算值，非上游实际发票。
  channel 自定义定价的模型 TotalCost 反映 admin 设定价，非官方价（需注意）。

### 问题6：内部 probe endpoint 鉴权
- `SubPilotConfig` 新增 `ProbeSecret`（env `GATEWAY_SUBPILOT_PROBE_SECRET`）。
- `/internal/subpilot` 组加 `subPilotProbeSecretMiddleware`：校验 `X-SubPilot-Secret`。
- **安全默认**：未配置 secret 时默认拒绝所有 probe 请求（401）。
- `subtle.ConstantTimeCompare` 常量时间比较防时序攻击。

## select/report/probe 链路变化

```
select
  → trySubPilotRecommend → recommend.LeaseID 写入 selection.SubPilotLeaseID
  → handler applySubPilotLease 合并到 ctx
  → 请求转发
  → 成功：RecordUsage → reportSuccessFromUsageLog(含 lease_id from ctx) → SubPilot 释放 lease
  → 失败：reportFailureForOpenAI(含 lease_id from ctx) → SubPilot 更新健康 + 释放 lease

probe（SubPilot 侧）
  → 委托 Sub2API /internal/subpilot/probe/:id（带 X-SubPilot-Secret）
  → Sub2API 中间件校验 secret → 通过则 TestAccountConnection 探测
```

## 测试结果

- `go test ./internal/service/...`：通过（含 4 个新 report 测试）。
- `go test ./internal/handler/...`：通过。
- `go test ./internal/server/...`：通过（含 4 个 probe 鉴权测试）。
- 关键验证：
  - lease_id 从 ctx 流入 success report body。
  - OpenAI success report 含 first_token_ms + official_usd_used（非 0）。
  - 无 lease_id 时失败 report 跳过（分级处理）。
  - 有 lease_id 时失败 report 含 group_id/model/lease_id。
  - probe endpoint 未配置 secret / 错误 header → 401；正确 header → 200。

## 已知风险和未完成事项

- **Claude 失败 report 尚未接入**：`reportFailureToSubPilot` 仍 0 调用。
  Claude failover 逻辑复杂，账号级失败上报点未确定。当前 Claude 失败靠
  SubPilot 侧 probe 探测发现（闭环但不实时）。建议后续在 Claude 上游错误
  failover 点接入。lease 释放靠 success report + lease TTL 自动过期兜底。
- **OpenAI failure report 的 lease 依赖**：只有走 SubPilot 推荐的请求
  （有 lease_id）才上报失败，原生调度失败不上报（符合分级处理要求）。
- **TotalCost 估算性质**：channel 自定义定价模型 TotalCost 非官方价。

## 是否需要生产配置变更

是。启用 SubPilot 接入需要：
- `GATEWAY_SUBPILOT_ENABLED=true`
- `GATEWAY_SUBPILOT_BASE_URL`：SubPilot 地址
- `GATEWAY_SUBPILOT_PROBE_SECRET`：与 SubPilot 端 SUB2API_PROBE_SECRET 一致（启用委托探测时必须）
- 默认仍安全：SubPilot 功能默认关闭（Enabled=false）。

## 回滚方式

- `git revert <merge-commit>` 回滚对应合并。
- 或设置 `GATEWAY_SUBPILOT_ENABLED=false` 关闭全部 SubPilot 功能（fail-open 回原生调度）。
