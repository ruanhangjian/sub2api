package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// subpilotReportSuccessRequest 对应 SubPilot /v1/dispatch/report-success。
type subpilotReportSuccessRequest struct {
	RequestID       string  `json:"request_id"`
	LeaseID         string  `json:"lease_id,omitempty"`
	APIKeyID        string  `json:"api_key_id,omitempty"`
	AccountID       string  `json:"account_id"`
	Platform        string  `json:"platform"`
	GroupID         string  `json:"group_id"`
	Model           string  `json:"model"`
	LatencyMS       int     `json:"latency_ms,omitempty"`
	FirstTokenMS    int     `json:"first_token_ms,omitempty"`
	OfficialUSDUsed float64 `json:"official_usd_used,omitempty"`
}

// subpilotReportFailureRequest 对应 SubPilot /v1/dispatch/report-failure。
type subpilotReportFailureRequest struct {
	RequestID     string `json:"request_id"`
	LeaseID       string `json:"lease_id,omitempty"`
	APIKeyID      string `json:"api_key_id,omitempty"`
	AccountID     string `json:"account_id"`
	Platform      string `json:"platform"`
	GroupID       string `json:"group_id"`
	Model         string `json:"model"`
	StatusCode    int    `json:"status_code,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
}

// subpilotLeaseFromContext 从 ctx 中取出 select 阶段 SubPilot 返回的 lease_id（若有）。
// 由 select 接入层在选中 SubPilot 推荐账号时写入 ctx。
type subpilotLeaseCtxKey struct{}

// WithSubPilotLeaseID 把 SubPilot /select 返回的 lease_id 写入 ctx。
// handler 在拿到含 SubPilotLeaseID 的 AccountSelectionResult 后应调用本函数
// 把 lease_id 合并到 c.Request.Context()，使后续 report 能释放 lease（问题1）。
// leaseID 为空时原样返回 ctx（原生调度路径无 lease）。
func WithSubPilotLeaseID(ctx context.Context, leaseID string) context.Context {
	if leaseID == "" {
		return ctx
	}
	return context.WithValue(ctx, subpilotLeaseCtxKey{}, leaseID)
}

// SubPilotLeaseIDFromContext 从 ctx 取出 lease_id，缺失返回空串。
func SubPilotLeaseIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(subpilotLeaseCtxKey{}).(string); ok {
		return v
	}
	return ""
}

// withSubPilotLeaseID 是 WithSubPilotLeaseID 的包内别名，保持向后兼容。
func withSubPilotLeaseID(ctx context.Context, leaseID string) context.Context {
	return WithSubPilotLeaseID(ctx, leaseID)
}

func subPilotLeaseIDFromContext(ctx context.Context) string {
	return SubPilotLeaseIDFromContext(ctx)
}

// reportSuccessToSubPilot 把成功请求结果上报给 SubPilot。best-effort：任何错误都静默忽略，
// 绝不影响用户请求。disabled 模式下直接返回。
//
// 该方法在 recordUsageCore 落库成功后调用，覆盖所有平台（Claude / OpenAI / Gemini）。
func (s *GatewayService) reportSuccessToSubPilot(ctx context.Context, req subpilotReportSuccessRequest) {
	sp := subPilotConfig(s.cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return
	}
	// report 走独立的短超时 ctx，不绑定用户请求的剩余时间。
	reportCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	subPilotClientSingleton.reportSuccess(reportCtx, sp, req)
}

// reportFailureToSubPilot 把失败请求结果上报给 SubPilot。best-effort。
func (s *GatewayService) reportFailureToSubPilot(ctx context.Context, req subpilotReportFailureRequest) {
	sp := subPilotConfig(s.cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	subPilotClientSingleton.reportFailure(reportCtx, sp, req)
}

// reportSuccess 实现 SubPilot /report-success 的 HTTP POST。任何错误都静默忽略。
func (c *SubPilotClient) reportSuccess(ctx context.Context, cfg config.SubPilotConfig, req subpilotReportSuccessRequest) {
	body, err := json.Marshal(req)
	if err != nil {
		return
	}
	url := strings.TrimRight(cfg.BaseURL, "/") + "/v1/dispatch/report-success"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// reportFailure 实现 SubPilot /report-failure 的 HTTP POST。任何错误都静默忽略。
func (c *SubPilotClient) reportFailure(ctx context.Context, cfg config.SubPilotConfig, req subpilotReportFailureRequest) {
	body, err := json.Marshal(req)
	if err != nil {
		return
	}
	url := strings.TrimRight(cfg.BaseURL, "/") + "/v1/dispatch/report-failure"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// SubPilotFailContext 携带失败 report 所需的请求级上下文。
// 问题5：OpenAI 失败 report 此前缺 group_id/model/lease_id 被 SubPilot 拒绝。
// handler 在调用 ReportOpenAIAccountScheduleResult 时传入。
type SubPilotFailContext struct {
	Ctx     context.Context // 携带 lease_id（由 applySubPilotLease 合并）；为 nil 则跳过 report
	GroupID *int64          // 分组 ID
	Model   string          // 请求模型
}

// reportFailureForOpenAI 是 OpenAIGatewayService 的失败 report 入口。
// 在 ReportOpenAIAccountScheduleResult(success=false) 时调用，向 SubPilot 上报
// 该账号本次请求失败。best-effort，任何错误都静默忽略。
//
// 问题5：补齐 group_id/model/lease_id/request_id。
// 关键约束：只在 lease_id 非空时才 report——lease_id 为空说明该请求不是 SubPilot
// 推荐的（原生调度失败），不涉及 lease 释放，report 也会被 SubPilot validateReport 拒绝，
// 故直接跳过（不假装闭环）。
func (s *OpenAIGatewayService) reportFailureForOpenAI(accountID int64, fail SubPilotFailContext) {
	sp := subPilotConfig(s.cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return
	}
	// 没有 ctx 或 lease_id → 不是 SubPilot 选的请求，跳过（report 会被拒）。
	var leaseID, requestID string
	if fail.Ctx != nil {
		leaseID = subPilotLeaseIDFromContext(fail.Ctx)
		requestID = requestIDFromContext(fail.Ctx)
	}
	if leaseID == "" {
		return
	}
	if requestID == "" {
		// 兜底：构造一个可追溯的 request_id。
		requestID = "openai-schedule-" + strconv.FormatInt(accountID, 10) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	req := subpilotReportFailureRequest{
		RequestID:    requestID,
		LeaseID:      leaseID,
		AccountID:    strconv.FormatInt(accountID, 10),
		Platform:     platformForSubPilot(PlatformOpenAI),
		Model:        fail.Model,
		ErrorCode:    "upstream_error",
		ErrorMessage: "openai account schedule reported failure",
	}
	if fail.GroupID != nil {
		req.GroupID = strconv.FormatInt(*fail.GroupID, 10)
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	subPilotClientSingleton.reportFailure(reportCtx, sp, req)
}

// reportSuccessFromUsageLog 从 recordUsageCore 已构建好的 usageLog + result + account
// 组装 SubPilot success report。best-effort，任何字段缺失都不报错。
// 问题5：提取为包级函数，Claude（*GatewayService）和 OpenAI（*OpenAIGatewayService）
// 共用同一套 success report 组装逻辑。
func reportSuccessFromUsageLog(cfg *config.Config, ctx context.Context, usageLog *UsageLog, account *Account, platform string, officialUSD float64) {
	if usageLog == nil || account == nil {
		return
	}
	sp := subPilotConfig(cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return
	}
	req := subpilotReportSuccessRequest{
		RequestID:       usageLog.RequestID,
		AccountID:       strconv.FormatInt(account.ID, 10),
		Platform:        platformForSubPilot(platform),
		Model:           usageLog.Model,
		OfficialUSDUsed: officialUSD,
		LeaseID:         subPilotLeaseIDFromContext(ctx),
	}
	if usageLog.DurationMs != nil {
		req.LatencyMS = *usageLog.DurationMs
	}
	if usageLog.GroupID != nil {
		req.GroupID = strconv.FormatInt(*usageLog.GroupID, 10)
	}
	// first_token_ms 直接取自 Sub2API 已自算的 usageLog.FirstTokenMs。
	// Sub2API 在 SSE stream reader 第一块有效 data 到达时已记录该值，
	// 无需 SubPilot 改流式转发逻辑。非流式请求 FirstTokenMs 为 nil，不报。
	if usageLog.FirstTokenMs != nil && *usageLog.FirstTokenMs > 0 {
		req.FirstTokenMS = *usageLog.FirstTokenMs
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	subPilotClientSingleton.reportSuccess(reportCtx, sp, req)
}

// reportSuccessFromUsageLog 是 Claude/Gemini（*GatewayService）的薄包装，委托给包级函数。
func (s *GatewayService) reportSuccessFromUsageLog(ctx context.Context, usageLog *UsageLog, account *Account, platform string, officialUSD float64) {
	reportSuccessFromUsageLog(s.cfg, ctx, usageLog, account, platform, officialUSD)
}

// reportSuccessFromUsageLogForOpenAI 是 OpenAI（*OpenAIGatewayService）的薄包装。
// 问题5：OpenAI success 路径此前完全没有 SubPilot 上报，现补齐。
func (s *OpenAIGatewayService) reportSuccessFromUsageLogForOpenAI(ctx context.Context, usageLog *UsageLog, account *Account, platform string) {
	reportSuccessFromUsageLog(s.cfg, ctx, usageLog, account, platform, usageLog.TotalCost)
}

// sanitizeSubPilotErrorMessage 截断并脱敏错误消息：限制长度，移除可能的 key/token 片段。
// 用于 report-failure 的 error_message，避免泄露凭据到 SubPilot。
func sanitizeSubPilotErrorMessage(msg string) string {
	if len(msg) > 200 {
		msg = msg[:200]
	}
	// 移除常见的 key/token 模式（保守处理，宁可误删也不泄露）。
	msg = redactKeyPatterns(msg)
	return msg
}

// redactKeyPatterns 移除疑似 API key / token 的子串。
func redactKeyPatterns(msg string) string {
	for _, pattern := range []string{"sk-", "Bearer ", "api_key=", "token=", "access_token="} {
		if idx := strings.Index(strings.ToLower(msg), strings.ToLower(pattern)); idx >= 0 {
			// 从模式出现处截断，保留前面的上下文，丢弃后面的可能凭据。
			msg = msg[:idx] + "[redacted]"
		}
	}
	return msg
}
