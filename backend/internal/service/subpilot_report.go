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

func withSubPilotLeaseID(ctx context.Context, leaseID string) context.Context {
	if leaseID == "" {
		return ctx
	}
	return context.WithValue(ctx, subpilotLeaseCtxKey{}, leaseID)
}

func subPilotLeaseIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(subpilotLeaseCtxKey{}).(string); ok {
		return v
	}
	return ""
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

// reportFailureForOpenAI 是 OpenAIGatewayService 的失败 report 入口。
// 在 ReportOpenAIAccountScheduleResult(success=false) 时调用，向 SubPilot 上报
// 该账号本次请求失败。best-effort，任何错误都静默忽略。
// group_id / request_id 在此调用点不可用（调度结果回写签名很窄），
// SubPilot 用 account_id 即可更新该账号的健康/冷却状态。
func (s *OpenAIGatewayService) reportFailureForOpenAI(accountID int64) {
	sp := subPilotConfig(s.cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	subPilotClientSingleton.reportFailure(reportCtx, sp, subpilotReportFailureRequest{
		RequestID:    "openai-schedule-" + strconv.FormatInt(accountID, 10) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		AccountID:    strconv.FormatInt(accountID, 10),
		Platform:     platformForSubPilot(PlatformOpenAI),
		ErrorCode:    "upstream_error",
		ErrorMessage: "openai account schedule reported failure",
	})
}

// reportSuccessFromUsageLog 从 recordUsageCore 已构建好的 usageLog + result + account
// 组装 SubPilot success report。best-effort，任何字段缺失都不报错。
func (s *GatewayService) reportSuccessFromUsageLog(ctx context.Context, usageLog *UsageLog, account *Account, platform string, officialUSD float64) {
	if usageLog == nil || account == nil {
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
	// 阶段5 按要求暂不上报 first_token_ms（留待阶段7 单独验证 SSE 完整性后再开启）。
	// Sub2API 已在 usageLog.FirstTokenMs 里自算了首字时间，阶段7 只需取消下面注释即可启用：
	// if usageLog.FirstTokenMs != nil && *usageLog.FirstTokenMs > 0 {
	//     req.FirstTokenMS = *usageLog.FirstTokenMs
	// }
	s.reportSuccessToSubPilot(ctx, req)
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
