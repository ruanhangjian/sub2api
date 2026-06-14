package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// subpilotRecommendRequest 对应 SubPilot /v1/dispatch/select 的请求体。
type subpilotRecommendRequest struct {
	RequestID           string `json:"request_id"`
	Platform            string `json:"platform"`
	GroupID             string `json:"group_id"`
	Model               string `json:"model"`
	SessionKey          string `json:"session_key,omitempty"`
	MaxAcceptableTTFTMS int    `json:"max_acceptable_ttft_ms,omitempty"`
}

// subpilotRecommendResponse 对应 SubPilot /v1/dispatch/select 的响应体。
type subpilotRecommendResponse struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
	GroupID  string `json:"group_id,omitempty"`
	Account  struct {
		ID           string `json:"id"`
		Name         string `json:"name,omitempty"`
		Platform     string `json:"platform,omitempty"`
		HealthStatus string `json:"health_status,omitempty"`
	} `json:"account,omitempty"`
	Lease struct {
		ID        string `json:"id,omitempty"`
		TTLMS     int64  `json:"ttl_ms,omitempty"`
		AccountID string `json:"account_id,omitempty"`
	} `json:"lease,omitempty"`
}

// SubPilotClient 封装对 SubPilot sidecar 的 HTTP 调用。
// 它是无状态的（配置在调用时传入），可被 GatewayService 和 OpenAIGatewayService 共享。
type SubPilotClient struct {
	httpClient *http.Client
}

// NewSubPilotClient 构造一个 SubPilot 客户端，HTTP 超时由调用方控制（通过 ctx）。
func NewSubPilotClient() *SubPilotClient {
	return &SubPilotClient{
		httpClient: &http.Client{
			// Transport 用默认；单次请求超时由调用方 ctx 控制，这里不设客户端级超时，
			// 避免与 ctx 超时叠加产生歧义。
		},
	}
}

// RecommendResult 是 SubPilot 推荐成功时的输出。
type RecommendResult struct {
	AccountID int64
	LeaseID   string
}

// RecommendAccount 调用 SubPilot /v1/dispatch/select 获取推荐账号。
// 任何错误（disabled / 超时 / 网络错误 / 非法响应 / SubPilot 返回 no_channel）
// 都返回 (0, nil) 即 fail-open，由调用方走原生调度。
//
// 重要：本函数只负责"问 SubPilot 拿推荐"，不做任何账号校验。
// 账号的 group/platform/model/status/concurrency 二次校验由调用方负责。
func (c *SubPilotClient) RecommendAccount(ctx context.Context, cfg config.SubPilotConfig, req subpilotRecommendRequest) (*RecommendResult, error) {
	if !cfg.Enabled || cfg.BaseURL == "" {
		return nil, nil // disabled 或未配置 → fail-open
	}

	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 80 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	body, err := json.Marshal(req)
	if err != nil {
		return nil, nil // 序列化失败 → fail-open
	}

	url := strings.TrimRight(cfg.BaseURL, "/") + "/v1/dispatch/select"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, nil // 超时 / 连接错误 → fail-open
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil // SubPilot 返回非 2xx → fail-open
	}

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, nil
	}

	var result subpilotRecommendResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, nil // 响应解析失败 → fail-open
	}

	if result.Decision != "selected" || result.Account.ID == "" {
		return nil, nil // SubPilot 没选中（no_channel / 冷却等）→ fail-open
	}

	accountID, err := parseAccountID(result.Account.ID)
	if err != nil || accountID <= 0 {
		return nil, nil // 账号 ID 非法 → fail-open
	}

	return &RecommendResult{
		AccountID: accountID,
		LeaseID:   result.Lease.ID,
	}, nil
}

// subpilotConfigForService 从 GatewayService 配置里读 SubPilot 配置。
// 在 disabled 时返回零值，调用方据此短路。
func subpilotConfigForService(cfg *config.Config) config.SubPilotConfig {
	if cfg == nil {
		return config.SubPilotConfig{}
	}
	return cfg.Gateway.SubPilot
}

// platformForSubPilot 把 Sub2API 内部平台名映射为 SubPilot 期望的平台名。
// SubPilot 约定 "openai" / "anthropic"。
func platformForSubPilot(platform string) string {
	switch strings.ToLower(platform) {
	case "anthropic", "claude":
		return "anthropic"
	default:
		return "openai"
	}
}

// parseAccountID 把 SubPilot 返回的字符串 account ID 解析为 int64。
// Sub2API 的 account ID 是 int64，SubPilot 透传的是字符串形式。
func parseAccountID(s string) (int64, error) {
	var id int64
	if _, err := fmt.Sscanf(s, "%d", &id); err != nil {
		return 0, err
	}
	return id, nil
}
