package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// reportCaptureServer 记录收到的最后一次请求方法和 body，用于验证 report 上报内容。
type reportCaptureServer struct {
	*httptest.Server
	lastMethod string
	lastPath   string
	lastBody   map[string]any
}

func newReportCaptureServer(t *testing.T) *reportCaptureServer {
	t.Helper()
	capture := &reportCaptureServer{}
	capture.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.lastMethod = r.Method
		capture.lastPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = json.Unmarshal(body, &parsed)
		capture.lastBody = parsed
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(capture.Server.Close)
	return capture
}

func TestReportSuccessPostsToSubPilot(t *testing.T) {
	srv := newReportCaptureServer(t)
	client := NewSubPilotClient()
	cfg := config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}

	client.reportSuccess(context.Background(), cfg, subpilotReportSuccessRequest{
		RequestID:    "req-1",
		AccountID:    "42",
		GroupID:      "6",
		Platform:     "openai",
		Model:        "gpt-4.1",
		LatencyMS:    840,
		FirstTokenMS: 520,
	})

	if srv.lastPath != "/v1/dispatch/report-success" {
		t.Fatalf("path = %q, want /v1/dispatch/report-success", srv.lastPath)
	}
	if srv.lastBody["request_id"] != "req-1" || srv.lastBody["account_id"] != "42" || srv.lastBody["latency_ms"] != float64(840) {
		t.Fatalf("unexpected body: %+v", srv.lastBody)
	}
	// 阶段7：first_token_ms 现在上报（取自 Sub2API 已自算的 usageLog.FirstTokenMs）。
	if srv.lastBody["first_token_ms"] != float64(520) {
		t.Fatalf("first_token_ms = %v, want 520: %+v", srv.lastBody["first_token_ms"], srv.lastBody)
	}
}

func TestReportFailurePostsToSubPilot(t *testing.T) {
	srv := newReportCaptureServer(t)
	client := NewSubPilotClient()
	cfg := config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}

	client.reportFailure(context.Background(), cfg, subpilotReportFailureRequest{
		RequestID:    "req-2",
		AccountID:    "42",
		StatusCode:   429,
		ErrorCode:    "rate_limited",
		ErrorMessage: "rate limited",
	})

	if srv.lastPath != "/v1/dispatch/report-failure" {
		t.Fatalf("path = %q, want /v1/dispatch/report-failure", srv.lastPath)
	}
	if srv.lastBody["status_code"] != float64(429) || srv.lastBody["error_code"] != "rate_limited" {
		t.Fatalf("unexpected body: %+v", srv.lastBody)
	}
}

func TestReportDisabledDoesNothing(t *testing.T) {
	srv := newReportCaptureServer(t)
	client := NewSubPilotClient()
	// disabled 模式不应发任何请求
	client.reportSuccess(context.Background(), config.SubPilotConfig{Enabled: false}, subpilotReportSuccessRequest{})
	if srv.lastMethod != "" {
		t.Fatalf("disabled report should not send request, got %s %s", srv.lastMethod, srv.lastPath)
	}
}

// TestReportFailureDoesNotBlock 证明 report 失败（如 SubPilot 不可达）不会阻塞或报错。
func TestReportFailureDoesNotBlock(t *testing.T) {
	client := NewSubPilotClient()
	// 指向一个不可达的端口，模拟 SubPilot 挂掉
	cfg := config.SubPilotConfig{Enabled: true, BaseURL: "http://127.0.0.1:1", TimeoutMS: 100}
	start := time.Now()
	client.reportFailure(context.Background(), cfg, subpilotReportFailureRequest{RequestID: "r", AccountID: "1"})
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("failed report should not block long: %v", elapsed)
	}
}

// TestReportSuccessDoesNotBlock 同理验证 success report 不可达时不阻塞。
func TestReportSuccessDoesNotBlock(t *testing.T) {
	client := NewSubPilotClient()
	cfg := config.SubPilotConfig{Enabled: true, BaseURL: "http://127.0.0.1:1", TimeoutMS: 100}
	start := time.Now()
	client.reportSuccess(context.Background(), cfg, subpilotReportSuccessRequest{RequestID: "r", AccountID: "1"})
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("failed report should not block long: %v", elapsed)
	}
}

// TestSanitizeSubPilotErrorMessageRedactsKeys 证明错误消息里的 key/token 被脱敏。
func TestSanitizeSubPilotErrorMessageRedactsKeys(t *testing.T) {
	cases := map[string]string{
		"auth failed: sk-abc123secret":           "auth failed: [redacted]",
		"Bearer eyJhbG token here":               "[redacted]",
		"error: api_key=secret_value":            "error: [redacted]",
		"normal error without keys":              "normal error without keys",
	}
	for in, want := range cases {
		got := sanitizeSubPilotErrorMessage(in)
		if got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeSubPilotErrorMessageTruncates(t *testing.T) {
	long := strings.Repeat("x", 300)
	got := sanitizeSubPilotErrorMessage(long)
	if len(got) > 200 {
		t.Fatalf("message not truncated: len=%d", len(got))
	}
}

func TestReportBodyContainsNoCredentials(t *testing.T) {
	srv := newReportCaptureServer(t)
	client := NewSubPilotClient()
	cfg := config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}

	// 即使传入含敏感信息的消息，report body 也不应泄露
	client.reportFailure(context.Background(), cfg, subpilotReportFailureRequest{
		RequestID:    "req-3",
		AccountID:    "42",
		ErrorMessage: sanitizeSubPilotErrorMessage("error with sk-secret-key inside"),
	})
	bodyStr, _ := json.Marshal(srv.lastBody)
	if strings.Contains(string(bodyStr), "sk-secret-key") {
		t.Fatalf("report body leaked credential: %s", bodyStr)
	}
	if !strings.Contains(string(bodyStr), "[redacted]") {
		t.Fatalf("report body should contain redacted marker: %s", bodyStr)
	}
}

// TestReportSuccessCarriesLeaseIDFromContext 证明问题1：lease_id 从 ctx 流入 success report body。
// handler 在 select 后用 WithSubPilotLeaseID 写入 ctx，reportSuccessFromUsageLog 从 ctx 读出。
func TestReportSuccessCarriesLeaseIDFromContext(t *testing.T) {
	srv := newReportCaptureServer(t)
	subPilotClientSingleton = NewSubPilotClient() // 重置单例
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}}}

	const leaseID = "lease_test_42_1700000000"
	ctx := WithSubPilotLeaseID(context.Background(), leaseID)

	groupID := int64(6)
	usageLog := &UsageLog{
		RequestID:    "req-lease-1",
		GroupID:      &groupID,
		Model:        "gpt-4.1",
		TotalCost:    0.0123,
	}
	account := &Account{ID: 42}

	reportSuccessFromUsageLog(cfg, ctx, usageLog, account, PlatformOpenAI, usageLog.TotalCost)

	if srv.lastBody["lease_id"] != leaseID {
		t.Fatalf("lease_id = %v, want %q: %+v", srv.lastBody["lease_id"], leaseID, srv.lastBody)
	}
	// 问题7：official_usd_used 应为 TotalCost（非 0）。
	if srv.lastBody["official_usd_used"] != 0.0123 {
		t.Fatalf("official_usd_used = %v, want 0.0123", srv.lastBody["official_usd_used"])
	}
}

// TestReportSuccessOpenAIIncludesFirstTokenMs 证明问题5：OpenAI success report 含 first_token_ms。
func TestReportSuccessOpenAIIncludesFirstTokenMs(t *testing.T) {
	srv := newReportCaptureServer(t)
	subPilotClientSingleton = NewSubPilotClient()
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}}}

	groupID := int64(1)
	ttft := 480
	usageLog := &UsageLog{
		RequestID:    "req-ttft-1",
		GroupID:      &groupID,
		Model:        "gpt-4o",
		TotalCost:    0.005,
		FirstTokenMs: &ttft,
	}
	account := &Account{ID: 7}

	reportSuccessFromUsageLog(cfg, context.Background(), usageLog, account, PlatformOpenAI, usageLog.TotalCost)

	if srv.lastBody["first_token_ms"] != float64(480) {
		t.Fatalf("first_token_ms = %v, want 480: %+v", srv.lastBody["first_token_ms"], srv.lastBody)
	}
}

// TestOpenAIFailureReportSkipsWithoutLease 证明问题5 分级处理：
// 没有 lease_id（非 SubPilot 选的请求）时跳过 report，不假装闭环。
func TestOpenAIFailureReportSkipsWithoutLease(t *testing.T) {
	srv := newReportCaptureServer(t)
	subPilotClientSingleton = NewSubPilotClient()
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}}}

	svc := &OpenAIGatewayService{cfg: cfg}
	groupID := int64(2)
	// ctx 不含 lease_id → 应跳过。
	svc.reportFailureForOpenAI(99, SubPilotFailContext{
		Ctx:     context.Background(),
		GroupID: &groupID,
		Model:   "gpt-4.1",
	})

	if srv.lastMethod != "" {
		t.Fatalf("should skip report without lease_id, got %s %s", srv.lastMethod, srv.lastPath)
	}
}

// TestOpenAIFailureReportSendsWithLease 证明问题5：有 lease_id 时失败 report 携带 group_id/model/lease_id。
func TestOpenAIFailureReportSendsWithLease(t *testing.T) {
	srv := newReportCaptureServer(t)
	subPilotClientSingleton = NewSubPilotClient()
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{Enabled: true, BaseURL: srv.URL, TimeoutMS: 500}}}

	svc := &OpenAIGatewayService{cfg: cfg}
	const leaseID = "lease_fail_99"
	groupID := int64(5)
	ctx := WithSubPilotLeaseID(context.Background(), leaseID)

	svc.reportFailureForOpenAI(99, SubPilotFailContext{
		Ctx:     ctx,
		GroupID: &groupID,
		Model:   "gpt-4o",
	})

	if srv.lastPath != "/v1/dispatch/report-failure" {
		t.Fatalf("path = %q, want report-failure", srv.lastPath)
	}
	if srv.lastBody["lease_id"] != leaseID {
		t.Fatalf("lease_id = %v, want %q", srv.lastBody["lease_id"], leaseID)
	}
	if srv.lastBody["group_id"] != "5" {
		t.Fatalf("group_id = %v, want 5", srv.lastBody["group_id"])
	}
	if srv.lastBody["model"] != "gpt-4o" {
		t.Fatalf("model = %v, want gpt-4o", srv.lastBody["model"])
	}
}
