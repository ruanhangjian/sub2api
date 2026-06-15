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
	})

	if srv.lastPath != "/v1/dispatch/report-success" {
		t.Fatalf("path = %q, want /v1/dispatch/report-success", srv.lastPath)
	}
	if srv.lastBody["request_id"] != "req-1" || srv.lastBody["account_id"] != "42" || srv.lastBody["latency_ms"] != float64(840) {
		t.Fatalf("unexpected body: %+v", srv.lastBody)
	}
	// 阶段5 不上报 first_token_ms（留待阶段7），确认 body 里不含该字段。
	if _, has := srv.lastBody["first_token_ms"]; has {
		t.Fatalf("stage5 should NOT report first_token_ms, got: %+v", srv.lastBody)
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
