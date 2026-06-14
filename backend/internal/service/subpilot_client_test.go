package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// subpilotTestServer 用 httptest 模拟 SubPilot /select，按 mode 返回不同响应。
// mode 取值："ok" / "no_channel" / "500" / "slow" / "garbage" / "empty_account"。
func subpilotTestServer(t *testing.T, mode string, delay time.Duration) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		switch mode {
		case "ok":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"decision": "selected",
				"reason":   "highest_score",
				"account":  map[string]any{"id": "42", "name": "test-acct", "platform": "openai"},
				"lease":    map[string]any{"id": "lease_1", "ttl_ms": 60000, "account_id": "42"},
			})
		case "no_channel":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"decision": "no_channel",
				"reason":   "no_dispatchable_channel",
			})
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
		case "garbage":
			_, _ = w.Write([]byte("not json at all"))
		case "empty_account":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"decision": "selected",
				"account":  map[string]any{"id": ""},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestRecommendAccountDisabledReturnsNil(t *testing.T) {
	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{Enabled: false}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("disabled should return (nil, nil), got %+v err=%v", got, err)
	}
}

func TestRecommendAccountEmptyBaseURLReturnsNil(t *testing.T) {
	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{Enabled: true, BaseURL: ""}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("empty BaseURL should fail-open, got %+v err=%v", got, err)
	}
}

func TestRecommendAccountSuccessReturnsAccountID(t *testing.T) {
	srv := subpilotTestServer(t, "ok", 0)
	defer srv.Close()

	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: srv.URL, TimeoutMS: 1000,
	}, subpilotRecommendRequest{Platform: "openai", GroupID: "1", Model: "gpt-4.1"})
	if err != nil || got == nil {
		t.Fatalf("valid recommendation expected, got %+v err=%v", got, err)
	}
	if got.AccountID != 42 {
		t.Fatalf("AccountID = %d, want 42", got.AccountID)
	}
	if got.LeaseID != "lease_1" {
		t.Fatalf("LeaseID = %q, want lease_1", got.LeaseID)
	}
}

func TestRecommendAccountNoChannelFailOpen(t *testing.T) {
	srv := subpilotTestServer(t, "no_channel", 0)
	defer srv.Close()

	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: srv.URL, TimeoutMS: 1000,
	}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("no_channel should fail-open (nil,nil), got %+v err=%v", got, err)
	}
}

func TestRecommendAccountServerErrorFailOpen(t *testing.T) {
	srv := subpilotTestServer(t, "500", 0)
	defer srv.Close()

	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: srv.URL, TimeoutMS: 1000,
	}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("500 should fail-open, got %+v err=%v", got, err)
	}
}

func TestRecommendAccountGarbageFailOpen(t *testing.T) {
	srv := subpilotTestServer(t, "garbage", 0)
	defer srv.Close()

	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: srv.URL, TimeoutMS: 1000,
	}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("garbage response should fail-open, got %+v err=%v", got, err)
	}
}

func TestRecommendAccountEmptyAccountFailOpen(t *testing.T) {
	srv := subpilotTestServer(t, "empty_account", 0)
	defer srv.Close()

	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: srv.URL, TimeoutMS: 1000,
	}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("empty account id should fail-open, got %+v err=%v", got, err)
	}
}

// TestRecommendAccountTimeoutFailOpen 证明 SubPilot 超时时 fail-open，
// 绝不阻塞调用方。这是阶段4 最关键的验收点之一。
func TestRecommendAccountTimeoutFailOpen(t *testing.T) {
	// SubPilot 模拟 200ms 延迟，但客户端只给 50ms 超时。
	srv := subpilotTestServer(t, "ok", 200*time.Millisecond)
	defer srv.Close()

	client := NewSubPilotClient()
	start := time.Now()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: srv.URL, TimeoutMS: 50,
	}, subpilotRecommendRequest{})
	elapsed := time.Since(start)

	if err != nil || got != nil {
		t.Fatalf("timeout should fail-open (nil,nil), got %+v err=%v", got, err)
	}
	// 必须在超时时间内返回，不拖到 200ms。
	if elapsed > 300*time.Millisecond {
		t.Fatalf("fail-open took too long: %v (timeout was 50ms)", elapsed)
	}
}

// TestRecommendAccountConnectionRefusedFailOpen 证明 SubPilot 不可达时 fail-open。
func TestRecommendAccountConnectionRefusedFailOpen(t *testing.T) {
	client := NewSubPilotClient()
	got, err := client.RecommendAccount(context.Background(), config.SubPilotConfig{
		Enabled: true, BaseURL: "http://127.0.0.1:1", TimeoutMS: 100,
	}, subpilotRecommendRequest{})
	if err != nil || got != nil {
		t.Fatalf("connection refused should fail-open, got %+v err=%v", got, err)
	}
}

func TestSubPilotConfigDefaults(t *testing.T) {
	// disabled 时返回 Enabled=false 的零值。
	got := subPilotConfig(&config.Config{})
	if got.Enabled {
		t.Fatalf("default should be disabled, got %+v", got)
	}
	// enabled 但 TimeoutMS 未设时默认 80。
	got = subPilotConfig(&config.Config{Gateway: config.GatewayConfig{
		SubPilot: config.SubPilotConfig{Enabled: true, BaseURL: "http://x"},
	}})
	if got.TimeoutMS != 80 {
		t.Fatalf("default TimeoutMS = %d, want 80", got.TimeoutMS)
	}
	if !got.Enabled || got.BaseURL != "http://x" {
		t.Fatalf("enabled config not passed through: %+v", got)
	}
}

func TestPlatformForSubPilot(t *testing.T) {
	cases := map[string]string{
		"openai":    "openai",
		"OPENAI":    "openai",
		"anthropic": "anthropic",
		"claude":    "anthropic",
		"gemini":    "openai", // 非 anthropic/claude 默认 openai
	}
	for in, want := range cases {
		if got := platformForSubPilot(in); got != want {
			t.Errorf("platformForSubPilot(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseAccountID(t *testing.T) {
	id, err := parseAccountID("42")
	if err != nil || id != 42 {
		t.Fatalf("parseAccountID(42) = %d err=%v, want 42", id, err)
	}
	id, err = parseAccountID("abc")
	if err == nil || id != 0 {
		t.Fatalf("parseAccountID(abc) should fail, got %d err=%v", id, err)
	}
}
