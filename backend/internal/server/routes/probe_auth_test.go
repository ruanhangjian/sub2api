package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
)

// newProbeTestRouter 构造一个仅含 probe 中间件 + 一个 ok handler 的测试路由。
func newProbeTestRouter(cfg *config.Config) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	g := r.Group("/internal/subpilot")
	g.Use(subPilotSecretMiddleware(cfg))
	g.POST("/probe/:id", func(c *gin.Context) { c.Status(http.StatusOK) })
	g.GET("/account-groups/:group_id/accounts/:account_id", func(c *gin.Context) { c.Status(http.StatusOK) })
	g.PUT("/account-groups/:group_id/accounts/:account_id", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func TestSubPilotSecretProtectsGroupAccountControlEndpoints(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{ProbeSecret: "s3cret"}}}
	r := newProbeTestRouter(cfg)

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/internal/subpilot/account-groups/2/accounts/1", nil)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("%s without shared secret should return 401, got %d", method, w.Code)
		}

		w = httptest.NewRecorder()
		req = httptest.NewRequest(method, "/internal/subpilot/account-groups/2/accounts/1", nil)
		req.Header.Set("X-SubPilot-Secret", "s3cret")
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s with shared secret should pass, got %d", method, w.Code)
		}
	}
}

// TestProbeSecretRejectsWhenUnconfigured 证明问题6 安全要求：
// 未配置 probe_secret 时默认拒绝所有 probe 请求（401）。
func TestProbeSecretRejectsWhenUnconfigured(t *testing.T) {
	cfg := &config.Config{} // ProbeSecret 为空
	r := newProbeTestRouter(cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/subpilot/probe/1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unconfigured secret should reject with 401, got %d", w.Code)
	}
}

// TestProbeSecretRejectsMissingHeader 证明配置了 secret 但请求未带 header 时返回 401。
func TestProbeSecretRejectsMissingHeader(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{ProbeSecret: "s3cret"}}}
	r := newProbeTestRouter(cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/subpilot/probe/1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing header should reject with 401, got %d", w.Code)
	}
}

// TestProbeSecretRejectsWrongHeader 证明 header 不匹配时返回 401。
func TestProbeSecretRejectsWrongHeader(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{ProbeSecret: "s3cret"}}}
	r := newProbeTestRouter(cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/subpilot/probe/1", nil)
	req.Header.Set("X-SubPilot-Secret", "wrong")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong header should reject with 401, got %d", w.Code)
	}
}

// TestProbeSecretAcceptsCorrectHeader 证明 header 正确时放行（200）。
func TestProbeSecretAcceptsCorrectHeader(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{SubPilot: config.SubPilotConfig{ProbeSecret: "s3cret"}}}
	r := newProbeTestRouter(cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/subpilot/probe/1", nil)
	req.Header.Set("X-SubPilot-Secret", "s3cret")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("correct header should pass with 200, got %d", w.Code)
	}
}
