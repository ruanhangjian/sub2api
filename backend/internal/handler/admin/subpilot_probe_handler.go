package admin

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

// subPilotProbeResponse 是内部 probe endpoint 返回给 SubPilot 的结构化结果。
type subPilotProbeResponse struct {
	Success      bool   `json:"success"`
	AccountID    int64  `json:"account_id"`
	LatencyMS    int64  `json:"latency_ms"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// SubPilotProbe 是 SubPilot 专用的内部账号探测端点。
//
// POST /api/v1/internal/subpilot/probe/:id
//
// 它复用 AccountTestService.TestAccountConnection（含 Sub2API 完整的 token 刷新、
// headers、平台分发逻辑），通过 httptest 捕获其 SSE 输出的 HTTP 状态码来判断
// 探测是否成功。SubPilot 用此端点探测 ChatGPT/OAuth 账号，避免自己硬模拟。
//
// 鉴权：依赖 docker 内网隔离（SubPilot 与 Sub2API 同 network），不经过 admin JWT。
// 探测用低 token（model 可选，prompt 固定为短文本）。
func (h *AccountHandler) SubPilotProbe(c *gin.Context) {
	if h.accountTestService == nil {
		response.InternalError(c, "account test service unavailable")
		return
	}

	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid account ID")
		return
	}

	var req struct {
		ModelID string `json:"model_id"`
		Prompt  string `json:"prompt"`
		Mode    string `json:"mode"`
	}
	_ = c.ShouldBindJSON(&req)
	if req.Prompt == "" {
		req.Prompt = "ping" // 最小输入，节省 token
	}

	// 用 httptest 捕获 TestAccountConnection 的 SSE 输出。
	// 我们只关心 HTTP 状态码：TestAccountConnection 成功时写 200 + SSE 数据，
	// 失败时写错误状态码或 sendErrorAndEnd。这里通过捕获 recorder 判断。
	recorder := httptest.NewRecorder()
	mockCtx, _ := gin.CreateTestContext(recorder)
	mockCtx.Request = c.Request.Clone(c.Request.Context())
	if mockCtx.Request.Body == nil {
		mockCtx.Request.Body = http.NoBody
	}
	// 复制 path param
	mockCtx.Params = c.Params

	start := time.Now()
	testErr := h.accountTestService.TestAccountConnection(mockCtx, accountID, req.ModelID, req.Prompt, req.Mode)
	latency := time.Since(start).Milliseconds()

	result := subPilotProbeResponse{
		AccountID: accountID,
		LatencyMS: latency,
	}

	// 判断成功：TestAccountConnection 成功时 recorder 状态码为 200 且有 body 输出。
	body := recorder.Body.Bytes()
	if testErr == nil && recorder.Code >= 200 && recorder.Code < 300 && len(bytes.TrimSpace(body)) > 0 {
		result.Success = true
	} else {
		result.Success = false
		// 从 SSE 输出里提取错误摘要（保守处理，截断 + 脱敏）。
		result.ErrorMessage = subPilotProbeExtractError(body)
		if result.ErrorMessage == "" && testErr != nil {
			result.ErrorMessage = testErr.Error()
		}
		if len(result.ErrorMessage) > 200 {
			result.ErrorMessage = result.ErrorMessage[:200]
		}
	}

	response.Success(c, result)
}

// subPilotProbeExtractError 从 SSE 输出里保守提取错误信息。
// SSE 格式为 data: {...}\n\n，我们找含 "error" 的行。
func subPilotProbeExtractError(body []byte) string {
	text := string(body)
	if len(text) > 500 {
		text = text[:500]
	}
	// 简单查找 error/type 字样，避免完整 JSON 解析的复杂度。
	for _, marker := range []string{`"error"`, `"message"`, `"type"`} {
		if idx := indexOf(text, marker); idx >= 0 {
			rest := text[idx:]
			if len(rest) > 200 {
				rest = rest[:200]
			}
			return rest
		}
	}
	return ""
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
