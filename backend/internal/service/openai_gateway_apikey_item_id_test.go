//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIGatewayService_APIKeyPassthrough_StripsInvalidInputItemIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"resp_test","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`,
		)),
	}}
	svc := newOpenAIImageGenerationControlTestService(upstream)
	c, _ := newOpenAIImageGenerationControlTestContext(true, "codex_cli_rs/0.144.1")
	account := newOpenAIImageGenerationControlTestAccount()
	account.Extra = map[string]any{"openai_passthrough": true}

	body := []byte(`{
		"model":"gpt-5.6-sol",
		"stream":false,
		"input":[
			{"type":"message","id":"item_bad_message","role":"assistant","content":[{"type":"output_text","text":"hello"}]},
			{"type":"function_call","id":"item_bad_call","call_id":"call_123","name":"exec_command","arguments":"{}"},
			{"type":"message","id":"msg_valid","role":"user","content":[{"type":"input_text","text":"continue"}]},
			{"type":"function_call","id":"fc_valid","call_id":"call_456","name":"apply_patch","arguments":"{}"},
			{"type":"function_call_output","id":"item_output","call_id":"call_123","output":"done"},
			{"type":"web_search_call","id":"item_unconstrained"},
			{"type":"reasoning","id":"item_bad_reasoning","encrypted_content":"enc","summary":[],"content":[{"type":"reasoning_text","text":"kept"}],"status":"completed","extra":"field"},
			{"type":"reasoning","id":"rs_valid","encrypted_content":"enc2","summary":[]},
			{"type":"reasoning","encrypted_content":"enc3","summary":[]}
		]
	}`)

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, upstream.lastReq)

	forwarded := upstream.lastBody
	require.False(t, gjson.GetBytes(forwarded, "input.0.id").Exists())
	require.Equal(t, "hello", gjson.GetBytes(forwarded, "input.0.content.0.text").String())
	require.False(t, gjson.GetBytes(forwarded, "input.1.id").Exists())
	require.Equal(t, "call_123", gjson.GetBytes(forwarded, "input.1.call_id").String())
	require.Equal(t, "exec_command", gjson.GetBytes(forwarded, "input.1.name").String())
	require.Equal(t, "{}", gjson.GetBytes(forwarded, "input.1.arguments").String())
	require.Equal(t, "msg_valid", gjson.GetBytes(forwarded, "input.2.id").String())
	require.Equal(t, "fc_valid", gjson.GetBytes(forwarded, "input.3.id").String())
	require.Equal(t, "item_output", gjson.GetBytes(forwarded, "input.4.id").String())
	require.Equal(t, "call_123", gjson.GetBytes(forwarded, "input.4.call_id").String())
	require.Equal(t, "item_unconstrained", gjson.GetBytes(forwarded, "input.5.id").String())
	require.False(t, gjson.GetBytes(forwarded, "input.6.id").Exists())
	require.Equal(t, "reasoning", gjson.GetBytes(forwarded, "input.6.type").String())
	require.Equal(t, "enc", gjson.GetBytes(forwarded, "input.6.encrypted_content").String())
	require.True(t, gjson.GetBytes(forwarded, "input.6.summary").IsArray())
	require.Equal(t, "kept", gjson.GetBytes(forwarded, "input.6.content.0.text").String())
	require.Equal(t, "completed", gjson.GetBytes(forwarded, "input.6.status").String())
	require.Equal(t, "field", gjson.GetBytes(forwarded, "input.6.extra").String())
	require.Equal(t, "rs_valid", gjson.GetBytes(forwarded, "input.7.id").String())
	require.False(t, gjson.GetBytes(forwarded, "input.8.id").Exists())
	require.Equal(t, "enc3", gjson.GetBytes(forwarded, "input.8.encrypted_content").String())
}

func TestSanitizeOpenAIResponsesInputItemIDs_ReasoningIDRules(t *testing.T) {
	for _, stream := range []bool{false, true} {
		stream := stream
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{
				"model":"gpt-5.6-sol",
				"stream":%v,
				"input":[
					{"type":"reasoning","id":"item_bad","encrypted_content":"xxx","summary":[],"content":[{"type":"reasoning_text","text":"kept"}],"status":"completed","extra":"field"},
					{"type":"reasoning","id":"rs_123","encrypted_content":"yyy","summary":[]},
					{"type":"reasoning","encrypted_content":"zzz","summary":[]},
					{"type":"message","id":"item_message","role":"assistant","content":[{"type":"output_text","text":"hello"}]},
					{"type":"function_call","id":"item_call","call_id":"call_1","name":"tool","arguments":"{}"},
					{"type":"custom_tool_call","id":"item_custom","call_id":"call_2","name":"custom","input":"{}"}
				]
			}`, stream))

			sanitized, changed, err := sanitizeOpenAIResponsesInputItemIDs(body)
			require.NoError(t, err)
			require.True(t, changed)

			require.Equal(t, stream, gjson.GetBytes(sanitized, "stream").Bool())
			require.False(t, gjson.GetBytes(sanitized, "input.0.id").Exists())
			require.Equal(t, "reasoning", gjson.GetBytes(sanitized, "input.0.type").String())
			require.Equal(t, "xxx", gjson.GetBytes(sanitized, "input.0.encrypted_content").String())
			require.True(t, gjson.GetBytes(sanitized, "input.0.summary").IsArray())
			require.Equal(t, "kept", gjson.GetBytes(sanitized, "input.0.content.0.text").String())
			require.Equal(t, "completed", gjson.GetBytes(sanitized, "input.0.status").String())
			require.Equal(t, "field", gjson.GetBytes(sanitized, "input.0.extra").String())

			require.Equal(t, "rs_123", gjson.GetBytes(sanitized, "input.1.id").String())
			require.False(t, gjson.GetBytes(sanitized, "input.2.id").Exists())
			require.False(t, gjson.GetBytes(sanitized, "input.3.id").Exists())
			require.False(t, gjson.GetBytes(sanitized, "input.4.id").Exists())
			require.False(t, gjson.GetBytes(sanitized, "input.5.id").Exists())
			require.Equal(t, "call_2", gjson.GetBytes(sanitized, "input.5.call_id").String())
		})
	}
}

func TestSanitizeOpenAIResponsesInputItemIDs_CompactKeepsReasoningIDs(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.6-sol",
		"input":[
			{"type":"reasoning","id":"item_bad","encrypted_content":"xxx","summary":[]},
			{"type":"message","id":"item_message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}
		]
	}`)

	sanitized, changed, err := sanitizeOpenAIResponsesInputItemIDs(body, false)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "item_bad", gjson.GetBytes(sanitized, "input.0.id").String())
	require.False(t, gjson.GetBytes(sanitized, "input.1.id").Exists())
}

func TestSanitizeOpenAIResponsesInputItemIDs_AllocationGrowthIsLinear(t *testing.T) {
	makeBody := func(itemCount int) []byte {
		items := make([]string, itemCount)
		for i := range items {
			items[i] = fmt.Sprintf(`{"type":"message","id":"item_%d","role":"user","content":[{"type":"input_text","text":"hello"}]}`, i)
		}
		return []byte(`{"model":"gpt-5.6-sol","input":[` + strings.Join(items, ",") + `]}`)
	}
	allocatedBytes := func(body []byte) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		sanitized, changed, err := sanitizeOpenAIResponsesInputItemIDs(body)
		runtime.ReadMemStats(&after)
		require.NoError(t, err)
		require.True(t, changed)
		require.NotEmpty(t, sanitized)
		return after.TotalAlloc - before.TotalAlloc
	}

	smallAllocated := allocatedBytes(makeBody(20))
	largeAllocated := allocatedBytes(makeBody(200))
	require.Less(t, largeAllocated, smallAllocated*30,
		"10x more input items must not cause quadratic whole-body allocation growth")
}
