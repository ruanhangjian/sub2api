package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type AsyncImageHandler struct {
	tasks   *service.ImageTaskService
	openAI  *OpenAIGatewayHandler
	execute func(platform string, c *gin.Context)
	apiKeys *service.APIKeyService
	lookup  func(context.Context, int64) (*service.APIKey, error)
	cfg     *config.Config

	runtimeOnce sync.Once
	engineMu    sync.RWMutex
	engine      *gin.Engine
}

func NewAsyncImageHandler(tasks *service.ImageTaskService, openAI *OpenAIGatewayHandler) *AsyncImageHandler {
	h := &AsyncImageHandler{tasks: tasks, openAI: openAI}
	h.execute = h.executeWithGateway
	return h
}

func ProvideAsyncImageHandler(tasks *service.ImageTaskService, openAI *OpenAIGatewayHandler, apiKeys *service.APIKeyService, cfg *config.Config) *AsyncImageHandler {
	h := NewAsyncImageHandler(tasks, openAI)
	h.apiKeys = apiKeys
	if apiKeys != nil {
		h.lookup = apiKeys.GetByID
	}
	h.cfg = cfg
	return h
}

// AttachEngine is called by server wiring after all routes and middleware have
// been registered. Workers execute through this engine so recovered jobs use
// the same auth, routing, SubPilot and billing path as synchronous requests.
func (h *AsyncImageHandler) AttachEngine(engine *gin.Engine) {
	if h == nil || engine == nil {
		return
	}
	h.engineMu.Lock()
	h.engine = engine
	h.engineMu.Unlock()
	if h.cfg == nil || !h.cfg.AsyncImage.Enabled {
		return
	}
	h.runtimeOnce.Do(func() { go h.runRuntime(context.Background()) })
}

// enabled reports whether the async image task feature and result storage are available.
func (h *AsyncImageHandler) enabled() bool {
	return h != nil && h.tasks != nil && (h.cfg == nil || h.cfg.AsyncImage.Enabled) && h.tasks.Enabled()
}

// pollable reports whether task lookups can be served. It is deliberately weaker
// than enabled(): results already written to Redis stay readable after the
// feature is switched off, so an in-flight task is never stranded.
func (h *AsyncImageHandler) pollable() bool {
	return h != nil && h.tasks != nil && h.tasks.Pollable()
}

// Submit accepts the same payload as the synchronous Images endpoint and
// returns before the upstream image generation begins.
func (h *AsyncImageHandler) Submit(c *gin.Context) {
	if !h.enabled() {
		imageTaskJSONError(c, http.StatusNotFound, "not_found_error", "async image tasks are not enabled")
		return
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.UserID <= 0 || apiKey.ID <= 0 {
		imageTaskError(c, service.ErrImageTaskForbidden)
		return
	}
	platform := ""
	if apiKey.Group != nil {
		platform = apiKey.Group.Platform
	}
	if platform != service.PlatformOpenAI && platform != service.PlatformGrok && platform != service.PlatformGemini {
		imageTaskJSONError(c, http.StatusNotFound, "not_found_error", "Images API is not supported for this platform")
		return
	}
	if !service.GroupAllowsImageGeneration(apiKey.Group) {
		imageTaskJSONError(c, http.StatusForbidden, "permission_error", service.ImageGenerationPermissionMessage())
		return
	}
	if h == nil || h.tasks == nil {
		imageTaskError(c, service.ErrImageTaskUnavailable)
		return
	}

	body, err := pkghttputil.ReadRequestBodyWithPrealloc(c.Request)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			imageTaskJSONError(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		imageTaskJSONError(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}
	if len(body) == 0 {
		imageTaskJSONError(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}
	if asyncImageRequestStreams(c.GetHeader("Content-Type"), body) {
		imageTaskJSONError(c, http.StatusBadRequest, "invalid_request_error", "streaming image requests cannot be submitted as asynchronous tasks")
		return
	}
	submission, err := h.parseSubmission(c, platform, body)
	if err != nil {
		imageTaskJSONError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if !h.checkSecurityAuditBeforeSubmit(c, apiKey, platform, submission.Model, body) {
		return
	}

	holdAmount := h.tasks.EstimateHold(c.Request.Context(), apiKey, submission.Model, submission.Size, submission.Count)
	var task *service.ImageTask
	created := true
	var legacyCtx *gin.Context
	var legacyRecorder *httptest.ResponseRecorder
	var legacyCancel context.CancelFunc
	if h.tasks.Durable() {
		task, created, err = h.tasks.Submit(c.Request.Context(), service.ImageTaskSubmitInput{
			Owner:    service.ImageTaskOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID},
			Platform: platform, Target: submission.Target, Model: submission.Model,
			Method: http.MethodPost, RequestPath: submission.Path,
			ContentType: c.GetHeader("Content-Type"), Headers: asyncImageHeaders(c),
			Body: submission.Body, IdempotencyKey: c.GetHeader("Idempotency-Key"), HoldAmount: holdAmount,
		})
	} else {
		// Production wiring always uses the durable queue above. This path keeps
		// isolated handler tests and cache-only embedders backward compatible.
		legacyCtx, legacyRecorder, legacyCancel = newAsyncImageContext(c, body, h.tasks.ExecutionTimeout())
		task, err = h.tasks.Create(c.Request.Context(), service.ImageTaskOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID})
	}
	if err != nil {
		if legacyCancel != nil {
			legacyCancel()
		}
		imageTaskError(c, err)
		return
	}

	pollURL := imageTaskPollURL(c.Request.URL.Path, task.ID)
	c.Header("Cache-Control", "no-store")
	c.Header("Location", pollURL)
	c.Header("Retry-After", "3")
	if !created {
		c.Header("Idempotent-Replayed", "true")
	}
	c.JSON(http.StatusAccepted, gin.H{
		"id":         task.ID,
		"task_id":    task.TaskID,
		"object":     task.Object,
		"status":     task.Status,
		"created_at": task.CreatedAt,
		"expires_at": task.ExpiresAt,
		"poll_url":   pollURL,
	})
	if legacyCtx != nil {
		go h.run(task.ID, platform, legacyCtx, legacyRecorder, legacyCancel)
	}
}

func (h *AsyncImageHandler) checkSecurityAuditBeforeSubmit(c *gin.Context, apiKey *service.APIKey, platform, model string, body []byte) bool {
	if h == nil || h.openAI == nil {
		return true
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		imageTaskJSONError(c, http.StatusInternalServerError, "api_error", "User context not found")
		return false
	}
	moderationBody := body
	if platform == service.PlatformGrok {
		parsed := service.ParseGrokMediaRequest(c.GetHeader("Content-Type"), body)
		model, moderationBody = parsed.Model, parsed.ModerationBody()
	} else if platform == service.PlatformGemini {
		// Gemini chat/native payloads are audited with their original schema.
	} else if h.openAI.gatewayService != nil {
		parsed, err := h.openAI.gatewayService.ParseOpenAIImagesRequest(c, body)
		if err != nil {
			imageTaskJSONError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
			return false
		}
		model, moderationBody = parsed.Model, parsed.ModerationBody()
	}
	if len(moderationBody) == 0 {
		c.Set(securityAuditCompletedContextKey, true)
		return true
	}
	reqLog := requestLogger(c, "handler.async_image.security_audit",
		zap.Int64("user_id", subject.UserID), zap.Int64("api_key_id", apiKey.ID), zap.String("model", model))
	protocol := service.ContentModerationProtocolOpenAIImages
	if platform == service.PlatformGemini {
		protocol = service.ContentModerationProtocolGemini
	}
	decision := h.openAI.checkSecurityAudit(c, reqLog, apiKey, subject, protocol, model, moderationBody)
	if decision != nil && !decision.AllowNextStage {
		h.openAI.openAISecurityAuditError(c, decision)
		return false
	}
	return true
}

func (h *AsyncImageHandler) Get(c *gin.Context) {
// Polling deliberately does not require new submissions to be enabled, only
// that the PostgreSQL task store remains reachable. Turning the storage switch
// off must not hide tasks that were already accepted.
	if !h.pollable() {
		imageTaskJSONError(c, http.StatusNotFound, "not_found_error", "async image tasks are not enabled")
		return
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.UserID <= 0 || apiKey.ID <= 0 {
		imageTaskError(c, service.ErrImageTaskForbidden)
		return
	}
	task, err := h.tasks.Get(c.Request.Context(), service.ImageTaskOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}, c.Param("task_id"))
	if err != nil {
		imageTaskError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	if task.Status == service.ImageTaskStatusQueued || task.Status == service.ImageTaskStatusProcessing {
		c.Header("Retry-After", "3")
	}
	c.JSON(http.StatusOK, task)
}

type asyncImageSubmission struct {
	Target string
	Path   string
	Model  string
	Size   string
	Count  int
	Body   []byte
}

func (h *AsyncImageHandler) parseSubmission(c *gin.Context, platform string, body []byte) (*asyncImageSubmission, error) {
	if platform == service.PlatformGrok {
		parsed := service.ParseGrokMediaRequest(c.GetHeader("Content-Type"), body)
		if strings.TrimSpace(parsed.Model) == "" {
			return nil, errors.New("model is required")
		}
		return &asyncImageSubmission{Target: "grok_images", Path: strings.TrimSuffix(c.Request.URL.Path, "/async"), Model: parsed.Model, Size: parsed.SizeTier, Count: parsed.N, Body: body}, nil
	}
	if platform == service.PlatformOpenAI {
		if h.openAI == nil || h.openAI.gatewayService == nil {
			var basic struct {
				Model, Size string
				N           int
				Stream      bool
			}
			if err := json.Unmarshal(body, &basic); err != nil {
				return nil, errors.New("invalid JSON request body")
			}
			if basic.Stream {
				return nil, errors.New("streaming image requests cannot be submitted as asynchronous tasks")
			}
			if basic.N <= 0 {
				basic.N = 1
			}
			return &asyncImageSubmission{Target: "openai_images", Path: strings.TrimSuffix(c.Request.URL.Path, "/async"), Model: basic.Model, Size: service.NormalizeImageBillingTierOrDefault(basic.Size), Count: basic.N, Body: body}, nil
		}
		parsed, err := h.openAI.gatewayService.ParseOpenAIImagesRequest(c, body)
		if err != nil {
			return nil, err
		}
		if parsed.Stream {
			return nil, errors.New("streaming image requests cannot be submitted as asynchronous tasks")
		}
		return &asyncImageSubmission{Target: "openai_images", Path: strings.TrimSuffix(c.Request.URL.Path, "/async"), Model: parsed.Model, Size: parsed.SizeTier, Count: parsed.N, Body: body}, nil
	}
	if isMultipartImagesContentType(c.GetHeader("Content-Type")) {
		return nil, errors.New("Gemini asynchronous image requests must use JSON")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, errors.New("invalid JSON request body")
	}
	var model string
	_ = json.Unmarshal(envelope["model"], &model)
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, errors.New("model is required")
	}
	var stream bool
	_ = json.Unmarshal(envelope["stream"], &stream)
	if stream {
		return nil, errors.New("streaming image requests cannot be submitted as asynchronous tasks")
	}
	size := geminiAsyncImageSize(body)
	count := 1
	if raw, ok := envelope["n"]; ok {
		_ = json.Unmarshal(raw, &count)
	}
	if count <= 0 {
		count = 1
	}
	if _, ok := envelope["messages"]; ok {
		return &asyncImageSubmission{Target: "gemini_chat", Path: "/v1/chat/completions", Model: model, Size: size, Count: count, Body: body}, nil
	}
	if _, ok := envelope["contents"]; ok {
		if !service.IsSafeGeminiModelPathSegment(model) {
			return nil, errors.New("invalid Gemini model")
		}
		delete(envelope, "model")
		delete(envelope, "stream")
		nativeBody, err := json.Marshal(envelope)
		if err != nil {
			return nil, err
		}
		return &asyncImageSubmission{Target: "gemini_native", Path: "/v1beta/models/" + model + ":generateContent", Model: model, Size: size, Count: count, Body: nativeBody}, nil
	}
	return nil, errors.New("Gemini request must contain messages or contents")
}

func geminiAsyncImageSize(body []byte) string {
	var req struct {
		GenerationConfig *struct {
			ImageConfig *struct {
				ImageSize string `json:"imageSize"`
			} `json:"imageConfig"`
		} `json:"generationConfig"`
	}
	if json.Unmarshal(body, &req) == nil && req.GenerationConfig != nil && req.GenerationConfig.ImageConfig != nil {
		return service.NormalizeImageBillingTierOrDefault(req.GenerationConfig.ImageConfig.ImageSize)
	}
	return service.NormalizeImageBillingTierOrDefault("")
}

func asyncImageHeaders(c *gin.Context) map[string]string {
	headers := map[string]string{"X-Sub2api-Remote-Addr": c.Request.RemoteAddr}
	for _, name := range []string{"User-Agent", "X-Forwarded-For", "X-Real-IP", "X-Request-ID", "OpenAI-Organization", "OpenAI-Project"} {
		if value := c.GetHeader(name); value != "" {
			headers[name] = value
		}
	}
	return headers
}

func (h *AsyncImageHandler) validateRequest(c *gin.Context, platform string, body []byte) error {
	if h.openAI == nil || h.openAI.gatewayService == nil {
		return nil
	}
	if platform == service.PlatformGrok {
		parsed := service.ParseGrokMediaRequest(c.GetHeader("Content-Type"), body)
		if strings.TrimSpace(parsed.Model) == "" {
			return errors.New("model is required")
		}
		return nil
	}
	parsed, err := h.openAI.gatewayService.ParseOpenAIImagesRequest(c, body)
	if err != nil {
		return err
	}
	if parsed.Stream {
		return errors.New("streaming image requests cannot be submitted as asynchronous tasks")
	}
	return nil
}

func (h *AsyncImageHandler) executeWithGateway(platform string, c *gin.Context) {
	if h.openAI == nil {
		imageTaskJSONError(c, http.StatusServiceUnavailable, "api_error", "image gateway is unavailable")
		return
	}
	if platform == service.PlatformGrok {
		h.openAI.GrokImages(c)
		return
	}
	h.openAI.Images(c)
}

func (h *AsyncImageHandler) Download(c *gin.Context) {
	if !h.pollable() {
		imageTaskJSONError(c, http.StatusNotFound, "not_found_error", "async image tasks are not enabled")
		return
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil {
		imageTaskError(c, service.ErrImageTaskForbidden)
		return
	}
	file, err := h.tasks.OpenImage(c.Request.Context(), service.ImageTaskOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}, c.Param("task_id"), c.Param("filename"))
	if err != nil {
		imageTaskError(c, err)
		return
	}
	defer func() { _ = file.Reader.Close() }()
	c.Header("Cache-Control", "private, max-age=3600, immutable")
	c.Header("X-Content-Type-Options", "nosniff")
	http.ServeContent(c.Writer, c.Request, file.Name, file.ModTime, file.Reader)
}

func (h *AsyncImageHandler) runRuntime(ctx context.Context) {
	concurrency := 60
	recoveryEvery, cleanupEvery, staleAfter := 30*time.Second, time.Hour, 35*time.Minute
	if h.cfg != nil {
		if h.cfg.AsyncImage.WorkerConcurrency > 0 {
			concurrency = h.cfg.AsyncImage.WorkerConcurrency
		}
		if h.cfg.AsyncImage.RecoveryIntervalSeconds > 0 {
			recoveryEvery = time.Duration(h.cfg.AsyncImage.RecoveryIntervalSeconds) * time.Second
		}
		if h.cfg.AsyncImage.CleanupIntervalMinutes > 0 {
			cleanupEvery = time.Duration(h.cfg.AsyncImage.CleanupIntervalMinutes) * time.Minute
		}
		if h.cfg.AsyncImage.StaleProcessingMinutes > 0 {
			staleAfter = time.Duration(h.cfg.AsyncImage.StaleProcessingMinutes) * time.Minute
		}
	}
	_ = h.tasks.Recover(ctx, staleAfter, 10000)
	for i := 0; i < concurrency; i++ {
		go h.runWorker(ctx, i)
	}
	go func() {
		recoveryTicker := time.NewTicker(recoveryEvery)
		cleanupTicker := time.NewTicker(cleanupEvery)
		defer recoveryTicker.Stop()
		defer cleanupTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-recoveryTicker.C:
				_ = h.tasks.Recover(ctx, staleAfter, 10000)
			case <-cleanupTicker.C:
				_, _ = h.tasks.CleanupExpired(ctx, 1000)
			}
		}
	}()
}

func (h *AsyncImageHandler) runWorker(ctx context.Context, workerID int) {
	for {
		if ctx.Err() != nil {
			return
		}
		id, err := h.tasks.Reserve(ctx, 5*time.Second)
		if errors.Is(err, service.ErrImageTaskQueueEmpty) {
			continue
		}
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		task, err := h.tasks.Claim(ctx, id)
		if errors.Is(err, service.ErrImageTaskNotFound) {
			continue
		}
		if err != nil {
			logger.L().Warn("image_task.worker_claim_failed", zap.Int("worker_id", workerID), zap.String("task_id", id), zap.Error(err))
			continue
		}
		h.executePersistentTask(ctx, task)
	}
}

type asyncImageAuditCompletedContextKey struct{}

func (h *AsyncImageHandler) executePersistentTask(parent context.Context, task *service.ImageTaskRecord) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.L().Error("image_task.execution_panicked", zap.String("task_id", task.ID), zap.Any("panic", recovered))
			h.failTask(task.ID, http.StatusInternalServerError, imageTaskErrorPayload("api_error", "image generation task panicked"))
		}
	}()
	if h.lookup == nil {
		h.failTask(task.ID, http.StatusServiceUnavailable, imageTaskErrorPayload("api_error", "API key service is unavailable"))
		return
	}
	apiKey, err := h.lookup(parent, task.APIKeyID)
	if err != nil || apiKey == nil || strings.TrimSpace(apiKey.Key) == "" {
		h.failTask(task.ID, http.StatusUnauthorized, imageTaskErrorPayload("authentication_error", "API key is no longer available"))
		return
	}
	h.engineMu.RLock()
	engine := h.engine
	h.engineMu.RUnlock()
	if engine == nil {
		h.failTask(task.ID, http.StatusServiceUnavailable, imageTaskErrorPayload("api_error", "image worker router is unavailable"))
		return
	}
	executionCtx, cancel := context.WithTimeout(parent, h.tasks.ExecutionTimeout())
	defer cancel()
	executionCtx = context.WithValue(executionCtx, asyncImageAuditCompletedContextKey{}, true)
	req := httptest.NewRequest(task.Method, task.RequestPath, bytes.NewReader(task.RequestBody)).WithContext(executionCtx)
	req.Header.Set("Authorization", "Bearer "+apiKey.Key)
	if task.ContentType != "" {
		req.Header.Set("Content-Type", task.ContentType)
	}
	for name, value := range task.Headers {
		if name == "X-Sub2api-Remote-Addr" {
			req.RemoteAddr = value
			continue
		}
		req.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)
	body := bytes.TrimSpace(recorder.Body.Bytes())
	if executionCtx.Err() != nil && len(body) == 0 {
		h.failTask(task.ID, http.StatusGatewayTimeout, imageTaskErrorPayload("timeout_error", "image generation task timed out"))
		return
	}
	statusCode := recorder.Code
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		if len(body) == 0 || !json.Valid(body) {
			h.failTask(task.ID, http.StatusBadGateway, imageTaskErrorPayload("api_error", "upstream returned an invalid image response"))
			return
		}
		if err := h.tasks.Complete(context.Background(), task.ID, statusCode, json.RawMessage(body)); err != nil {
			logger.L().Error("image_task.complete_store_failed", zap.String("task_id", task.ID), zap.Error(err))
		}
		return
	}
	h.failTask(task.ID, statusCode, extractImageTaskError(body))
}

func (h *AsyncImageHandler) run(taskID, platform string, taskCtx *gin.Context, recorder *httptest.ResponseRecorder, cancel context.CancelFunc) {
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.L().Error("image_task.execution_panicked", zap.String("task_id", taskID), zap.Any("panic", recovered))
			h.failTask(taskID, http.StatusInternalServerError, imageTaskErrorPayload("api_error", "image generation task panicked"))
		}
	}()

	h.execute(platform, taskCtx)
	body := bytes.TrimSpace(recorder.Body.Bytes())
	if err := taskCtx.Request.Context().Err(); err != nil && len(body) == 0 {
		h.failTask(taskID, http.StatusGatewayTimeout, imageTaskErrorPayload("timeout_error", "image generation task timed out"))
		return
	}
	statusCode := recorder.Code
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		if len(body) == 0 || !json.Valid(body) {
			h.failTask(taskID, http.StatusBadGateway, imageTaskErrorPayload("api_error", "upstream returned an invalid image response"))
			return
		}
		if err := h.tasks.Complete(context.Background(), taskID, statusCode, json.RawMessage(body)); err != nil {
			logger.L().Error("image_task.complete_store_failed", zap.String("task_id", taskID), zap.Error(err))
		}
		return
	}
	h.failTask(taskID, statusCode, extractImageTaskError(body))
}

func (h *AsyncImageHandler) failTask(taskID string, statusCode int, taskErr json.RawMessage) {
	if err := h.tasks.Fail(context.Background(), taskID, statusCode, taskErr); err != nil {
		logger.L().Error("image_task.failure_store_failed", zap.String("task_id", taskID), zap.Error(err))
	}
}

func newAsyncImageContext(c *gin.Context, body []byte, timeoutDuration time.Duration) (*gin.Context, *httptest.ResponseRecorder, context.CancelFunc) {
	base := context.WithoutCancel(c.Request.Context())
	executionCtx, cancel := context.WithTimeout(base, timeoutDuration)
	request := c.Request.Clone(executionCtx)
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	request.ContentLength = int64(len(body))
	request.URL.Path = strings.TrimSuffix(request.URL.Path, "/async")

	taskCtx := c.Copy()
	recorder := httptest.NewRecorder()
	recorderCtx, _ := gin.CreateTestContext(recorder)
	taskCtx.Writer = recorderCtx.Writer
	taskCtx.Request = request
	return taskCtx, recorder, cancel
}

func asyncImageRequestStreams(contentType string, body []byte) bool {
	if isMultipartImagesContentType(contentType) {
		return false
	}
	var envelope struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &envelope) == nil && envelope.Stream
}

func imageTaskPollURL(submitPath, taskID string) string {
	if strings.HasPrefix(submitPath, "/v1/") {
		return "/v1/images/tasks/" + taskID
	}
	return "/images/tasks/" + taskID
}

func extractImageTaskError(body []byte) json.RawMessage {
	if json.Valid(body) {
		var envelope struct {
			Error json.RawMessage `json:"error"`
		}
		if json.Unmarshal(body, &envelope) == nil && len(envelope.Error) > 0 && json.Valid(envelope.Error) {
			return envelope.Error
		}
		return json.RawMessage(body)
	}
	return imageTaskErrorPayload("api_error", "image generation failed")
}

func imageTaskErrorPayload(errorType, message string) json.RawMessage {
	data, _ := json.Marshal(gin.H{"type": errorType, "message": message})
	return data
}

func imageTaskError(c *gin.Context, err error) {
	status := infraerrors.Code(err)
	code := infraerrors.Reason(err)
	message := infraerrors.Message(err)
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(code) == "" {
		code = "IMAGE_TASK_ERROR"
	}
	imageTaskJSONError(c, status, code, message)
}

func imageTaskJSONError(c *gin.Context, status int, code, message string) {
	c.Header("Cache-Control", "no-store")
	c.JSON(status, gin.H{"error": gin.H{"type": code, "code": code, "message": message}})
}
