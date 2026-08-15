package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	ImageTaskStatusPending    = "pending"
	ImageTaskStatusQueued     = "queued"
	ImageTaskStatusProcessing = "processing"
	ImageTaskStatusCompleted  = "completed"
	ImageTaskStatusFailed     = "failed"

	defaultImageTaskTTL              = 24 * time.Hour
	defaultImageTaskExecutionTimeout = 30 * time.Minute
)

var (
	ErrImageTaskNotFound            = infraerrors.New(http.StatusNotFound, "IMAGE_TASK_NOT_FOUND", "image task not found")
	ErrImageTaskForbidden           = infraerrors.New(http.StatusForbidden, "IMAGE_TASK_FORBIDDEN", "image task does not belong to this API key")
	ErrImageTaskUnavailable         = infraerrors.New(http.StatusServiceUnavailable, "IMAGE_TASK_UNAVAILABLE", "image task storage is unavailable")
	ErrImageTaskIdempotencyConflict = infraerrors.New(http.StatusConflict, "IMAGE_TASK_IDEMPOTENCY_CONFLICT", "idempotency key was already used with a different request")
	ErrImageTaskQueueEmpty          = infraerrors.New(http.StatusNotFound, "IMAGE_TASK_QUEUE_EMPTY", "image task queue is empty")
)

// ImageTaskRecord is the private PostgreSQL representation of an asynchronous
// image request. Ownership and request snapshot fields are omitted from the
// public view.
type ImageTaskRecord struct {
	ID             string            `json:"id"`
	UserID         int64             `json:"user_id"`
	APIKeyID       int64             `json:"api_key_id"`
	Platform       string            `json:"platform,omitempty"`
	Target         string            `json:"target,omitempty"`
	Model          string            `json:"model,omitempty"`
	Method         string            `json:"method,omitempty"`
	RequestPath    string            `json:"request_path,omitempty"`
	ContentType    string            `json:"content_type,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	RequestBody    []byte            `json:"request_body,omitempty"`
	RequestHash    string            `json:"request_hash,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Status         string            `json:"status"`
	HTTPStatus     int               `json:"http_status,omitempty"`
	Result         json.RawMessage   `json:"result,omitempty"`
	Error          json.RawMessage   `json:"error,omitempty"`
	CreatedAt      int64             `json:"created_at"`
	CompletedAt    *int64            `json:"completed_at,omitempty"`
	ExpiresAt      int64             `json:"expires_at"`
	StartedAt      *int64            `json:"started_at,omitempty"`
	HoldAmount     float64           `json:"hold_amount,omitempty"`
	HoldReleasedAt *int64            `json:"hold_released_at,omitempty"`
}

// ImageTask is the API-safe task representation returned to callers.
type ImageTask struct {
	ID          string          `json:"id"`
	TaskID      string          `json:"task_id"`
	Object      string          `json:"object"`
	Status      string          `json:"status"`
	HTTPStatus  int             `json:"http_status,omitempty"`
	ImageURL    string          `json:"image_url,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	CreatedAt   int64           `json:"created_at"`
	CompletedAt *int64          `json:"completed_at,omitempty"`
	ExpiresAt   int64           `json:"expires_at"`
}

type ImageTaskOwner struct {
	UserID   int64
	APIKeyID int64
}

type ImageTaskStore interface {
	Save(ctx context.Context, task *ImageTaskRecord, ttl time.Duration) error
	Get(ctx context.Context, id string) (*ImageTaskRecord, error)
}

// DurableImageTaskStore is the PostgreSQL source of truth used by the worker.
// Save/Get stay in the small base interface so existing cache-only tests remain
// useful and older deployments fail closed instead of losing tasks.
type DurableImageTaskStore interface {
	ImageTaskStore
	Create(ctx context.Context, task *ImageTaskRecord) error
	GetByIdempotencyKey(ctx context.Context, owner ImageTaskOwner, key string) (*ImageTaskRecord, error)
	ClaimQueued(ctx context.Context, id string) (*ImageTaskRecord, error)
	MarkQueued(ctx context.Context, id string) error
	MarkHoldReleased(ctx context.Context, id string, at time.Time) error
	ListQueued(ctx context.Context, limit int) ([]string, error)
	ListUnreleasedTerminalHolds(ctx context.Context, limit int) ([]*ImageTaskRecord, error)
	FailStalePending(ctx context.Context, cutoff time.Time, taskError json.RawMessage) ([]*ImageTaskRecord, error)
	FailStaleProcessing(ctx context.Context, cutoff time.Time, taskError json.RawMessage) ([]*ImageTaskRecord, error)
	DeleteExpired(ctx context.Context, now time.Time, limit int) (int, error)
}

type ImageTaskQueue interface {
	Enqueue(ctx context.Context, id string) error
	Reserve(ctx context.Context, timeout time.Duration) (string, error)
}

type ImageTaskSubmitInput struct {
	Owner          ImageTaskOwner
	Platform       string
	Target         string
	Model          string
	Method         string
	RequestPath    string
	ContentType    string
	Headers        map[string]string
	Body           []byte
	IdempotencyKey string
	HoldAmount     float64
}

// ImageStorageResolver reports the currently effective result-storage binding.
// It exists so the async image feature can be switched on and off from the admin
// UI without a restart: the wiring below is fixed at startup, but the answer to
// "is result storage configured right now" is re-read (and cached) per call.
type ImageStorageResolver func() (uploader *ImageResultUploader, enabled bool)

type ImageTaskService struct {
	store             ImageTaskStore
	uploader          *ImageResultUploader
	enabled           bool
	resolve           ImageStorageResolver
	ttl               time.Duration
	executionTimeout  time.Duration
	queue             ImageTaskQueue
	billingRepo       UsageBillingRepository
	authCache         APIKeyAuthCacheInvalidator
	billingService    *BillingService
	userGroupRateRepo UserGroupRateRepository
	config            *config.Config
}

func NewImageTaskService(store ImageTaskStore) *ImageTaskService {
	return NewImageTaskServiceWithOptions(store, defaultImageTaskTTL, defaultImageTaskExecutionTimeout)
}

func NewImageTaskServiceWithOptions(store ImageTaskStore, ttl, executionTimeout time.Duration) *ImageTaskService {
	if ttl <= 0 {
		ttl = defaultImageTaskTTL
	}
	if executionTimeout <= 0 {
		executionTimeout = defaultImageTaskExecutionTimeout
	}
	return &ImageTaskService{store: store, ttl: ttl, executionTimeout: executionTimeout}
}

// NewImageTaskServiceWithUploader 构造一个已启用的图片任务服务：结果会先经 uploader
// 转存到对象存储再落 Redis。uploader 为 nil 时不做转存（仅用于测试）。
func NewImageTaskServiceWithUploader(store ImageTaskStore, uploader *ImageResultUploader, ttl, executionTimeout time.Duration) *ImageTaskService {
	s := NewImageTaskServiceWithOptions(store, ttl, executionTimeout)
	s.uploader = uploader
	s.enabled = true
	return s
}

// NewImageTaskServiceWithResolver 构造一个由 resolver 决定启用状态的服务：
// 开关与凭证来自后台设置，保存后立即生效，无需重启。
func NewImageTaskServiceWithResolver(store ImageTaskStore, resolve ImageStorageResolver, ttl, executionTimeout time.Duration) *ImageTaskService {
	s := NewImageTaskServiceWithOptions(store, ttl, executionTimeout)
	s.resolve = resolve
	return s
}

func NewDurableImageTaskService(store DurableImageTaskStore, queue ImageTaskQueue, billingRepo UsageBillingRepository, authCache APIKeyAuthCacheInvalidator, billingService *BillingService, userGroupRateRepo UserGroupRateRepository, cfg *config.Config, resolve ImageStorageResolver, ttl, executionTimeout time.Duration) *ImageTaskService {
	s := NewImageTaskServiceWithResolver(store, resolve, ttl, executionTimeout)
	s.queue = queue
	s.billingRepo = billingRepo
	s.authCache = authCache
	s.billingService = billingService
	s.userGroupRateRepo = userGroupRateRepo
	s.config = cfg
	return s
}

func (s *ImageTaskService) EstimateHold(ctx context.Context, apiKey *APIKey, model, size string, count int) float64 {
	if s == nil || s.billingService == nil || apiKey == nil || apiKey.Group == nil || count <= 0 {
		return 0
	}
	if s.config != nil && s.config.RunMode == config.RunModeSimple {
		return 0
	}
	if apiKey.Group.IsSubscriptionType() {
		return 0
	}
	multiplier := apiKey.Group.RateMultiplier
	if s.userGroupRateRepo != nil {
		if rate, err := s.userGroupRateRepo.GetByUserAndGroup(ctx, apiKey.UserID, apiKey.Group.ID); err == nil && rate != nil {
			multiplier = *rate
		}
	}
	multiplier = resolveImageRateMultiplier(apiKey, multiplier)
	cost := s.billingService.CalculateImageCost(model, size, count, imagePriceConfigFromAPIKey(apiKey), multiplier)
	if cost == nil || cost.ActualCost < 0 {
		return 0
	}
	return cost.ActualCost
}

// current 返回当前生效的 uploader 与启用状态。
// 注入了 resolver 时以 resolver 为准（后台设置可热切换），否则回落到构造时固定的值。
func (s *ImageTaskService) current() (*ImageResultUploader, bool) {
	if s == nil {
		return nil, false
	}
	if s.resolve != nil {
		return s.resolve()
	}
	return s.uploader, s.enabled
}

// Enabled 表示异步图片任务功能和结果存储是否可用。
// 关闭时 handler 直接返回 404，不创建任务。
func (s *ImageTaskService) Enabled() bool {
	if s == nil || s.store == nil {
		return false
	}
	_, enabled := s.current()
	return enabled
}

// Pollable 表示已创建的任务能否被查询。
// 比 Enabled 弱：只要 store 可用即可，从而在功能被关掉后仍能取回进行中的任务结果。
func (s *ImageTaskService) Pollable() bool {
	return s != nil && s.store != nil
}

func (s *ImageTaskService) Durable() bool {
	if s == nil || s.queue == nil {
		return false
	}
	_, ok := s.store.(DurableImageTaskStore)
	return ok
}

func (s *ImageTaskService) ExecutionTimeout() time.Duration {
	if s == nil || s.executionTimeout <= 0 {
		return defaultImageTaskExecutionTimeout
	}
	return s.executionTimeout
}

func (s *ImageTaskService) OpenImage(ctx context.Context, owner ImageTaskOwner, taskID, filename string) (*ImageStoredFile, error) {
	if _, err := s.Get(ctx, owner, taskID); err != nil {
		return nil, err
	}
	uploader, enabled := s.current()
	if !enabled || uploader == nil {
		return nil, ErrImageTaskNotFound
	}
	file, err := uploader.Open(ctx, strings.TrimSpace(taskID), strings.TrimSpace(filename))
	if err != nil {
		return nil, ErrImageTaskNotFound
	}
	return file, nil
}

func (s *ImageTaskService) Create(ctx context.Context, owner ImageTaskOwner) (*ImageTask, error) {
	if s == nil || s.store == nil {
		return nil, ErrImageTaskUnavailable
	}
	now := time.Now().UTC()
	task := &ImageTaskRecord{
		ID:        "imgtask_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		UserID:    owner.UserID,
		APIKeyID:  owner.APIKeyID,
		Status:    ImageTaskStatusProcessing,
		CreatedAt: now.Unix(),
		ExpiresAt: now.Add(s.ttl).Unix(),
	}
	if err := s.store.Save(ctx, task, s.ttl); err != nil {
		return nil, ErrImageTaskUnavailable.WithCause(err)
	}
	return imageTaskToPublic(task), nil
}

func (s *ImageTaskService) Submit(ctx context.Context, in ImageTaskSubmitInput) (*ImageTask, bool, error) {
	store, ok := s.store.(DurableImageTaskStore)
	if s == nil || !ok || s.queue == nil {
		return nil, false, ErrImageTaskUnavailable
	}
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)
	hash := hashImageTaskRequest(in)
	if in.IdempotencyKey != "" {
		existing, err := store.GetByIdempotencyKey(ctx, in.Owner, in.IdempotencyKey)
		if err == nil {
			if existing.RequestHash != hash {
				return nil, false, ErrImageTaskIdempotencyConflict
			}
			if existing.Status == ImageTaskStatusQueued {
				_ = s.queue.Enqueue(ctx, existing.ID)
			}
			return imageTaskToPublic(existing), false, nil
		}
		if !errors.Is(err, ErrImageTaskNotFound) {
			return nil, false, ErrImageTaskUnavailable.WithCause(err)
		}
	}

	now := time.Now().UTC()
	task := &ImageTaskRecord{
		ID:     "imgtask_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		UserID: in.Owner.UserID, APIKeyID: in.Owner.APIKeyID,
		Platform: strings.TrimSpace(in.Platform), Target: strings.TrimSpace(in.Target), Model: strings.TrimSpace(in.Model),
		Method: strings.TrimSpace(in.Method), RequestPath: strings.TrimSpace(in.RequestPath), ContentType: strings.TrimSpace(in.ContentType),
		Headers: cloneImageTaskHeaders(in.Headers), RequestBody: append([]byte(nil), in.Body...), RequestHash: hash,
		IdempotencyKey: in.IdempotencyKey, Status: ImageTaskStatusPending,
		CreatedAt: now.Unix(), ExpiresAt: now.Add(s.ttl).Unix(), HoldAmount: nonNegativeImageTaskAmount(in.HoldAmount),
	}
	if err := store.Create(ctx, task); err != nil {
		if in.IdempotencyKey != "" {
			existing, getErr := store.GetByIdempotencyKey(ctx, in.Owner, in.IdempotencyKey)
			if getErr == nil {
				if existing.RequestHash != hash {
					return nil, false, ErrImageTaskIdempotencyConflict
				}
				return imageTaskToPublic(existing), false, nil
			}
		}
		return nil, false, ErrImageTaskUnavailable.WithCause(err)
	}
	if err := s.reserveHold(ctx, task); err != nil {
		_ = s.Fail(context.Background(), task.ID, http.StatusPaymentRequired, imageTaskErrorJSON("insufficient_balance", err.Error()))
		return nil, false, err
	}
	if err := store.MarkQueued(ctx, task.ID); err != nil {
		_ = s.releaseHold(context.Background(), task)
		_ = s.Fail(context.Background(), task.ID, http.StatusServiceUnavailable, imageTaskErrorJSON("queue_error", "failed to activate image task"))
		return nil, false, ErrImageTaskUnavailable.WithCause(err)
	}
	task.Status = ImageTaskStatusQueued
	if err := s.queue.Enqueue(ctx, task.ID); err != nil {
		_ = s.releaseHold(context.Background(), task)
		_ = s.Fail(context.Background(), task.ID, http.StatusServiceUnavailable, imageTaskErrorJSON("queue_error", "failed to enqueue image task"))
		return nil, false, ErrImageTaskUnavailable.WithCause(err)
	}
	return imageTaskToPublic(task), true, nil
}

func nonNegativeImageTaskAmount(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}

func hashImageTaskRequest(in ImageTaskSubmitInput) string {
	h := sha256.New()
	_, _ = h.Write([]byte(strings.TrimSpace(in.Platform)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(in.Target)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(in.ContentType)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(in.Body)
	return hex.EncodeToString(h.Sum(nil))
}

func cloneImageTaskHeaders(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *ImageTaskService) Get(ctx context.Context, owner ImageTaskOwner, id string) (*ImageTask, error) {
	if s == nil || s.store == nil {
		return nil, ErrImageTaskUnavailable
	}
	task, err := s.store.Get(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, ErrImageTaskNotFound) {
			return nil, ErrImageTaskNotFound
		}
		return nil, ErrImageTaskUnavailable.WithCause(err)
	}
	if task.UserID != owner.UserID || task.APIKeyID != owner.APIKeyID {
		// Do not reveal whether a random task ID exists for another caller.
		return nil, ErrImageTaskNotFound
	}
	return imageTaskToPublic(task), nil
}

func (s *ImageTaskService) Claim(ctx context.Context, id string) (*ImageTaskRecord, error) {
	store, ok := s.store.(DurableImageTaskStore)
	if s == nil || !ok {
		return nil, ErrImageTaskUnavailable
	}
	task, err := store.ClaimQueued(ctx, strings.TrimSpace(id))
	if err != nil {
		return nil, err
	}
	if err := s.releaseHold(ctx, task); err != nil {
		_ = s.Fail(context.Background(), task.ID, http.StatusInternalServerError, imageTaskErrorJSON("billing_hold_release_failed", "failed to release reserved balance"))
		return nil, err
	}
	return task, nil
}

func (s *ImageTaskService) Reserve(ctx context.Context, timeout time.Duration) (string, error) {
	if s == nil || s.queue == nil {
		return "", ErrImageTaskUnavailable
	}
	return s.queue.Reserve(ctx, timeout)
}

func (s *ImageTaskService) Recover(ctx context.Context, staleAfter time.Duration, limit int) error {
	store, ok := s.store.(DurableImageTaskStore)
	if s == nil || !ok || s.queue == nil {
		return ErrImageTaskUnavailable
	}
	queued, err := store.ListQueued(ctx, limit)
	if err != nil {
		return err
	}
	for _, id := range queued {
		_ = s.queue.Enqueue(ctx, id)
	}
	abandoned, pendingErr := store.FailStalePending(ctx, time.Now().Add(-5*time.Minute), imageTaskErrorJSON("submission_interrupted", "image task submission was interrupted before it entered the queue"))
	if pendingErr != nil {
		return pendingErr
	}
	for _, task := range abandoned {
		_ = s.releaseHold(ctx, task)
	}
	if staleAfter > 0 {
		failed, failErr := store.FailStaleProcessing(ctx, time.Now().Add(-staleAfter), imageTaskErrorJSON("execution_interrupted", "image task execution was interrupted; it was not retried to avoid duplicate upstream charges"))
		if failErr != nil {
			return failErr
		}
		for _, task := range failed {
			_ = s.releaseHold(ctx, task)
		}
	}
	unreleased, releaseErr := store.ListUnreleasedTerminalHolds(ctx, limit)
	if releaseErr != nil {
		return releaseErr
	}
	for _, task := range unreleased {
		_ = s.releaseHold(ctx, task)
	}
	return nil
}

func (s *ImageTaskService) CleanupExpired(ctx context.Context, limit int) (int, error) {
	store, ok := s.store.(DurableImageTaskStore)
	if s == nil || !ok {
		return 0, ErrImageTaskUnavailable
	}
	return store.DeleteExpired(ctx, time.Now(), limit)
}

func (s *ImageTaskService) reserveHold(ctx context.Context, task *ImageTaskRecord) error {
	if task == nil || task.HoldAmount <= 0 {
		return nil
	}
	if s.billingRepo == nil {
		return ErrImageTaskUnavailable.WithCause(errors.New("image task billing repository is unavailable"))
	}
	cmd := &BatchImageBalanceHoldCommand{
		RequestID: BatchImageHoldRequestID(task.ID), APIKeyID: task.APIKeyID,
		UserID: task.UserID, BatchID: task.ID, HoldAmount: task.HoldAmount,
		RequestPayloadHash: task.RequestHash,
	}
	if _, err := s.billingRepo.ReserveBatchImageBalance(ctx, cmd); err != nil {
		if errors.Is(err, ErrBatchImageInsufficientBalance) {
			return ErrBatchImageInsufficientBalance
		}
		return ErrImageTaskUnavailable.WithCause(err)
	}
	s.invalidateAuthCache(ctx, task.UserID)
	return nil
}

func (s *ImageTaskService) releaseHold(ctx context.Context, task *ImageTaskRecord) error {
	if task == nil || task.HoldAmount <= 0 || task.HoldReleasedAt != nil {
		return nil
	}
	store, ok := s.store.(DurableImageTaskStore)
	if !ok || s.billingRepo == nil {
		return ErrImageTaskUnavailable
	}
	cmd := &BatchImageBalanceHoldCommand{
		RequestID: BatchImageReleaseRequestID(task.ID), APIKeyID: task.APIKeyID,
		UserID: task.UserID, BatchID: task.ID, HoldAmount: task.HoldAmount,
		RequestPayloadHash: task.RequestHash,
	}
	if _, err := s.billingRepo.ReleaseBatchImageBalance(ctx, cmd); err != nil && !errors.Is(err, ErrUsageBillingRequestConflict) {
		return ErrImageTaskUnavailable.WithCause(err)
	}
	now := time.Now().UTC()
	if err := store.MarkHoldReleased(ctx, task.ID, now); err != nil {
		return ErrImageTaskUnavailable.WithCause(err)
	}
	releasedAt := now.Unix()
	task.HoldReleasedAt = &releasedAt
	s.invalidateAuthCache(ctx, task.UserID)
	return nil
}

func (s *ImageTaskService) invalidateAuthCache(ctx context.Context, userID int64) {
	if s.authCache != nil && userID > 0 {
		s.authCache.InvalidateAuthCacheByUserID(ctx, userID)
	}
}

func (s *ImageTaskService) Complete(ctx context.Context, id string, statusCode int, result json.RawMessage) error {
	if !json.Valid(result) {
		return s.Fail(ctx, id, http.StatusBadGateway, imageTaskErrorJSON("api_error", "upstream returned a non-JSON image response"))
	}
	uploader, storageEnabled := s.current()
	if s.Durable() && (!storageEnabled || uploader == nil) {
		return s.Fail(ctx, id, http.StatusServiceUnavailable, imageTaskErrorJSON("image_storage_unavailable", "image result storage became unavailable"))
	}
	if uploader != nil {
		rewritten, err := uploader.Rewrite(ctx, id, result)
		if err != nil {
			// 转存失败不回退存 base64，避免大 blob 进入状态库。
			logger.L().Error("image_task.offload_failed", zap.String("task_id", id), zap.Error(err))
			return s.Fail(ctx, id, http.StatusBadGateway, imageTaskErrorJSON("api_error", "failed to store generated image to object storage"))
		}
		result = rewritten
	}
	return s.finish(ctx, id, ImageTaskStatusCompleted, statusCode, result, nil)
}

func (s *ImageTaskService) Fail(ctx context.Context, id string, statusCode int, taskErr json.RawMessage) error {
	if !json.Valid(taskErr) {
		taskErr = imageTaskErrorJSON("api_error", "image generation failed")
	}
	return s.finish(ctx, id, ImageTaskStatusFailed, statusCode, nil, taskErr)
}

func (s *ImageTaskService) finish(ctx context.Context, id, status string, statusCode int, result, taskErr json.RawMessage) error {
	if s == nil || s.store == nil {
		return ErrImageTaskUnavailable
	}
	task, err := s.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrImageTaskNotFound) {
			return ErrImageTaskNotFound
		}
		return ErrImageTaskUnavailable.WithCause(err)
	}
	now := time.Now().UTC()
	completedAt := now.Unix()
	task.Status = status
	task.HTTPStatus = statusCode
	task.Result = result
	task.Error = taskErr
	task.CompletedAt = &completedAt
	task.ExpiresAt = now.Add(s.ttl).Unix()
	if err := s.store.Save(ctx, task, s.ttl); err != nil {
		return ErrImageTaskUnavailable.WithCause(err)
	}
	return nil
}

func imageTaskToPublic(task *ImageTaskRecord) *ImageTask {
	if task == nil {
		return nil
	}
	return &ImageTask{
		ID:          task.ID,
		TaskID:      task.ID,
		Object:      "image.generation.task",
		Status:      task.Status,
		HTTPStatus:  task.HTTPStatus,
		ImageURL:    firstImageTaskURL(task.Result),
		Result:      task.Result,
		Error:       task.Error,
		CreatedAt:   task.CreatedAt,
		CompletedAt: task.CompletedAt,
		ExpiresAt:   task.ExpiresAt,
	}
}

func firstImageTaskURL(result json.RawMessage) string {
	if len(result) == 0 || !json.Valid(result) {
		return ""
	}
	var response struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if json.Unmarshal(result, &response) == nil && len(response.Data) > 0 {
		if value := strings.TrimSpace(response.Data[0].URL); value != "" {
			return value
		}
	}
	var nested any
	if json.Unmarshal(result, &nested) != nil {
		return ""
	}
	return findNestedImageTaskURL(nested)
}

func findNestedImageTaskURL(value any) string {
	switch current := value.(type) {
	case map[string]any:
		for _, key := range []string{"fileUri", "file_uri", "url"} {
			if candidate, ok := current[key].(string); ok && strings.TrimSpace(candidate) != "" {
				return strings.TrimSpace(candidate)
			}
		}
		for _, child := range current {
			if found := findNestedImageTaskURL(child); found != "" {
				return found
			}
		}
	case []any:
		for _, child := range current {
			if found := findNestedImageTaskURL(child); found != "" {
				return found
			}
		}
	case string:
		marker := "![image]("
		if start := strings.Index(current, marker); start >= 0 {
			rest := current[start+len(marker):]
			if end := strings.Index(rest, ")"); end > 0 {
				return strings.TrimSpace(rest[:end])
			}
		}
	}
	return ""
}

func imageTaskErrorJSON(errorType, message string) json.RawMessage {
	data, _ := json.Marshal(map[string]string{"type": errorType, "message": message})
	return data
}
