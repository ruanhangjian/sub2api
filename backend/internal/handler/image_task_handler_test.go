package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type asyncImageMemoryStore struct {
	mu    sync.RWMutex
	tasks map[string]*service.ImageTaskRecord
}

func (s *asyncImageMemoryStore) Save(_ context.Context, task *service.ImageTaskRecord, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	s.tasks[task.ID] = &copy
	return nil
}

func (s *asyncImageMemoryStore) Get(_ context.Context, id string) (*service.ImageTaskRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task := s.tasks[id]
	if task == nil {
		return nil, service.ErrImageTaskNotFound
	}
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	return &copy, nil
}

func TestAsyncImageHandlerSubmitAndPoll(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	release := make(chan struct{})
	h := &AsyncImageHandler{tasks: tasks}
	h.execute = func(_ string, c *gin.Context) {
		<-release
		c.JSON(http.StatusOK, gin.H{"created": 123, "data": []gin.H{{"url": "https://example.test/image.png"}}})
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		groupID := int64(3)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
			ID:      9,
			UserID:  7,
			GroupID: &groupID,
			Group:   &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true},
		})
		c.Next()
	})
	router.POST("/v1/images/generations/async", h.Submit)
	router.GET("/v1/images/tasks/:task_id", h.Get)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(`{"model":"gpt-image-1","prompt":"cat"}`)).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.Equal(t, "3", w.Header().Get("Retry-After"))

	var accepted struct {
		TaskID  string `json:"task_id"`
		Status  string `json:"status"`
		PollURL string `json:"poll_url"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &accepted))
	require.Equal(t, service.ImageTaskStatusProcessing, accepted.Status)
	require.Equal(t, "/v1/images/tasks/"+accepted.TaskID, accepted.PollURL)
	require.Equal(t, accepted.PollURL, w.Header().Get("Location"))

	// The detached background request must survive completion of/cancellation
	// from the short submission request.
	cancelRequest()
	close(release)
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9}, accepted.TaskID)
		return err == nil && got.Status == service.ImageTaskStatusCompleted
	}, time.Second, 10*time.Millisecond)

	pollReq := httptest.NewRequest(http.MethodGet, accepted.PollURL, nil)
	pollWriter := httptest.NewRecorder()
	router.ServeHTTP(pollWriter, pollReq)
	require.Equal(t, http.StatusOK, pollWriter.Code)
	require.Equal(t, "no-store", pollWriter.Header().Get("Cache-Control"))
	require.Empty(t, pollWriter.Header().Get("Retry-After"))
	require.Contains(t, pollWriter.Body.String(), "https://example.test/image.png")
}

// When object storage is not configured the feature is fully disabled: the
// endpoints must return 404 without creating a task or writing to Redis.
func TestAsyncImageHandlerDisabledReturns404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}
	tasks := service.NewImageTaskServiceWithOptions(store, time.Hour, time.Minute) // enabled == false
	h := &AsyncImageHandler{tasks: tasks}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		groupID := int64(3)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
			ID:      9,
			UserID:  7,
			GroupID: &groupID,
			Group:   &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true},
		})
		c.Next()
	})
	router.POST("/v1/images/generations/async", h.Submit)
	router.GET("/v1/images/tasks/:task_id", h.Get)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(`{"model":"gpt-image-1","prompt":"cat"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "not enabled")

	pollReq := httptest.NewRequest(http.MethodGet, "/v1/images/tasks/imgtask_missing", nil)
	pollWriter := httptest.NewRecorder()
	router.ServeHTTP(pollWriter, pollReq)
	require.Equal(t, http.StatusNotFound, pollWriter.Code)

	// No task was created / persisted.
	require.Empty(t, store.tasks)
}

func TestAsyncImageSubmissionTargetsAllImagePlatforms(t *testing.T) {
	h := NewAsyncImageHandler(nil, nil)
	tests := []struct {
		name, platform, body, wantTarget, wantPath, wantModel, wantSize string
	}{
		{name: "OpenAI", platform: service.PlatformOpenAI, body: `{"model":"gpt-image-2","size":"2048x2048","n":2}`, wantTarget: "openai_images", wantPath: "/v1/images/generations", wantModel: "gpt-image-2", wantSize: "2K"},
		{name: "Grok", platform: service.PlatformGrok, body: `{"model":"grok-imagine-image","size":"1024x1024"}`, wantTarget: "grok_images", wantPath: "/v1/images/generations", wantModel: "grok-imagine-image", wantSize: "1K"},
		{name: "Gemini chat", platform: service.PlatformGemini, body: `{"model":"gemini-3.1-flash-image","messages":[{"role":"user","content":"draw"}],"generationConfig":{"imageConfig":{"imageSize":"4K"}}}`, wantTarget: "gemini_chat", wantPath: "/v1/chat/completions", wantModel: "gemini-3.1-flash-image", wantSize: "4K"},
		{name: "Gemini native", platform: service.PlatformGemini, body: `{"model":"gemini-3-pro-image","contents":[{"parts":[{"text":"draw"}]}]}`, wantTarget: "gemini_native", wantPath: "/v1beta/models/gemini-3-pro-image:generateContent", wantModel: "gemini-3-pro-image", wantSize: "2K"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = req
			got, err := h.parseSubmission(c, tt.platform, []byte(tt.body))
			require.NoError(t, err)
			require.Equal(t, tt.wantTarget, got.Target)
			require.Equal(t, tt.wantPath, got.Path)
			require.Equal(t, tt.wantModel, got.Model)
			require.Equal(t, tt.wantSize, got.Size)
			if tt.wantTarget == "gemini_native" {
				require.NotContains(t, string(got.Body), `"model"`)
			}
		})
	}
}

func TestAsyncImagePersistentWorkerUsesOriginalGatewayRouteOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "OpenAI", path: "/v1/images/generations", body: `{"model":"gpt-image-2","size":"1024x1024"}`},
		{name: "Grok", path: "/v1/images/generations", body: `{"model":"grok-imagine-image","size":"2048x2048"}`},
		{name: "Gemini chat", path: "/v1/chat/completions", body: `{"model":"gemini-3.1-flash-image","messages":[{"role":"user","content":"draw"}]}`},
		{name: "Gemini native", path: "/v1beta/models/gemini-3-pro-image:generateContent", body: `{"contents":[{"parts":[{"text":"draw"}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}
			tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
			created, err := tasks.Create(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9})
			require.NoError(t, err)

			calls := 0
			router := gin.New()
			router.POST(tt.path, func(c *gin.Context) {
				calls++
				require.Equal(t, "Bearer sk-worker-test", c.GetHeader("Authorization"))
				requestBody, readErr := io.ReadAll(c.Request.Body)
				require.NoError(t, readErr)
				require.JSONEq(t, tt.body, string(requestBody))
				c.JSON(http.StatusOK, gin.H{"data": []gin.H{{"url": "https://example.test/image.png"}}})
			})

			h := NewAsyncImageHandler(tasks, nil)
			h.lookup = func(_ context.Context, id int64) (*service.APIKey, error) {
				require.Equal(t, int64(9), id)
				return &service.APIKey{ID: id, UserID: 7, Key: "sk-worker-test"}, nil
			}
			h.AttachEngine(router)
			h.executePersistentTask(context.Background(), &service.ImageTaskRecord{
				ID: created.ID, UserID: 7, APIKeyID: 9, Method: http.MethodPost,
				RequestPath: tt.path, ContentType: "application/json", RequestBody: []byte(tt.body),
			})

			require.Equal(t, 1, calls, "one task must enter the original gateway exactly once")
			got, err := tasks.Get(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9}, created.ID)
			require.NoError(t, err)
			require.Equal(t, service.ImageTaskStatusCompleted, got.Status)
		})
	}
}

type blockingAsyncImageQueue struct {
	mu      sync.Mutex
	entered int
	release chan struct{}
}

func (*blockingAsyncImageQueue) Enqueue(context.Context, string) error { return nil }

func (q *blockingAsyncImageQueue) Reserve(ctx context.Context, _ time.Duration) (string, error) {
	q.mu.Lock()
	q.entered++
	q.mu.Unlock()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-q.release:
		return "", service.ErrImageTaskQueueEmpty
	}
}

type emptyDurableImageTaskStore struct{ *asyncImageMemoryStore }

func (*emptyDurableImageTaskStore) Create(context.Context, *service.ImageTaskRecord) error {
	return nil
}
func (*emptyDurableImageTaskStore) GetByIdempotencyKey(context.Context, service.ImageTaskOwner, string) (*service.ImageTaskRecord, error) {
	return nil, service.ErrImageTaskNotFound
}
func (*emptyDurableImageTaskStore) ClaimQueued(context.Context, string) (*service.ImageTaskRecord, error) {
	return nil, service.ErrImageTaskNotFound
}
func (*emptyDurableImageTaskStore) MarkQueued(context.Context, string) error { return nil }
func (*emptyDurableImageTaskStore) MarkHoldReleased(context.Context, string, time.Time) error {
	return nil
}
func (*emptyDurableImageTaskStore) ListQueued(context.Context, int) ([]string, error) {
	return nil, nil
}
func (*emptyDurableImageTaskStore) ListUnreleasedTerminalHolds(context.Context, int) ([]*service.ImageTaskRecord, error) {
	return nil, nil
}
func (*emptyDurableImageTaskStore) FailStalePending(context.Context, time.Time, json.RawMessage) ([]*service.ImageTaskRecord, error) {
	return nil, nil
}
func (*emptyDurableImageTaskStore) FailStaleProcessing(context.Context, time.Time, json.RawMessage) ([]*service.ImageTaskRecord, error) {
	return nil, nil
}
func (*emptyDurableImageTaskStore) DeleteExpired(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func TestAsyncImageRuntimeStartsConfiguredWorkerConcurrency(t *testing.T) {
	store := &emptyDurableImageTaskStore{asyncImageMemoryStore: &asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}}
	queue := &blockingAsyncImageQueue{release: make(chan struct{})}
	tasks := service.NewDurableImageTaskService(store, queue, nil, nil, nil, nil, nil, func() (*service.ImageResultUploader, bool) {
		return nil, true
	}, time.Hour, time.Minute)
	h := &AsyncImageHandler{tasks: tasks, cfg: &config.Config{AsyncImage: config.AsyncImageConfig{
		WorkerConcurrency: 60, RecoveryIntervalSeconds: 3600, CleanupIntervalMinutes: 60, StaleProcessingMinutes: 35,
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.runRuntime(ctx)
	require.Eventually(t, func() bool {
		queue.mu.Lock()
		defer queue.mu.Unlock()
		return queue.entered == 60
	}, time.Second, 10*time.Millisecond)
	cancel()
}

func TestAsyncImagePersistentWorkerStoresGatewayFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	created, err := tasks.Create(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9})
	require.NoError(t, err)

	router := gin.New()
	router.POST("/v1/images/generations", func(c *gin.Context) {
		c.JSON(http.StatusBadGateway, gin.H{"error": gin.H{"type": "upstream_error", "message": "mock failure"}})
	})
	h := NewAsyncImageHandler(tasks, nil)
	h.lookup = func(context.Context, int64) (*service.APIKey, error) {
		return &service.APIKey{ID: 9, UserID: 7, Key: "sk-worker-test"}, nil
	}
	h.AttachEngine(router)
	h.executePersistentTask(context.Background(), &service.ImageTaskRecord{
		ID: created.ID, UserID: 7, APIKeyID: 9, Method: http.MethodPost,
		RequestPath: "/v1/images/generations", ContentType: "application/json", RequestBody: []byte(`{"model":"gpt-image-2"}`),
	})

	got, err := tasks.Get(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9}, created.ID)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	require.Equal(t, http.StatusBadGateway, got.HTTPStatus)
	require.Contains(t, string(got.Error), "mock failure")
}
