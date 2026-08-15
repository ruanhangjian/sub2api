package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type durableImageTaskMemoryStore struct {
	mu    sync.Mutex
	tasks map[string]*ImageTaskRecord
}

func newDurableImageTaskMemoryStore() *durableImageTaskMemoryStore {
	return &durableImageTaskMemoryStore{tasks: map[string]*ImageTaskRecord{}}
}

func cloneDurableImageTask(task *ImageTaskRecord) *ImageTaskRecord {
	if task == nil {
		return nil
	}
	cp := *task
	cp.RequestBody = append([]byte(nil), task.RequestBody...)
	cp.Result = append(json.RawMessage(nil), task.Result...)
	cp.Error = append(json.RawMessage(nil), task.Error...)
	cp.Headers = cloneImageTaskHeaders(task.Headers)
	return &cp
}

func (s *durableImageTaskMemoryStore) Create(_ context.Context, task *ImageTaskRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[task.ID]; ok {
		return ErrImageTaskIdempotencyConflict
	}
	s.tasks[task.ID] = cloneDurableImageTask(task)
	return nil
}
func (s *durableImageTaskMemoryStore) Save(_ context.Context, task *ImageTaskRecord, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = cloneDurableImageTask(task)
	return nil
}
func (s *durableImageTaskMemoryStore) Get(_ context.Context, id string) (*ImageTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[id]
	if task == nil {
		return nil, ErrImageTaskNotFound
	}
	return cloneDurableImageTask(task), nil
}
func (s *durableImageTaskMemoryStore) GetByIdempotencyKey(_ context.Context, owner ImageTaskOwner, key string) (*ImageTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.tasks {
		if task.UserID == owner.UserID && task.APIKeyID == owner.APIKeyID && task.IdempotencyKey == key {
			return cloneDurableImageTask(task), nil
		}
	}
	return nil, ErrImageTaskNotFound
}
func (s *durableImageTaskMemoryStore) ClaimQueued(_ context.Context, id string) (*ImageTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[id]
	if task == nil || task.Status != ImageTaskStatusQueued {
		return nil, ErrImageTaskNotFound
	}
	task.Status = ImageTaskStatusProcessing
	now := time.Now().Unix()
	task.StartedAt = &now
	return cloneDurableImageTask(task), nil
}
func (s *durableImageTaskMemoryStore) MarkQueued(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[id]
	if task == nil || task.Status != ImageTaskStatusPending {
		return ErrImageTaskNotFound
	}
	task.Status = ImageTaskStatusQueued
	return nil
}
func (s *durableImageTaskMemoryStore) MarkHoldReleased(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[id]
	if task == nil {
		return ErrImageTaskNotFound
	}
	value := at.Unix()
	task.HoldReleasedAt = &value
	return nil
}
func (s *durableImageTaskMemoryStore) ListQueued(_ context.Context, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := []string{}
	for id, task := range s.tasks {
		if task.Status == ImageTaskStatusQueued && len(ids) < limit {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
func (s *durableImageTaskMemoryStore) ListUnreleasedTerminalHolds(_ context.Context, limit int) ([]*ImageTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*ImageTaskRecord{}
	for _, task := range s.tasks {
		if len(out) < limit && (task.Status == ImageTaskStatusCompleted || task.Status == ImageTaskStatusFailed) && task.HoldAmount > 0 && task.HoldReleasedAt == nil {
			out = append(out, cloneDurableImageTask(task))
		}
	}
	return out, nil
}
func (s *durableImageTaskMemoryStore) FailStalePending(_ context.Context, cutoff time.Time, taskError json.RawMessage) ([]*ImageTaskRecord, error) {
	return s.failStale(ImageTaskStatusPending, cutoff, taskError), nil
}
func (s *durableImageTaskMemoryStore) FailStaleProcessing(_ context.Context, cutoff time.Time, taskError json.RawMessage) ([]*ImageTaskRecord, error) {
	return s.failStale(ImageTaskStatusProcessing, cutoff, taskError), nil
}
func (s *durableImageTaskMemoryStore) failStale(status string, cutoff time.Time, taskError json.RawMessage) []*ImageTaskRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*ImageTaskRecord{}
	for _, task := range s.tasks {
		updatedAt := task.CreatedAt
		if task.StartedAt != nil {
			updatedAt = *task.StartedAt
		}
		if task.Status == status && time.Unix(updatedAt, 0).Before(cutoff) {
			task.Status = ImageTaskStatusFailed
			task.Error = taskError
			out = append(out, cloneDurableImageTask(task))
		}
	}
	return out
}
func (s *durableImageTaskMemoryStore) DeleteExpired(_ context.Context, now time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for id, task := range s.tasks {
		if deleted < limit && task.ExpiresAt < now.Unix() {
			delete(s.tasks, id)
			deleted++
		}
	}
	return deleted, nil
}

type imageTaskMemoryQueue struct {
	mu  sync.Mutex
	ids []string
}

func (q *imageTaskMemoryQueue) Enqueue(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ids = append(q.ids, id)
	return nil
}
func (q *imageTaskMemoryQueue) Reserve(_ context.Context, _ time.Duration) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ids) == 0 {
		return "", ErrImageTaskQueueEmpty
	}
	id := q.ids[0]
	q.ids = q.ids[1:]
	return id, nil
}

type imageTaskBillingRepo struct{ reserve, release int }

func (*imageTaskBillingRepo) Apply(context.Context, *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	return &UsageBillingApplyResult{}, nil
}
func (r *imageTaskBillingRepo) ReserveBatchImageBalance(context.Context, *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	r.reserve++
	return &BatchImageBalanceHoldResult{Applied: true}, nil
}
func (*imageTaskBillingRepo) CaptureBatchImageBalance(context.Context, *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	return &BatchImageBalanceHoldResult{}, nil
}
func (r *imageTaskBillingRepo) ReleaseBatchImageBalance(context.Context, *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	r.release++
	return &BatchImageBalanceHoldResult{Applied: true}, nil
}

type imageTaskAuthInvalidator struct{ users []int64 }

func (*imageTaskAuthInvalidator) InvalidateAuthCacheByKey(context.Context, string) {}
func (s *imageTaskAuthInvalidator) InvalidateAuthCacheByUserID(_ context.Context, id int64) {
	s.users = append(s.users, id)
}
func (*imageTaskAuthInvalidator) InvalidateAuthCacheByGroupID(context.Context, int64) {}

func TestDurableImageTaskSubmitIdempotencyAndHoldLifecycle(t *testing.T) {
	store, queue, billing, auth := newDurableImageTaskMemoryStore(), &imageTaskMemoryQueue{}, &imageTaskBillingRepo{}, &imageTaskAuthInvalidator{}
	svc := NewDurableImageTaskService(store, queue, billing, auth, nil, nil, &config.Config{}, func() (*ImageResultUploader, bool) { return nil, true }, time.Hour, time.Minute)
	in := ImageTaskSubmitInput{Owner: ImageTaskOwner{UserID: 7, APIKeyID: 9}, Platform: PlatformOpenAI, Target: "openai_images", Model: "gpt-image-2", Method: "POST", RequestPath: "/v1/images/generations", ContentType: "application/json", Body: []byte(`{"model":"gpt-image-2"}`), IdempotencyKey: "same", HoldAmount: .25}

	created, isNew, err := svc.Submit(context.Background(), in)
	require.NoError(t, err)
	require.True(t, isNew)
	require.Equal(t, ImageTaskStatusQueued, created.Status)
	require.Equal(t, 1, billing.reserve)
	require.Len(t, queue.ids, 1)
	replayed, isNew, err := svc.Submit(context.Background(), in)
	require.NoError(t, err)
	require.False(t, isNew)
	require.Equal(t, created.ID, replayed.ID)
	require.Equal(t, 1, billing.reserve)
	in.Body = []byte(`{"model":"different"}`)
	_, _, err = svc.Submit(context.Background(), in)
	require.ErrorIs(t, err, ErrImageTaskIdempotencyConflict)

	task, err := svc.Claim(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusProcessing, task.Status)
	require.Equal(t, 1, billing.release)
	require.NotNil(t, task.HoldReleasedAt)
	require.Equal(t, []int64{7, 7}, auth.users)
}

func TestImageTaskEstimateHoldUsesConfiguredResolutionPrice(t *testing.T) {
	p1, p2, p4 := .1, .25, .6
	svc := &ImageTaskService{billingService: newTestBillingService(), config: &config.Config{}}
	key := &APIKey{UserID: 7, Group: &Group{ID: 9, RateMultiplier: 2, ImagePrice1K: &p1, ImagePrice2K: &p2, ImagePrice4K: &p4}}
	require.InDelta(t, .2, svc.EstimateHold(context.Background(), key, "gpt-image-2", "1K", 1), 1e-10)
	require.InDelta(t, 1.0, svc.EstimateHold(context.Background(), key, "gpt-image-2", "2K", 2), 1e-10)
	require.InDelta(t, 1.2, svc.EstimateHold(context.Background(), key, "gpt-image-2", "4K", 1), 1e-10)
}

func TestDurableImageTaskRecoverReleasesTerminalHold(t *testing.T) {
	store, queue, billing := newDurableImageTaskMemoryStore(), &imageTaskMemoryQueue{}, &imageTaskBillingRepo{}
	task := &ImageTaskRecord{
		ID: "imgtask_terminal", UserID: 7, APIKeyID: 9, Status: ImageTaskStatusFailed,
		RequestHash: "hash", HoldAmount: .25, CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}
	require.NoError(t, store.Create(context.Background(), task))
	svc := NewDurableImageTaskService(store, queue, billing, nil, nil, nil, &config.Config{}, func() (*ImageResultUploader, bool) {
		return nil, true
	}, time.Hour, time.Minute)

	require.NoError(t, svc.Recover(context.Background(), 0, 100))
	require.Equal(t, 1, billing.release)
	got, err := store.Get(context.Background(), task.ID)
	require.NoError(t, err)
	require.NotNil(t, got.HoldReleasedAt)

	require.NoError(t, svc.Recover(context.Background(), 0, 100))
	require.Equal(t, 1, billing.release, "recovery must release a hold idempotently")
}

func TestDurableImageTaskCompleteFailsClosedWhenStorageUnavailable(t *testing.T) {
	store, queue := newDurableImageTaskMemoryStore(), &imageTaskMemoryQueue{}
	task := &ImageTaskRecord{
		ID: "imgtask_storage", UserID: 7, APIKeyID: 9, Status: ImageTaskStatusProcessing,
		CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}
	require.NoError(t, store.Create(context.Background(), task))
	svc := NewDurableImageTaskService(store, queue, nil, nil, nil, nil, &config.Config{}, func() (*ImageResultUploader, bool) {
		return nil, false
	}, time.Hour, time.Minute)
	b64Result := json.RawMessage(`{"data":[{"b64_json":"large-base64"}]}`)

	require.NoError(t, svc.Complete(context.Background(), task.ID, 200, b64Result))
	got, err := store.Get(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusFailed, got.Status)
	require.Contains(t, string(got.Error), "image_storage_unavailable")
	require.Empty(t, got.Result, "durable tasks must never persist raw image base64")
}

var _ DurableImageTaskStore = (*durableImageTaskMemoryStore)(nil)
var _ ImageTaskQueue = (*imageTaskMemoryQueue)(nil)
