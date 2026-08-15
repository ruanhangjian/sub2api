//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDurableImageTaskStoreCompletedLifecycle(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{
		Email:        fmt.Sprintf("image-task-%d@example.com", time.Now().UnixNano()),
		PasswordHash: "hash",
		Balance:      100,
	})
	apiKey := mustCreateApiKey(t, client, &service.APIKey{
		UserID: user.ID,
		Key:    "sk-image-task-" + uuid.NewString(),
		Name:   "image-task",
	})

	now := time.Now().UTC().Truncate(time.Second)
	task := &service.ImageTaskRecord{
		ID:             "imgtask_" + uuid.NewString(),
		UserID:         user.ID,
		APIKeyID:       apiKey.ID,
		Platform:       service.PlatformOpenAI,
		Target:         "openai_images",
		Model:          "gpt-image-2",
		Method:         "POST",
		RequestPath:    "/v1/images/generations",
		ContentType:    "application/json",
		Headers:        map[string]string{"x-request-id": "request-1"},
		RequestBody:    []byte(`{"model":"gpt-image-2","size":"4K"}`),
		RequestHash:    "request-hash",
		IdempotencyKey: uuid.NewString(),
		Status:         service.ImageTaskStatusPending,
		HoldAmount:     0.04,
		CreatedAt:      now.Unix(),
		ExpiresAt:      now.Add(24 * time.Hour).Unix(),
	}

	store := NewDurableImageTaskStore(integrationDB)
	require.NoError(t, store.Create(ctx, task))
	require.NoError(t, store.MarkQueued(ctx, task.ID))

	claimed, err := store.ClaimQueued(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusProcessing, claimed.Status)
	require.Equal(t, task.RequestBody, claimed.RequestBody)

	completedAt := now.Add(time.Minute).Unix()
	holdReleasedAt := now.Add(30 * time.Second).Unix()
	claimed.Status = service.ImageTaskStatusCompleted
	claimed.HTTPStatus = 200
	claimed.Result = json.RawMessage(`{"data":[{"url":"/v1/images/tasks/result.png"}]}`)
	claimed.CompletedAt = &completedAt
	claimed.HoldReleasedAt = &holdReleasedAt
	require.NoError(t, store.Save(ctx, claimed, 24*time.Hour))

	saved, err := store.Get(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusCompleted, saved.Status)
	require.Equal(t, 200, saved.HTTPStatus)
	require.JSONEq(t, string(claimed.Result), string(saved.Result))
	require.Empty(t, saved.RequestBody)
	require.Empty(t, saved.Headers)
	require.Equal(t, &completedAt, saved.CompletedAt)
	require.Equal(t, &holdReleasedAt, saved.HoldReleasedAt)
}
