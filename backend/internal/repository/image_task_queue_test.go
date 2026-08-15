package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestImageTaskQueueRoundTrip(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	queue := NewImageTaskQueue(client, "test:image:ready")
	require.NoError(t, queue.Enqueue(context.Background(), "imgtask_abc"))
	require.NoError(t, queue.Enqueue(context.Background(), "imgtask_abc"))
	require.Equal(t, int64(1), client.LLen(context.Background(), "test:image:ready").Val())
	id, err := queue.Reserve(context.Background(), time.Second)
	require.NoError(t, err)
	require.Equal(t, "imgtask_abc", id)
	require.NoError(t, queue.Enqueue(context.Background(), "imgtask_abc"), "a reserved task can be recovered and enqueued again")
	require.Equal(t, int64(1), client.LLen(context.Background(), "test:image:ready").Val())
}
