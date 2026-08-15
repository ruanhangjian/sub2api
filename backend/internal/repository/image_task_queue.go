package repository

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

type imageTaskQueue struct {
	rdb            *redis.Client
	readyKey       string
	inflightPrefix string
}

const imageTaskQueueInflightTTL = 48 * time.Hour

var imageTaskEnqueueScript = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
  redis.call("LPUSH", KEYS[2], ARGV[1])
  return 1
end
return 0
`)

var imageTaskReserveScript = redis.NewScript(`
local task = redis.call("RPOP", KEYS[1])
if not task then return nil end
redis.call("DEL", KEYS[2] .. task)
return task
`)

func NewImageTaskQueue(rdb *redis.Client, readyKey string) service.ImageTaskQueue {
	if strings.TrimSpace(readyKey) == "" {
		readyKey = "image_task:queue:ready"
	}
	return &imageTaskQueue{rdb: rdb, readyKey: readyKey, inflightPrefix: readyKey + ":inflight:"}
}

func ProvideImageTaskQueue(rdb *redis.Client, cfg *config.Config) service.ImageTaskQueue {
	key := ""
	if cfg != nil {
		key = cfg.AsyncImage.QueueReadyKey
	}
	return NewImageTaskQueue(rdb, key)
}

func (q *imageTaskQueue) Enqueue(ctx context.Context, id string) error {
	if q == nil || q.rdb == nil || !validImageTaskID(id) {
		return service.ErrImageTaskUnavailable
	}
	_, err := imageTaskEnqueueScript.Run(ctx, q.rdb, []string{q.inflightPrefix + id, q.readyKey}, id, imageTaskQueueInflightTTL.Milliseconds()).Int()
	return err
}

func (q *imageTaskQueue) Reserve(ctx context.Context, timeout time.Duration) (string, error) {
	if q == nil || q.rdb == nil {
		return "", service.ErrImageTaskUnavailable
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		raw, err := imageTaskReserveScript.Run(ctx, q.rdb, []string{q.readyKey, q.inflightPrefix}).Result()
		if err == nil {
			id, ok := raw.(string)
			if !ok || !validImageTaskID(id) {
				return "", service.ErrImageTaskUnavailable
			}
			return id, nil
		}
		if !errors.Is(err, redis.Nil) {
			return "", err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", service.ErrImageTaskQueueEmpty
		}
		wait := 200 * time.Millisecond
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}

func validImageTaskID(id string) bool {
	id = strings.TrimSpace(id)
	return strings.HasPrefix(id, "imgtask_") && len(id) > len("imgtask_")
}

var _ service.ImageTaskQueue = (*imageTaskQueue)(nil)
