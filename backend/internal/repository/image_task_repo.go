package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type durableImageTaskStore struct {
	db *sql.DB
}

func NewDurableImageTaskStore(db *sql.DB) service.DurableImageTaskStore {
	return &durableImageTaskStore{db: db}
}

const imageTaskSelect = `
SELECT task_id, user_id, api_key_id, platform, target, model, method,
       request_path, content_type, request_headers, request_body, request_hash,
       COALESCE(idempotency_key, ''), status, http_status, result, error,
       hold_amount, hold_released_at, started_at, completed_at,
       expires_at, created_at
FROM image_tasks`

func (s *durableImageTaskStore) Create(ctx context.Context, task *service.ImageTaskRecord) error {
	if s == nil || s.db == nil || task == nil {
		return service.ErrImageTaskUnavailable
	}
	headers, err := json.Marshal(task.Headers)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO image_tasks (
    task_id, user_id, api_key_id, platform, target, model, method,
    request_path, content_type, request_headers, request_body, request_hash,
    idempotency_key, status, hold_amount, expires_at, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,''),$14,$15,$16,$17,$17)`,
		task.ID, task.UserID, task.APIKeyID, task.Platform, task.Target, task.Model,
		task.Method, task.RequestPath, task.ContentType, headers, task.RequestBody,
		task.RequestHash, task.IdempotencyKey, task.Status, task.HoldAmount,
		time.Unix(task.ExpiresAt, 0).UTC(), time.Unix(task.CreatedAt, 0).UTC(),
	)
	return err
}

func (s *durableImageTaskStore) Save(ctx context.Context, task *service.ImageTaskRecord, _ time.Duration) error {
	if s == nil || s.db == nil || task == nil {
		return service.ErrImageTaskUnavailable
	}
	completedAt := imageTaskUnixTime(task.CompletedAt)
	startedAt := imageTaskUnixTime(task.StartedAt)
	holdReleasedAt := imageTaskUnixTime(task.HoldReleasedAt)
	res, err := s.db.ExecContext(ctx, `
UPDATE image_tasks SET status=$2::text, http_status=$3, result=$4, error=$5,
	started_at=$6, completed_at=$7, hold_released_at=$8, expires_at=$9,
	request_body=CASE WHEN $2::text IN ('completed','failed') THEN ''::bytea ELSE request_body END,
	request_headers=CASE WHEN $2::text IN ('completed','failed') THEN '{}'::jsonb ELSE request_headers END,
    updated_at=NOW()
WHERE task_id=$1 AND status NOT IN ('completed','failed')`, task.ID, task.Status, task.HTTPStatus,
		nullJSON(task.Result), nullJSON(task.Error), startedAt, completedAt,
		holdReleasedAt, time.Unix(task.ExpiresAt, 0).UTC())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var exists bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM image_tasks WHERE task_id=$1)`, task.ID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return service.ErrImageTaskNotFound
		}
	}
	return nil
}

func (s *durableImageTaskStore) Get(ctx context.Context, id string) (*service.ImageTaskRecord, error) {
	return scanImageTask(s.db.QueryRowContext(ctx, imageTaskSelect+" WHERE task_id=$1", id))
}

func (s *durableImageTaskStore) GetByIdempotencyKey(ctx context.Context, owner service.ImageTaskOwner, key string) (*service.ImageTaskRecord, error) {
	return scanImageTask(s.db.QueryRowContext(ctx, imageTaskSelect+`
 WHERE user_id=$1 AND api_key_id=$2 AND idempotency_key=$3
 ORDER BY id DESC LIMIT 1`, owner.UserID, owner.APIKeyID, key))
}

func (s *durableImageTaskStore) ClaimQueued(ctx context.Context, id string) (*service.ImageTaskRecord, error) {
	return scanImageTask(s.db.QueryRowContext(ctx, `
UPDATE image_tasks
SET status='processing', started_at=NOW(), updated_at=NOW()
WHERE task_id=$1 AND status='queued'
RETURNING task_id, user_id, api_key_id, platform, target, model, method,
          request_path, content_type, request_headers, request_body, request_hash,
          COALESCE(idempotency_key, ''), status, http_status, result, error,
          hold_amount, hold_released_at, started_at, completed_at,
          expires_at, created_at`, id))
}

func (s *durableImageTaskStore) MarkQueued(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE image_tasks
SET status='queued', updated_at=NOW()
WHERE task_id=$1 AND status='pending'`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return service.ErrImageTaskNotFound
	}
	return nil
}

func (s *durableImageTaskStore) MarkHoldReleased(ctx context.Context, id string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE image_tasks
SET hold_released_at=COALESCE(hold_released_at,$2), updated_at=NOW()
WHERE task_id=$1`, id, at)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return service.ErrImageTaskNotFound
	}
	return nil
}

func (s *durableImageTaskStore) ListQueued(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT task_id FROM image_tasks
WHERE status='queued' ORDER BY created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *durableImageTaskStore) ListUnreleasedTerminalHolds(ctx context.Context, limit int) ([]*service.ImageTaskRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, imageTaskSelect+`
 WHERE status IN ('completed','failed') AND hold_amount > 0 AND hold_released_at IS NULL
 ORDER BY updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*service.ImageTaskRecord, 0)
	for rows.Next() {
		task, scanErr := scanImageTask(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *durableImageTaskStore) FailStalePending(ctx context.Context, cutoff time.Time, taskError json.RawMessage) ([]*service.ImageTaskRecord, error) {
	return s.failStaleTasks(ctx, "pending", cutoff, taskError)
}

func (s *durableImageTaskStore) FailStaleProcessing(ctx context.Context, cutoff time.Time, taskError json.RawMessage) ([]*service.ImageTaskRecord, error) {
	return s.failStaleTasks(ctx, "processing", cutoff, taskError)
}

func (s *durableImageTaskStore) failStaleTasks(ctx context.Context, status string, cutoff time.Time, taskError json.RawMessage) ([]*service.ImageTaskRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
UPDATE image_tasks SET status='failed', http_status=502, error=$2,
    completed_at=NOW(), updated_at=NOW(), request_body=''::bytea, request_headers='{}'::jsonb
WHERE status=$3 AND updated_at < $1
RETURNING task_id, user_id, api_key_id, platform, target, model, method,
          request_path, content_type, request_headers, request_body, request_hash,
          COALESCE(idempotency_key, ''), status, http_status, result, error,
          hold_amount, hold_released_at, started_at, completed_at,
          expires_at, created_at`, cutoff, taskError, status)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*service.ImageTaskRecord, 0)
	for rows.Next() {
		task, err := scanImageTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *durableImageTaskStore) DeleteExpired(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 1000
	}
	res, err := s.db.ExecContext(ctx, `
WITH expired AS (
    SELECT id FROM image_tasks WHERE expires_at < $1 ORDER BY expires_at LIMIT $2
)
DELETE FROM image_tasks WHERE id IN (SELECT id FROM expired)`, now, limit)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

type imageTaskScanner interface{ Scan(dest ...any) error }

func scanImageTask(row imageTaskScanner) (*service.ImageTaskRecord, error) {
	var task service.ImageTaskRecord
	var headers, result, taskErr []byte
	var holdReleased, started, completed sql.NullTime
	var expiresAt, createdAt time.Time
	err := row.Scan(
		&task.ID, &task.UserID, &task.APIKeyID, &task.Platform, &task.Target,
		&task.Model, &task.Method, &task.RequestPath, &task.ContentType, &headers,
		&task.RequestBody, &task.RequestHash, &task.IdempotencyKey, &task.Status,
		&task.HTTPStatus, &result, &taskErr, &task.HoldAmount, &holdReleased,
		&started, &completed, &expiresAt, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrImageTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(headers) > 0 {
		_ = json.Unmarshal(headers, &task.Headers)
	}
	if len(result) > 0 {
		task.Result = append(json.RawMessage(nil), result...)
	}
	if len(taskErr) > 0 {
		task.Error = append(json.RawMessage(nil), taskErr...)
	}
	task.CreatedAt, task.ExpiresAt = createdAt.Unix(), expiresAt.Unix()
	task.HoldReleasedAt = imageTaskNullTimeUnix(holdReleased)
	task.StartedAt = imageTaskNullTimeUnix(started)
	task.CompletedAt = imageTaskNullTimeUnix(completed)
	return &task, nil
}

func imageTaskNullTimeUnix(value sql.NullTime) *int64 {
	if !value.Valid {
		return nil
	}
	v := value.Time.Unix()
	return &v
}

func imageTaskUnixTime(value *int64) any {
	if value == nil {
		return nil
	}
	return time.Unix(*value, 0).UTC()
}

func nullJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

var _ service.DurableImageTaskStore = (*durableImageTaskStore)(nil)
