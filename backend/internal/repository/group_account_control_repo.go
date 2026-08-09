package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type groupAccountControlRepository struct {
	db *sql.DB
}

func NewGroupAccountControlRepository(db *sql.DB) service.GroupAccountControlRepository {
	return &groupAccountControlRepository{db: db}
}

func (r *groupAccountControlRepository) Get(ctx context.Context, accountID, groupID int64) (*service.GroupAccountControl, error) {
	return getGroupAccountControl(ctx, r.db, accountID, groupID)
}

func (r *groupAccountControlRepository) SetState(
	ctx context.Context,
	accountID, groupID int64,
	state string,
	confirmLastAvailable bool,
) (*service.GroupAccountControl, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockGroupAccountControlSubjects(ctx, tx, accountID, groupID); err != nil {
		return nil, err
	}

	current, err := getGroupAccountControl(ctx, tx, accountID, groupID)
	if err != nil {
		return nil, err
	}
	if state == service.GroupAccountStatePaused && !current.Member {
		return nil, service.ErrGroupAccountNotMember
	}

	if state != service.GroupAccountStateActive && current.AccountAvailable && current.AvailableMemberCount <= 1 && !confirmLastAvailable {
		return nil, service.ErrGroupAccountLastAvailable.WithMetadata(map[string]string{
			"account_id":             fmt.Sprintf("%d", accountID),
			"group_id":               fmt.Sprintf("%d", groupID),
			"available_member_count": fmt.Sprintf("%d", current.AvailableMemberCount),
		})
	}

	switch state {
	case service.GroupAccountStateActive:
		_, err = tx.ExecContext(ctx, `
			INSERT INTO account_groups (account_id, group_id, priority, enabled, created_at)
			VALUES ($1, $2, 50, TRUE, NOW())
			ON CONFLICT (account_id, group_id) DO UPDATE SET enabled = TRUE
		`, accountID, groupID)
	case service.GroupAccountStatePaused:
		_, err = tx.ExecContext(ctx, `
			UPDATE account_groups SET enabled = FALSE
			WHERE account_id = $1 AND group_id = $2
		`, accountID, groupID)
	case service.GroupAccountStateRemoved:
		_, err = tx.ExecContext(ctx, `
			DELETE FROM account_groups WHERE account_id = $1 AND group_id = $2
		`, accountID, groupID)
	}
	if err != nil {
		return nil, err
	}

	payload := map[string]any{"group_ids": []int64{groupID}}
	if err := enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountGroupsChanged, &accountID, nil, payload); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return r.Get(ctx, accountID, groupID)
}

func lockGroupAccountControlSubjects(ctx context.Context, tx *sql.Tx, accountID, groupID int64) error {
	var lockedGroupID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM groups WHERE id = $1 AND deleted_at IS NULL FOR UPDATE
	`, groupID).Scan(&lockedGroupID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return service.ErrGroupAccountNotFound
		}
		return err
	}

	var lockedAccountID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM accounts WHERE id = $1 AND deleted_at IS NULL FOR UPDATE
	`, accountID).Scan(&lockedAccountID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return service.ErrGroupAccountNotFound
		}
		return err
	}
	return nil
}

type groupAccountControlQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getGroupAccountControl(ctx context.Context, q groupAccountControlQueryer, accountID, groupID int64) (*service.GroupAccountControl, error) {
	var (
		accountExists        bool
		groupExists          bool
		member               bool
		enabled              bool
		accountAvailable     bool
		priority             int
		availableMemberCount int64
		enabledMemberCount   int64
	)

	err := q.QueryRowContext(ctx, `
		SELECT
			EXISTS(SELECT 1 FROM accounts WHERE id = $1 AND deleted_at IS NULL),
			EXISTS(SELECT 1 FROM groups WHERE id = $2 AND deleted_at IS NULL),
			EXISTS(SELECT 1 FROM account_groups WHERE account_id = $1 AND group_id = $2),
			COALESCE((SELECT enabled FROM account_groups WHERE account_id = $1 AND group_id = $2), FALSE),
			EXISTS(
				SELECT 1
				FROM account_groups ag
				JOIN accounts a ON a.id = ag.account_id
				WHERE ag.account_id = $1 AND ag.group_id = $2
				  AND ag.enabled = TRUE
				  AND a.deleted_at IS NULL
				  AND a.status = 'active'
				  AND a.schedulable = TRUE
				  AND (a.expires_at IS NULL OR a.expires_at > NOW() OR a.auto_pause_on_expired = FALSE)
				  AND (a.rate_limit_reset_at IS NULL OR a.rate_limit_reset_at <= NOW())
				  AND (a.overload_until IS NULL OR a.overload_until <= NOW())
				  AND (a.temp_unschedulable_until IS NULL OR a.temp_unschedulable_until <= NOW())
			),
			COALESCE((SELECT priority FROM account_groups WHERE account_id = $1 AND group_id = $2), 50),
			(
				SELECT COUNT(*)
				FROM account_groups ag
				JOIN accounts a ON a.id = ag.account_id
				WHERE ag.group_id = $2
				  AND ag.enabled = TRUE
				  AND a.deleted_at IS NULL
				  AND a.status = 'active'
				  AND a.schedulable = TRUE
				  AND (a.expires_at IS NULL OR a.expires_at > NOW() OR a.auto_pause_on_expired = FALSE)
				  AND (a.rate_limit_reset_at IS NULL OR a.rate_limit_reset_at <= NOW())
				  AND (a.overload_until IS NULL OR a.overload_until <= NOW())
				  AND (a.temp_unschedulable_until IS NULL OR a.temp_unschedulable_until <= NOW())
			),
			(
				SELECT COUNT(*)
				FROM account_groups ag
				JOIN accounts a ON a.id = ag.account_id
				WHERE ag.group_id = $2 AND ag.enabled = TRUE AND a.deleted_at IS NULL
			)
	`, accountID, groupID).Scan(
		&accountExists,
		&groupExists,
		&member,
		&enabled,
		&accountAvailable,
		&priority,
		&availableMemberCount,
		&enabledMemberCount,
	)
	if err != nil {
		return nil, err
	}
	if !accountExists || !groupExists {
		return nil, service.ErrGroupAccountNotFound
	}

	state := service.GroupAccountStateRemoved
	if member {
		state = service.GroupAccountStatePaused
		if enabled {
			state = service.GroupAccountStateActive
		}
	}
	return &service.GroupAccountControl{
		AccountID:                 accountID,
		GroupID:                   groupID,
		State:                     state,
		Member:                    member,
		Enabled:                   enabled,
		AccountAvailable:          accountAvailable,
		Priority:                  priority,
		AvailableMemberCount:      availableMemberCount,
		EnabledMemberCount:        enabledMemberCount,
		ForceAccountCheckRequired: state != service.GroupAccountStateActive,
	}, nil
}
