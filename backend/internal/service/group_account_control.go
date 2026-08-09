package service

import (
	"context"
	"fmt"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	GroupAccountStateActive  = "active"
	GroupAccountStatePaused  = "paused"
	GroupAccountStateRemoved = "removed"
)

var (
	ErrGroupAccountNotFound      = infraerrors.NotFound("GROUP_ACCOUNT_NOT_FOUND", "account or group not found")
	ErrGroupAccountNotMember     = infraerrors.NotFound("GROUP_ACCOUNT_NOT_MEMBER", "account is not a member of this group")
	ErrGroupAccountLastAvailable = infraerrors.Conflict("GROUP_ACCOUNT_LAST_AVAILABLE", "this is the last available account in the group; confirmation is required")
)

// GroupAccountControl describes one account's membership in one group.
// A removed membership is represented by Member=false instead of a row.
type GroupAccountControl struct {
	AccountID                 int64  `json:"account_id"`
	GroupID                   int64  `json:"group_id"`
	State                     string `json:"state"`
	Member                    bool   `json:"member"`
	Enabled                   bool   `json:"enabled"`
	AccountAvailable          bool   `json:"account_available"`
	Priority                  int    `json:"priority"`
	AvailableMemberCount      int64  `json:"available_member_count"`
	EnabledMemberCount        int64  `json:"enabled_member_count"`
	ForceAccountCheckRequired bool   `json:"force_account_check_required"`
}

// GroupAccountControlRepository is intentionally separate from GroupRepository:
// this feature is also called by the internal SubPilot endpoint and should not
// force every existing repository test double to grow a new interface.
type GroupAccountControlRepository interface {
	Get(ctx context.Context, accountID, groupID int64) (*GroupAccountControl, error)
	SetState(ctx context.Context, accountID, groupID int64, state string, confirmLastAvailable bool) (*GroupAccountControl, error)
}

type GroupAccountControlService struct {
	repo GroupAccountControlRepository
}

func NewGroupAccountControlService(repo GroupAccountControlRepository) *GroupAccountControlService {
	return &GroupAccountControlService{repo: repo}
}

func (s *GroupAccountControlService) Get(ctx context.Context, accountID, groupID int64) (*GroupAccountControl, error) {
	if accountID <= 0 || groupID <= 0 {
		return nil, fmt.Errorf("account_id and group_id must be positive")
	}
	return s.repo.Get(ctx, accountID, groupID)
}

func (s *GroupAccountControlService) SetState(ctx context.Context, accountID, groupID int64, state string, confirmLastAvailable bool) (*GroupAccountControl, error) {
	if accountID <= 0 || groupID <= 0 {
		return nil, fmt.Errorf("account_id and group_id must be positive")
	}
	switch state {
	case GroupAccountStateActive, GroupAccountStatePaused, GroupAccountStateRemoved:
	default:
		return nil, fmt.Errorf("state must be active, paused, or removed")
	}
	return s.repo.SetState(ctx, accountID, groupID, state, confirmLastAvailable)
}
