package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type groupAccountControlRepoStub struct {
	result    *GroupAccountControl
	state     string
	confirm   bool
	accountID int64
	groupID   int64
}

func (s *groupAccountControlRepoStub) Get(_ context.Context, accountID, groupID int64) (*GroupAccountControl, error) {
	s.accountID, s.groupID = accountID, groupID
	return s.result, nil
}

func (s *groupAccountControlRepoStub) SetState(_ context.Context, accountID, groupID int64, state string, confirm bool) (*GroupAccountControl, error) {
	s.accountID, s.groupID, s.state, s.confirm = accountID, groupID, state, confirm
	return s.result, nil
}

func TestGroupAccountControlServiceSetState(t *testing.T) {
	repo := &groupAccountControlRepoStub{result: &GroupAccountControl{State: GroupAccountStatePaused}}
	svc := NewGroupAccountControlService(repo)

	got, err := svc.SetState(context.Background(), 7, 9, GroupAccountStatePaused, true)
	require.NoError(t, err)
	require.Equal(t, GroupAccountStatePaused, got.State)
	require.Equal(t, int64(7), repo.accountID)
	require.Equal(t, int64(9), repo.groupID)
	require.Equal(t, GroupAccountStatePaused, repo.state)
	require.True(t, repo.confirm)
}

func TestGroupAccountControlServiceRejectsInvalidInput(t *testing.T) {
	svc := NewGroupAccountControlService(&groupAccountControlRepoStub{})

	_, err := svc.SetState(context.Background(), 1, 2, "disabled", false)
	require.ErrorContains(t, err, "active, paused, or removed")

	_, err = svc.Get(context.Background(), 0, 2)
	require.ErrorContains(t, err, "must be positive")
}
