package repository

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

var groupAccountControlColumns = []string{
	"account_exists", "group_exists", "member", "enabled", "account_available", "priority", "available_count", "enabled_count",
}

func TestGroupAccountControlRepositoryGetPaused(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery("SELECT").WithArgs(int64(11), int64(22)).
		WillReturnRows(sqlmock.NewRows(groupAccountControlColumns).AddRow(true, true, true, false, false, 3, 2, 4))

	repo := NewGroupAccountControlRepository(db)
	got, err := repo.Get(context.Background(), 11, 22)
	require.NoError(t, err)
	require.Equal(t, service.GroupAccountStatePaused, got.State)
	require.True(t, got.Member)
	require.False(t, got.Enabled)
	require.True(t, got.ForceAccountCheckRequired)
	require.Equal(t, int64(2), got.AvailableMemberCount)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGroupAccountControlRepositoryProtectsLastAvailable(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM groups").WithArgs(int64(22)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(22))
	mock.ExpectQuery("SELECT id FROM accounts").WithArgs(int64(11)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(11))
	mock.ExpectQuery("SELECT").WithArgs(int64(11), int64(22)).
		WillReturnRows(sqlmock.NewRows(groupAccountControlColumns).AddRow(true, true, true, true, true, 3, 1, 1))
	mock.ExpectRollback()

	repo := NewGroupAccountControlRepository(db)
	_, err = repo.SetState(context.Background(), 11, 22, service.GroupAccountStatePaused, false)
	require.ErrorIs(t, err, service.ErrGroupAccountLastAvailable)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGroupAccountControlRepositoryPauseRefreshesOutbox(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM groups").WithArgs(int64(22)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(22))
	mock.ExpectQuery("SELECT id FROM accounts").WithArgs(int64(11)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(11))
	mock.ExpectQuery("SELECT").WithArgs(int64(11), int64(22)).
		WillReturnRows(sqlmock.NewRows(groupAccountControlColumns).AddRow(true, true, true, true, true, 3, 2, 2))
	mock.ExpectExec("UPDATE account_groups SET enabled = FALSE").WithArgs(int64(11), int64(22)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO scheduler_outbox").
		WithArgs(service.SchedulerOutboxEventAccountGroupsChanged, sqlmock.AnyArg(), nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("SELECT").WithArgs(int64(11), int64(22)).
		WillReturnRows(sqlmock.NewRows(groupAccountControlColumns).AddRow(true, true, true, false, false, 3, 1, 1))

	repo := NewGroupAccountControlRepository(db)
	got, err := repo.SetState(context.Background(), 11, 22, service.GroupAccountStatePaused, false)
	require.NoError(t, err)
	require.Equal(t, service.GroupAccountStatePaused, got.State)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGroupAccountControlRepositoryActiveRestoresRemovedMembership(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM groups").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(22))
	mock.ExpectQuery("SELECT id FROM accounts").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(11))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(groupAccountControlColumns).AddRow(true, true, false, false, false, 50, 1, 1))
	mock.ExpectExec("INSERT INTO account_groups").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO scheduler_outbox").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(groupAccountControlColumns).AddRow(true, true, true, true, true, 50, 2, 2))

	repo := NewGroupAccountControlRepository(db)
	got, err := repo.SetState(context.Background(), 11, 22, service.GroupAccountStateActive, false)
	require.NoError(t, err)
	require.Equal(t, service.GroupAccountStateActive, got.State)
	require.NoError(t, mock.ExpectationsWereMet())
}
