package service

import "time"

type AccountGroup struct {
	AccountID int64
	GroupID   int64
	Priority  int
	// SchedulingDisabled is intentionally zero-value enabled for backwards
	// compatibility with old snapshots and test fixtures.
	SchedulingDisabled bool
	CreatedAt          time.Time

	Account *Account
	Group   *Group
}
