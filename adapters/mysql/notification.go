package mysql

import (
	"context"
	"database/sql"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// CompleteNotificationHandoff records durable outbox handoff and releases the reservation.
func (s *Store[EntityID, StateID]) CompleteNotificationHandoff(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, intentID string,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if intentID == "" {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		_, execution, err := s.fencedExecutionForProgress(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status != transition.Notifying || !execution.Notify || execution.NotificationIntentID != intentID ||
			execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded {
			return transition.ErrInvalidHook
		}
		execution.Status = transition.Complete
		if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
			return err
		}
		return releaseReservation(ctx, tx, lease.ExecutionID, clock())
	})
}
