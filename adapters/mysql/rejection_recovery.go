package mysql

import (
	"context"
	"database/sql"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// StartRejectedRecovery begins recovery authorized by a definitive host receipt.
func (s *Store[EntityID, StateID]) StartRejectedRecovery(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now, execution, err := s.fencedExecutionForProgress(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitDenied ||
			execution.CommitReceipt.Failure == nil {
			return transition.ErrInvalidHook
		}
		if execution.RecoveryStarted {
			return nil
		}
		if execution.Status != transition.CommitRejected {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil {
			return err
		}
		if !work.Progress.Completed || !work.Progress.Result.Success ||
			(work.Progress.Dispatch.Kind != transition.HookEnter && work.Progress.Dispatch.Kind != transition.HookTransition) {
			return transition.ErrInvalidHook
		}
		deadline, err := leaseExpiry(now, execution.Policy.OverallTimeout)
		if err != nil {
			return err
		}
		prepareRecovery(lease.ExecutionID, &work, true, now, *execution.CommitReceipt.Failure)
		if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
			return err
		}
		execution.RecoveryStarted = true
		execution.RejectionRecoveryDeadline = deadline
		execution.CycleDeadline = deadline
		execution.Status = transition.Aborting
		return saveExecution(ctx, tx, lease.ExecutionID, execution)
	})
}
