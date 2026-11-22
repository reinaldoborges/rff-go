package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// FinishAbort durably completes successful recovery and releases its reservation.
func (s *Store[EntityID, StateID]) FinishAbort(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		_, execution, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status != transition.Aborting || !execution.RecoveryStarted {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil {
			return err
		}
		if work.Progress.Dispatch.Kind != transition.HookRecover || !work.Progress.Completed || !work.Progress.Result.Success {
			return transition.ErrInvalidHook
		}
		execution.Status = transition.Aborted
		if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM rff_active WHERE execution_id = ?`, lease.ExecutionID)
		if err != nil {
			return fmt.Errorf("release aborted execution: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect released reservation: %w", err)
		}
		if count != 1 {
			return transition.ErrStaleLease
		}
		return nil
	})
}
