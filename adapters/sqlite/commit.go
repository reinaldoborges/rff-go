package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RecordCommitReceipt durably records the host's final result under a fence.
func (s *Store[EntityID, StateID]) RecordCommitReceipt(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, receipt transition.CommitReceipt,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if receipt.ExecutionID == "" || receipt.OperationID == "" ||
		(receipt.Outcome == transition.CommitSucceeded && receipt.Failure != nil) ||
		(receipt.Outcome == transition.CommitDenied && receipt.Failure == nil) ||
		(receipt.Outcome != transition.CommitSucceeded && receipt.Outcome != transition.CommitDenied) {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		_, execution, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if receipt.ExecutionID != execution.ID || receipt.OperationID != execution.CommitOperationID {
			return transition.ErrConflict
		}
		if execution.CommitReceipt != nil {
			prior := execution.CommitReceipt
			if prior.ExecutionID == receipt.ExecutionID && prior.OperationID == receipt.OperationID && prior.Outcome == receipt.Outcome &&
				(prior.Failure == nil && receipt.Failure == nil || prior.Failure != nil && receipt.Failure != nil && *prior.Failure == *receipt.Failure) {
				return nil
			}
			return transition.ErrConflict
		}
		if execution.Status != transition.Running || execution.RecoveryStarted {
			return transition.ErrInvalidHook
		}
		execution.CommitReceipt = &receipt
		switch receipt.Outcome {
		case transition.CommitDenied:
			execution.Status = transition.CommitRejected
		case transition.CommitSucceeded:
			if execution.Notify {
				execution.Status = transition.Notifying
			} else {
				execution.Status = transition.Complete
			}
		}
		if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
			return err
		}
		if execution.Status == transition.Complete {
			result, err := tx.ExecContext(ctx, `DELETE FROM rff_active WHERE execution_id = ?`, lease.ExecutionID)
			if err != nil {
				return fmt.Errorf("release committed execution: %w", err)
			}
			count, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect committed reservation: %w", err)
			}
			if count != 1 {
				return transition.ErrStaleLease
			}
		}
		return nil
	})
}
