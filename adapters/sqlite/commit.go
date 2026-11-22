package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
		stored, err := s.readControlExecution(ctx, tx, lease)
		if err != nil {
			return err
		}
		if receipt.ExecutionID != stored.ID || receipt.OperationID != stored.CommitOperationID {
			return transition.ErrConflict
		}
		if stored.CommitReceipt != nil {
			prior := stored.CommitReceipt
			if prior.ExecutionID == receipt.ExecutionID && prior.OperationID == receipt.OperationID && prior.Outcome == receipt.Outcome &&
				(prior.Failure == nil && receipt.Failure == nil || prior.Failure != nil && receipt.Failure != nil && *prior.Failure == *receipt.Failure) {
				return nil
			}
			return transition.ErrConflict
		}
		_, execution, err := s.fencedExecutionForProgress(ctx, tx, lease, clock)
		if err != nil {
			return err
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
				execution.NotificationIntentID = notificationID(execution.ID)
				execution.Status = transition.Notifying
			} else {
				execution.Status = transition.Complete
			}

		}
		if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
			return err
		}
		if execution.Status == transition.Complete {
			return releaseReservation(ctx, tx, lease.ExecutionID)
		}
		return nil
	})
}

func notificationID(executionID string) string {
	sum := sha256.Sum256([]byte("rff-notification:" + executionID))
	return "rff-notification:" + hex.EncodeToString(sum[:])
}
