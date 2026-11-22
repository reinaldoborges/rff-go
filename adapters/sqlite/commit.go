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
				sum := sha256.Sum256([]byte("rff-notification:" + execution.ID))
				execution.NotificationIntentID = "rff-notification:" + hex.EncodeToString(sum[:])
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
