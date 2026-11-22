package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// CommitAtomic executes a host-owned domain commit in the adapter's SQLite
// transaction, recording its receipt and optional notification intent together.
// The callback must check source state and invariants, use only tx for mutations,
// never commit, roll back, or retain tx, and return a safe definitive receipt.
// Rejection discards callback writes; errors roll back the entire operation.
// Identical operation replay returns the persisted receipt without calling commit.
func (s *Store[EntityID, StateID]) CommitAtomic(
	ctx context.Context,
	lease transition.TransitionLease[EntityID],
	clock func() time.Time,
	commit func(context.Context, *sql.Tx, transition.Execution[EntityID, StateID]) (transition.CommitReceipt, error),
) (transition.CommitReceipt, error) {
	var receipt transition.CommitReceipt
	if clock == nil || commit == nil {
		return receipt, errors.New("atomic commit requires a clock and callback")
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		stored, err := s.readControlExecution(ctx, tx, lease)
		if err != nil {
			return err
		}
		if stored.CommitReceipt != nil {
			receipt = *stored.CommitReceipt
			return nil
		}
		_, stored, err = s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if stored.Status != transition.Running || stored.CommitOperationID == "" || stored.RecoveryStarted {
			return transition.ErrInvalidHook
		}
		data, err := json.Marshal(stored)
		if err != nil {
			return fmt.Errorf("encode atomic commit execution: %w", err)
		}
		execution, err := s.decodeRecord(data)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `SAVEPOINT rff_domain_commit`); err != nil {
			return fmt.Errorf("start domain commit savepoint: %w", err)
		}
		receipt, err = commit(ctx, tx, execution)
		if err != nil {
			return err
		}
		if receipt.ExecutionID != stored.ID || receipt.OperationID != stored.CommitOperationID ||
			(receipt.Outcome == transition.CommitSucceeded && receipt.Failure != nil) ||
			(receipt.Outcome == transition.CommitDenied && receipt.Failure == nil) ||
			(receipt.Outcome != transition.CommitSucceeded && receipt.Outcome != transition.CommitDenied) {
			return transition.ErrInvalidHook
		}
		decisionTime := clock()
		if decisionTime.IsZero() {
			return transition.ErrInvalidLease
		}
		if err := s.validTransitionFence(ctx, tx, lease, decisionTime); err != nil {
			return err
		}
		if receipt.Outcome == transition.CommitDenied {
			if _, err := tx.ExecContext(ctx, `ROLLBACK TO rff_domain_commit`); err != nil {
				return fmt.Errorf("rollback rejected domain commit: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `RELEASE rff_domain_commit`); err != nil {
			return fmt.Errorf("release domain commit savepoint: %w", err)
		}
		stored.CommitReceipt = &receipt
		if receipt.Outcome == transition.CommitDenied {
			stored.Status = transition.CommitRejected
		} else {
			stored.Status = transition.Complete
			if stored.Notify {
				stored.NotificationIntentID = notificationID(stored.ID)
				work := notificationWork{Version: 1, IntentID: stored.NotificationIntentID, DueAt: decisionTime.UTC()}
				data, err := json.Marshal(work)
				if err != nil {
					return fmt.Errorf("encode notification work: %w", err)
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO rff_notification_work (execution_id, work) VALUES (?, ?)`, stored.ID, data); err != nil {
					return fmt.Errorf("create notification work: %w", err)
				}
			}
		}
		if err := saveExecution(ctx, tx, stored.ID, stored); err != nil {
			return err
		}
		if stored.Status == transition.Complete {
			return releaseReservation(ctx, tx, stored.ID)
		}
		return nil
	})
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	return receipt, nil
}
