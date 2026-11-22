package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ApplyHookControl applies a host-authorized pre-commit control.
func (s *Store[EntityID, StateID]) ApplyHookControl(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, control transition.HookControl,
) (transition.HookControlReceipt, error) {
	var receipt transition.HookControlReceipt
	if clock == nil {
		return receipt, transition.ErrInvalidLease
	}
	if control.ID == "" || control.Action != transition.RetryIntervention && control.Action != transition.AbortIntervention {
		return receipt, transition.ErrInvalidHook
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		entity, err := s.encodeEntity(lease.Entity)
		if err != nil {
			return err
		}
		var data []byte
		err = tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ? AND entity = ?`, lease.ExecutionID, entity).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return fmt.Errorf("read control execution: %w", err)
		}
		var stored diskRecord
		if err := json.Unmarshal(data, &stored); err != nil {
			return fmt.Errorf("%w: decode control execution: %w", ErrUnsupportedSchema, err)
		}
		if stored.SchemaVersion != transition.CurrentSchemaVersion || stored.ID != lease.ExecutionID || !bytes.Equal(stored.Entity, entity) {
			return fmt.Errorf("%w: invalid control execution", ErrUnsupportedSchema)
		}
		for _, prior := range stored.Controls {
			if prior.Control.ID == control.ID {
				if prior.Control != control {
					return transition.ErrConflict
				}
				receipt = prior
				return nil
			}
		}
		now, execution, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status != transition.NeedsIntervention || execution.Failure == nil {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil && !errors.Is(err, transition.ErrNoHookActivity) {
			return err
		}
		hasWork := err == nil
		if control.Action == transition.AbortIntervention {
			if execution.RecoveryStarted || execution.CommitOperationID != "" || execution.Failure == nil {
				return transition.ErrInvalidHook
			}
			if hasWork && work.Started && !work.Progress.Completed {
				return transition.ErrBusy
			}
			prepareRecovery(lease.ExecutionID, &work, hasWork, now, *execution.Failure)
			if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
				return err
			}
			execution.RecoveryStarted = true
		} else if execution.CommitOperationID != "" && !execution.RecoveryStarted {
			return transition.ErrInvalidHook
		} else if hasWork {
			if execution.RecoveryStarted && work.Progress.Dispatch.Kind != transition.HookRecover {
				return transition.ErrInvalidHook
			}
			if work.Progress.Completed && !work.Progress.Result.Success {
				if err := resumeFailedHook(lease.ExecutionID, &work, now); err != nil {
					return err
				}
			} else if work.Progress.Completed && work.Progress.Dispatch.Kind != transition.HookRetry {
				return transition.ErrInvalidHook
			}
			if !work.Progress.Completed {
				if work.Started {
					return transition.ErrBusy
				}
				work.Owner = ""
				work.ExpiresAt = time.Time{}
			}
			work.BudgetUsed = 0
			if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
				return err
			}
		} else if execution.RecoveryStarted {
			return transition.ErrInvalidHook
		}
		deadline, err := leaseExpiry(now, execution.Policy.OverallTimeout)
		if err != nil {
			return err
		}
		receipt = transition.HookControlReceipt{
			Control: control, AcceptedAt: now.UTC(), Deadline: deadline,
			Failure: execution.Failure, Disposition: execution.FailureDisposition,
		}
		execution.Controls = append(execution.Controls, receipt)
		execution.CycleDeadline = deadline
		execution.Failure = nil
		execution.FailureDisposition = ""
		if execution.RecoveryStarted {
			execution.Status = transition.Aborting
		} else {
			execution.Status = transition.Waiting
		}
		return saveExecution(ctx, tx, lease.ExecutionID, execution)
	})
	if err != nil {
		return transition.HookControlReceipt{}, err
	}
	return receipt, nil
}
