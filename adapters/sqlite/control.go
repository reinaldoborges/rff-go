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

// ApplyCheckedHookControl checks behavior atomically before a new control.
func (s *Store[EntityID, StateID]) ApplyCheckedHookControl(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, control transition.HookControl,
	check func(transition.Execution[EntityID, StateID], transition.HookCall) error,
) (transition.HookControlReceipt, error) {
	if check == nil {
		return transition.HookControlReceipt{}, transition.ErrInvalidRequest
	}
	var receipt transition.HookControlReceipt
	var compatibilityErr error
	if clock == nil {
		return receipt, transition.ErrInvalidLease
	}
	if control.ID == "" || control.Action != transition.RetryIntervention && control.Action != transition.AbortIntervention {
		return receipt, transition.ErrInvalidHook
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		stored, err := s.readControlExecution(ctx, tx, lease)
		if err != nil {
			return err
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
		if execution.Compatibility != nil {
			return transition.ErrCompatibility
		}
		{
			data, err := json.Marshal(execution)
			if err != nil {
				return fmt.Errorf("encode control execution: %w", err)
			}
			decoded, err := s.decodeRecord(data)
			if err == nil {
				kind := transition.HookRetry
				if control.Action == transition.AbortIntervention || execution.RecoveryStarted {
					kind = transition.HookRecover
				}
				err = check(decoded, transition.HookCall{Kind: kind, Failure: execution.Failure})
			}
			now = clock()
			if now.IsZero() {
				return transition.ErrInvalidLease
			}
			if fenceErr := s.validTransitionFence(ctx, tx, lease, now); fenceErr != nil {
				return fenceErr
			}
			if err != nil {
				var incompatible *transition.CompatibilityError
				if !errors.As(err, &incompatible) || !validDiagnostic(execution, incompatible.Diagnostic) {
					return err
				}
				execution.Compatibility = &incompatible.Diagnostic
				if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
					return err
				}
				compatibilityErr = err
				return nil
			}
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
	if compatibilityErr != nil {
		return transition.HookControlReceipt{}, compatibilityErr
	}
	return receipt, nil
}
