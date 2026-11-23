package mysql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// EnterDeadlineIntervention records a classified elapsed overall deadline.
func (s *Store[EntityID, StateID]) EnterDeadlineIntervention(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, result transition.HookResult,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if result.Success || !validHookResult(result) {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now, execution, err := s.fencedExecutionForProgress(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status == transition.NeedsIntervention {
			if execution.Failure != nil && *execution.Failure == result.Failure && execution.FailureDisposition == result.Disposition {
				return nil
			}
			return transition.ErrConflict
		}
		if (execution.Status != transition.Aborting &&
			(execution.CommitOperationID != "" || execution.Status != transition.Waiting && execution.Status != transition.Running)) ||
			execution.effectiveDeadline().IsZero() || now.Before(execution.effectiveDeadline()) {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil && !errors.Is(err, transition.ErrNoHookActivity) {
			return err
		}
		if err == nil {
			if work.Started && !work.Progress.Completed {
				return transition.ErrBusy
			}
			if work.Progress.Completed && work.Progress.Result.Success &&
				(work.Progress.Dispatch.Kind == transition.HookEnter || work.Progress.Dispatch.Kind == transition.HookTransition) {
				return transition.ErrInvalidHook
			}
		}
		failure := result.Failure
		execution.Failure = &failure
		execution.FailureDisposition = result.Disposition
		execution.Status = transition.NeedsIntervention
		return saveExecution(ctx, tx, lease.ExecutionID, execution)
	})
}
