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

// EnterHookIntervention records a policy-blocked hook failure under the current fence.
func (s *Store[EntityID, StateID]) EnterHookIntervention(ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now, execution, err := s.interventionExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status == transition.NeedsIntervention {
			return nil
		}
		if execution.CommitOperationID != "" || execution.Status != transition.Waiting && execution.Status != transition.Running {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil {
			return err
		}
		if !work.Progress.Completed {
			return transition.ErrBusy
		}
		if work.Progress.Result.Success && work.Progress.Dispatch.Kind != transition.HookRetry {
			return transition.ErrInvalidHook
		}
		var lastFailure *transition.HookResult
		for i := len(work.Progress.Attempts) - 1; i >= 0; i-- {
			result := work.Progress.Attempts[i].Result
			if result != nil && !result.Success {
				lastFailure = result
				break
			}
		}
		if lastFailure == nil {
			return transition.ErrInvalidHook
		}
		if work.Progress.Result.Success {
			if work.BudgetUsed < execution.Policy.MaxAttempts && now.Before(execution.Deadline) {
				return transition.ErrInvalidHook
			}
		} else if _, err := retryDelay(execution.Policy, work); err == nil && now.Before(execution.Deadline) {
			return transition.ErrInvalidHook
		} else if err != nil && !errors.Is(err, transition.ErrAttemptBudget) && !errors.Is(err, transition.ErrRetryIntervention) {
			return err
		}
		failure := lastFailure.Failure
		execution.Failure = &failure
		execution.FailureDisposition = lastFailure.Disposition
		execution.Status = transition.NeedsIntervention
		return saveIntervention(ctx, tx, lease.ExecutionID, execution)
	})
}

func (s *Store[EntityID, StateID]) interventionExecution(
	ctx context.Context, tx *sql.Tx, lease transition.TransitionLease[EntityID], clock func() time.Time,
) (time.Time, diskRecord, error) {
	now := clock()
	var execution diskRecord
	if now.IsZero() {
		return now, execution, transition.ErrInvalidLease
	}
	if err := s.validTransitionFence(ctx, tx, lease, now); err != nil {
		return now, execution, err
	}
	var data []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
		return now, execution, fmt.Errorf("read intervention execution: %w", err)
	}
	if err := json.Unmarshal(data, &execution); err != nil {
		return now, execution, fmt.Errorf("%w: decode intervention execution: %w", ErrUnsupportedSchema, err)
	}
	if execution.SchemaVersion != transition.CurrentSchemaVersion || execution.ID != lease.ExecutionID {
		return now, execution, fmt.Errorf("%w: invalid intervention execution", ErrUnsupportedSchema)
	}
	return now, execution, nil
}

func saveIntervention(ctx context.Context, tx *sql.Tx, id string, execution diskRecord) error {
	data, err := json.Marshal(execution)
	if err != nil {
		return fmt.Errorf("encode intervention execution: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rff_executions SET record = ? WHERE id = ?`, data, id); err != nil {
		return fmt.Errorf("record hook intervention: %w", err)
	}
	return nil
}
