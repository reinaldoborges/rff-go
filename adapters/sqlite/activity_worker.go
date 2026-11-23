package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ClaimHook claims a due, unstarted hook without consuming an attempt.
func (s *Store[EntityID, StateID]) ClaimHook(ctx context.Context, owner string, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	var lease transition.HookLease
	if owner == "" || clock == nil || ttl <= 0 {
		return lease, transition.ErrInvalidLease
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		expiry, err := leaseExpiry(now, ttl)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT w.execution_id, w.work, e.record FROM rff_hook_work AS w
			JOIN rff_active AS a ON a.execution_id = w.execution_id
			JOIN rff_executions AS e ON e.id = w.execution_id ORDER BY w.execution_id`)
		if err != nil {
			return fmt.Errorf("list hook work: %w", err)
		}
		var id string
		var work hookWork
		for rows.Next() {
			var candidateID string
			var data, executionData []byte
			if err := rows.Scan(&candidateID, &data, &executionData); err != nil {
				return errors.Join(fmt.Errorf("read hook work: %w", err), rows.Close())
			}
			var candidate hookWork
			if err := json.Unmarshal(data, &candidate); err != nil {
				return errors.Join(fmt.Errorf("%w: decode hook work: %w", ErrUnsupportedSchema, err), rows.Close())
			}
			if err := validateWork(candidateID, candidate); err != nil {
				return errors.Join(err, rows.Close())
			}
			var execution diskRecord
			if err := json.Unmarshal(executionData, &execution); err != nil || !transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != candidateID {
				return errors.Join(fmt.Errorf("%w: invalid hook execution", ErrUnsupportedSchema), err, rows.Close())
			}
			if (candidate.Progress.Dispatch.Kind == transition.HookRecover && execution.Status != transition.Aborting) ||
				(candidate.Progress.Dispatch.Kind != transition.HookRecover && execution.Status != transition.Waiting && execution.Status != transition.Running) {
				continue
			}
			if execution.Compatibility != nil || candidate.Progress.Completed || candidate.Started || now.Before(candidate.Progress.AvailableAt) ||
				(candidate.Owner != "" && now.Before(candidate.ExpiresAt)) {
				continue
			}
			id, work = candidateID, candidate
			break
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("list hook work: %w", err)
		}
		if id == "" {
			return transition.ErrNoHookActivity
		}
		if work.Generation == math.MaxInt64 {
			return fmt.Errorf("hook lease generation exhausted for execution %q", id)
		}
		work.Generation++
		work.Owner = owner
		work.ExpiresAt = expiry
		if err := saveHook(ctx, tx, id, work); err != nil {
			return err
		}
		lease = transition.HookLease{
			ExecutionID: id, ActivityID: work.Progress.ActivityID, Owner: owner,
			Generation: uint64(work.Generation), ExpiresAt: expiry,
		}
		return nil
	})
	if err != nil {
		return transition.HookLease{}, err
	}
	return lease, nil
}

func loadClaim(ctx context.Context, tx *sql.Tx, lease transition.HookLease, now time.Time) (hookWork, error) {
	work, err := loadHook(ctx, tx, lease.ExecutionID)
	if errors.Is(err, transition.ErrNoHookActivity) {
		return hookWork{}, transition.ErrStaleActivityLease
	}
	if err != nil {
		return hookWork{}, err
	}
	if err := validHookLease(ctx, tx, lease, work, now); err != nil {
		return hookWork{}, err
	}
	return work, nil
}

// InspectHook reads the execution snapshot and hook bound to a current claim.
func (s *Store[EntityID, StateID]) InspectHook(ctx context.Context, lease transition.HookLease, clock func() time.Time) (transition.Execution[EntityID, StateID], transition.HookCall, error) {
	var data []byte
	var call transition.HookCall
	if clock == nil {
		return transition.Execution[EntityID, StateID]{}, call, transition.ErrInvalidLease
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		work, err := loadClaim(ctx, tx, lease, clock())
		if err != nil {
			return err
		}
		if work.Progress.Completed {
			return transition.ErrStaleActivityLease
		}
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read hook execution: %w", err)
		}
		call.Kind = work.Progress.Dispatch.Kind
		call.Failure = work.Progress.Dispatch.Failure
		return nil
	})
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, transition.HookCall{}, err
	}
	execution, err := s.decodeRecord(data)
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, transition.HookCall{}, err
	}
	if execution.ID != lease.ExecutionID {
		return transition.Execution[EntityID, StateID]{}, transition.HookCall{}, fmt.Errorf("%w: hook execution ID mismatch", ErrUnsupportedSchema)
	}
	return execution, call, nil
}

// StartHook durably counts the attempt before exposing it to a worker.
func (s *Store[EntityID, StateID]) StartHook(ctx context.Context, lease transition.HookLease, clock func() time.Time) (transition.StartedHook, error) {
	var started transition.StartedHook
	if clock == nil {
		return started, transition.ErrInvalidLease
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		work, err := loadClaim(ctx, tx, lease, now)
		if err != nil {
			return err
		}
		if work.Progress.Completed {
			return transition.ErrStaleActivityLease
		}
		if err := ensureCompatibleExecution(ctx, tx, lease.ExecutionID); err != nil {
			return err
		}
		if work.Started {
			last := work.Progress.Attempts[len(work.Progress.Attempts)-1]
			started = transition.StartedHook{Attempt: last.Metadata, StartedAt: last.StartedAt}
			return nil
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read hook execution: %w", err)
		}
		var execution diskRecord
		if err := json.Unmarshal(data, &execution); err != nil {
			return fmt.Errorf("%w: decode hook execution: %w", ErrUnsupportedSchema, err)
		}
		if !transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != lease.ExecutionID {
			return fmt.Errorf("%w: invalid hook execution", ErrUnsupportedSchema)
		}
		if (work.Progress.Dispatch.Kind == transition.HookRecover && execution.Status != transition.Aborting) ||
			(work.Progress.Dispatch.Kind != transition.HookRecover && execution.Status != transition.Waiting && execution.Status != transition.Running) {
			return transition.ErrInvalidHook
		}
		deadline := execution.effectiveDeadline()
		if execution.Policy.MaxAttempts == 0 || execution.Policy.AttemptTimeout <= 0 || execution.Policy.OverallTimeout <= 0 || deadline.IsZero() {
			return fmt.Errorf("%w: invalid execution timing", transition.ErrCompatibility)
		}
		if !now.Before(deadline) {
			return transition.ErrTransitionDeadline
		}
		if work.BudgetUsed >= execution.Policy.MaxAttempts {
			return transition.ErrAttemptBudget
		}
		metadata := fsm.AttemptMetadata{
			ExecutionID: lease.ExecutionID, Number: uint(len(work.Progress.Attempts) + 1),
			LeaseGeneration: lease.Generation, RetryCycle: work.Progress.Dispatch.RetryCycle,
			ActivityID: lease.ActivityID, ActivityIdempotencyKey: lease.ActivityID,
		}
		work.Started = true
		work.BudgetUsed++
		work.Progress.Attempts = append(work.Progress.Attempts, transition.HookAttempt{
			Metadata: metadata, Kind: work.Progress.Dispatch.Kind, StartedAt: now.UTC(),
		})
		started = transition.StartedHook{Attempt: metadata, StartedAt: now.UTC()}
		if execution.Status != transition.Aborting {
			execution.Status = transition.Running
		}
		data, err = json.Marshal(execution)
		if err != nil {
			return fmt.Errorf("encode hook execution: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_executions SET record = ? WHERE id = ?`, data, lease.ExecutionID); err != nil {
			return fmt.Errorf("start hook execution: %w", err)
		}
		return saveHook(ctx, tx, lease.ExecutionID, work)
	})
	if err != nil {
		return transition.StartedHook{}, err
	}
	return started, nil
}

// RenewHook extends a current, unexpired worker claim.
func (s *Store[EntityID, StateID]) RenewHook(ctx context.Context, lease transition.HookLease, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	if clock == nil || ttl <= 0 {
		return transition.HookLease{}, transition.ErrInvalidLease
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		expiry, err := leaseExpiry(now, ttl)
		if err != nil {
			return err
		}
		work, err := loadClaim(ctx, tx, lease, now)
		if err != nil {
			return err
		}
		if work.Progress.Completed {
			return transition.ErrStaleActivityLease
		}
		work.ExpiresAt = expiry
		lease.ExpiresAt = expiry
		return saveHook(ctx, tx, lease.ExecutionID, work)
	})
	if err != nil {
		return transition.HookLease{}, err
	}
	return lease, nil
}

// ReportHook records a safe outcome only for the current started claim.
func (s *Store[EntityID, StateID]) ReportHook(ctx context.Context, lease transition.HookLease, result transition.HookResult, clock func() time.Time) error {
	if clock == nil || !validHookResult(result) {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		work, err := loadClaim(ctx, tx, lease, now)
		if err != nil {
			return err
		}
		if !work.Started {
			return transition.ErrInvalidHook
		}
		if work.Progress.Completed {
			if work.Progress.Result == result {
				return nil
			}
			return transition.ErrConflict
		}
		last := &work.Progress.Attempts[len(work.Progress.Attempts)-1]
		last.CompletedAt = now.UTC()
		last.Result = &result
		work.Progress.Completed = true
		work.Progress.Result = result
		return saveHook(ctx, tx, lease.ExecutionID, work)
	})
}
