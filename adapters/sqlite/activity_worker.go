package sqlite

import (
	"bytes"
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

// CheckHookActivities validates the durable inventory without a claim.
func (s *Store[EntityID, StateID]) CheckHookActivities(ctx context.Context) error {
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT execution_id FROM rff_hook_work ORDER BY execution_id`)
		if err != nil {
			return fmt.Errorf("list hook work for integrity check: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return errors.Join(fmt.Errorf("read hook work for integrity check: %w", err), rows.Close())
			}
			ids = append(ids, id)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("list hook work for integrity check: %w", err)
		}
		for _, id := range ids {
			if _, err := loadHook(ctx, tx, id); err != nil {
				return err
			}
		}
		return nil
	})
}

// ClaimHook claims a due, unstarted hook without consuming an attempt. A corrupt
// projection can hide earlier work until a later candidate check or integrity scan observes it.
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
		var id string
		err = tx.QueryRowContext(ctx, `SELECT w.execution_id FROM rff_hook_work AS w
			JOIN rff_active AS a ON a.execution_id = w.execution_id
			JOIN rff_executions AS e ON e.id = w.execution_id
			WHERE w.completed = 0 AND w.started = 0
				AND (w.available_at_seconds < ? OR (w.available_at_seconds = ? AND w.available_at_nanoseconds <= ?))
				AND (w.lease_owner_present = 0 OR w.lease_expires_at_seconds < ? OR
					(w.lease_expires_at_seconds = ? AND w.lease_expires_at_nanoseconds <= ?))
				AND ((w.dispatch_kind = 'recover' AND e.selection_status = 'Aborting') OR
					(w.dispatch_kind != 'recover' AND e.selection_status IN ('Waiting', 'Running')))
				AND e.selection_compatibility = 0
				AND NOT EXISTS (SELECT 1 FROM rff_integrity_findings f INDEXED BY rff_integrity_open_lookup
					WHERE f.kind = 'hook' AND f.locator = w.row_id AND f.resolved_at IS NULL)
				AND NOT EXISTS (SELECT 1 FROM rff_integrity_findings f INDEXED BY rff_integrity_open_lookup
					WHERE f.kind = 'execution' AND f.locator = e.row_id AND f.resolved_at IS NULL)
				AND NOT EXISTS (SELECT 1 FROM rff_integrity_findings f INDEXED BY rff_integrity_open_lookup
					WHERE f.kind = 'reservation' AND f.locator = a.row_id AND f.resolved_at IS NULL)
			ORDER BY w.execution_id LIMIT 1`,
			now.Unix(), now.Unix(), now.Nanosecond(), now.Unix(), now.Unix(), now.Nanosecond()).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrNoHookActivity
		}
		if err != nil {
			return fmt.Errorf("select eligible hook work: %w", err)
		}
		work, err := loadHook(ctx, tx, id)
		if err != nil {
			return err
		}
		execution, err := loadHookExecution(ctx, tx, id)
		if err != nil {
			return err
		}
		if (work.Progress.Dispatch.Kind == transition.HookRecover && execution.Status != transition.Aborting) ||
			(work.Progress.Dispatch.Kind != transition.HookRecover && execution.Status != transition.Waiting && execution.Status != transition.Running) ||
			execution.Compatibility != nil {
			return recordExecutionFinding(ctx, tx, id, "projection_mismatch", "selection")
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

func loadHookExecution(ctx context.Context, tx *sql.Tx, id string) (diskRecord, error) {
	var execution diskRecord
	var rowID, revision int64
	var reservationID sql.NullInt64
	var data, entity, reservationEntity []byte
	var status string
	var compatibility int
	err := tx.QueryRowContext(ctx, `SELECT e.row_id, e.revision, e.record, e.selection_status, e.selection_compatibility,
		e.entity, a.row_id, a.entity FROM rff_executions e
		LEFT JOIN rff_active a ON a.execution_id = e.id WHERE e.id = ?`, id).Scan(
		&rowID, &revision, &data, &status, &compatibility, &entity, &reservationID, &reservationEntity,
	)
	if err != nil {
		return execution, fmt.Errorf("read hook execution: %w", err)
	}
	if rowID <= 0 || revision <= 0 {
		return execution, fmt.Errorf("%w: invalid hook execution locator or revision", ErrUnsupportedSchema)
	}
	quarantined, err := executionQuarantined(ctx, tx, rowID)
	if err != nil {
		return execution, fmt.Errorf("check execution quarantine: %w", err)
	}
	if quarantined {
		return execution, transition.ErrIntegrity
	}
	if err := json.Unmarshal(data, &execution); err != nil ||
		!transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != id {
		findingErr := recordExecutionFinding(ctx, tx, id, "invalid_execution", "record")
		return execution, errors.Join(err, findingErr)
	}
	if !bytes.Equal(entity, execution.Entity) {
		return execution, recordExecutionFinding(ctx, tx, id, "invalid_execution", "record")
	}
	if status != string(execution.Status) || (compatibility == 1) != (execution.Compatibility != nil) ||
		compatibility < 0 || compatibility > 1 {
		return execution, recordExecutionFinding(ctx, tx, id, "projection_mismatch", "selection")
	}
	if reservationID.Valid {
		quarantined, err := reservationQuarantined(ctx, tx, reservationID.Int64)
		if err != nil {
			return execution, fmt.Errorf("check reservation quarantine: %w", err)
		}
		if quarantined {
			return execution, transition.ErrIntegrity
		}
		if !bytes.Equal(entity, reservationEntity) {
			return execution, recordReservationFinding(ctx, tx, reservationID.Int64)
		}
	} else if execution.Status != transition.Complete && execution.Status != transition.Aborted {
		return execution, recordExecutionFinding(ctx, tx, id, "invalid_execution", "record")
	}
	return execution, nil
}

func loadClaim(ctx context.Context, tx *sql.Tx, lease transition.HookLease, now time.Time) (hookWork, error) {
	if err := transition.ValidateDurableID(lease.ExecutionID); err != nil {
		return hookWork{}, errors.Join(err, transition.ErrStaleActivityLease)
	}
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
		if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
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
