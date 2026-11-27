package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ReadCompatibility inspects a diagnostic without decoding application state.
func (s *Store[EntityID, StateID]) ReadCompatibility(ctx context.Context, id string) (*transition.CompatibilityDiagnostic, error) {
	var result *transition.CompatibilityDiagnostic
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		record, err := loadHookExecution(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrExecutionNotActive
		}
		if err != nil {
			return err
		}
		if record.Compatibility != nil && !validDiagnostic(record, *record.Compatibility) {
			return fmt.Errorf("%w: invalid compatibility diagnostic", ErrUnsupportedSchema)
		}
		result = record.Compatibility
		return nil
	})
	return result, err
}

// BlockUnreadableExecution records a decode failure without requiring decoded IDs.
func (s *Store[EntityID, StateID]) BlockUnreadableExecution(ctx context.Context, id string) error {
	if err := transition.ValidateDurableID(id); err != nil {
		return err
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT e.record FROM rff_executions AS e
			JOIN rff_active AS a ON a.execution_id = e.id WHERE e.id = ?`, id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrExecutionNotActive
		}
		if err != nil {
			return fmt.Errorf("read unreadable execution: %w", err)
		}
		if _, err := loadHookExecution(ctx, tx, id); err != nil {
			return err
		}
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil || record.ID != id || !transition.SupportedExecutionSchema(record.SchemaVersion) {
			return fmt.Errorf("%w: invalid unreadable execution", ErrUnsupportedSchema)
		}
		if record.Compatibility != nil {
			return nil
		}
		_, err = s.decodeRecord(data)
		if err == nil {
			return nil
		}
		var compatibility *transition.CompatibilityError
		if !errors.As(err, &compatibility) || !validDiagnostic(record, compatibility.Diagnostic) {
			return err
		}
		var generation int64
		err = tx.QueryRowContext(ctx, `SELECT generation FROM rff_transition_leases WHERE execution_id = ?`, id).Scan(&generation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read unreadable execution fence: %w", err)
		}
		if err == nil {
			if generation <= 0 || generation == math.MaxInt64 {
				return fmt.Errorf("%w: unreadable execution fence cannot advance", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE rff_transition_leases SET generation = ? WHERE execution_id = ?`, generation+1, id); err != nil {
				return fmt.Errorf("fence unreadable execution: %w", err)
			}
		}
		record.Compatibility = &compatibility.Diagnostic
		return saveExecution(ctx, tx, id, record)
	})
}

func validDiagnostic(record diskRecord, diagnostic transition.CompatibilityDiagnostic) bool {
	if diagnostic.Name == "" || diagnostic.Version == "" {
		return false
	}
	if diagnostic.Check != "" && diagnostic.Check != transition.CompatibilityUnknownAttempt && diagnostic.Check != transition.CompatibilityDeadline {
		return false
	}
	switch diagnostic.Component {
	case transition.CompatibilityMigration:
		return true
	case transition.CompatibilityMachine, transition.CompatibilityClassifier, transition.CompatibilityHandler,
		transition.CompatibilityEntity, transition.CompatibilityState:
		return diagnostic.Name == record.MachineID && diagnostic.Version == record.MachineVersion
	case transition.CompatibilityInputCodec:
		return diagnostic.Name == record.Input.Codec.Name && diagnostic.Version == record.Input.Codec.Version
	case transition.CompatibilityEventCodec:
		return diagnostic.Name == record.Event.Codec.Name && diagnostic.Version == record.Event.Codec.Version
	default:
		return false
	}
}

// BlockHookCompatibility records a safe diagnostic without starting an attempt.
func (s *Store[EntityID, StateID]) BlockHookCompatibility(
	ctx context.Context, lease transition.HookLease, diagnostic transition.CompatibilityDiagnostic, clock func() time.Time,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		work, err := loadClaim(ctx, tx, lease, now)
		if err != nil {
			return err
		}
		if work.Progress.Completed {
			return transition.ErrInvalidHook
		}
		if work.Started && diagnostic.Check != transition.CompatibilityUnknownAttempt ||
			!work.Started && diagnostic.Check != "" {
			return transition.ErrInvalidHook
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read incompatible execution: %w", err)
		}
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil || record.ID != lease.ExecutionID || !transition.SupportedExecutionSchema(record.SchemaVersion) {
			return fmt.Errorf("%w: invalid incompatible execution", ErrUnsupportedSchema)
		}
		if !validDiagnostic(record, diagnostic) {
			return transition.ErrInvalidHook
		}
		record.Compatibility = &diagnostic
		fenceHook(&work, now)
		if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
			return err
		}
		return saveExecution(ctx, tx, lease.ExecutionID, record)
	})
}

// BlockExecutionCompatibility records a coordinator-detected compatibility failure.
func (s *Store[EntityID, StateID]) BlockExecutionCompatibility(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, diagnostic transition.CompatibilityDiagnostic,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		_, record, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if !validDiagnostic(record, diagnostic) {
			return transition.ErrInvalidHook
		}
		if record.Compatibility != nil && *record.Compatibility != diagnostic {
			return transition.ErrConflict
		}
		record.Compatibility = &diagnostic
		return saveExecution(ctx, tx, lease.ExecutionID, record)
	})
}

// BlockQuiescedCompatibility records a discovered failure only if the
// quiesced snapshot has not changed since validation.
func (s *Store[EntityID, StateID]) BlockQuiescedCompatibility(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time,
	expected transition.Execution[EntityID, StateID], expectedProgress *transition.HookProgress, diagnostic transition.CompatibilityDiagnostic,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if (expected.Status != transition.NeedsIntervention &&
		((expected.Status != transition.Waiting && expected.Status != transition.Running || expected.CommitOperationID != "") &&
			(expected.Status != transition.Aborting || !expected.RecoveryStarted))) || expected.Compatibility != nil ||
		expected.ID != lease.ExecutionID || expected.Entity != lease.Entity {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now, record, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		current, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode current execution: %w", err)
		}
		snapshot, err := s.encodeRecord(expected, record.Entity)
		if err != nil {
			return fmt.Errorf("encode expected execution: %w", err)
		}
		if !bytes.Equal(current, snapshot) {
			return transition.ErrConflict
		}
		if !validDiagnostic(record, diagnostic) {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil && !errors.Is(err, transition.ErrNoHookActivity) {
			return err
		}
		if errors.Is(err, transition.ErrNoHookActivity) != (expectedProgress == nil) {
			return transition.ErrConflict
		}
		if expectedProgress != nil {
			current := work.Progress
			current.Started, current.LeaseExpiresAt, current.BudgetUsed = work.Started, work.ExpiresAt, work.BudgetUsed
			if !reflect.DeepEqual(current, *expectedProgress) {
				return transition.ErrConflict
			}
			if record.Status != transition.NeedsIntervention && (work.Started || work.Progress.Completed) {
				return transition.ErrConflict
			}
			if work.Owner != "" && !work.Started {
				fenceHook(&work, now)
				if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
					return err
				}
			}
		}
		record.Compatibility = &diagnostic
		return saveExecution(ctx, tx, lease.ExecutionID, record)
	})
}

// RevalidateCompatibility updates a diagnostic only if the expected block
// remains current, preventing a stale revalidation from clearing a newer block.
func (s *Store[EntityID, StateID]) RevalidateCompatibility(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time,
	expected transition.CompatibilityDiagnostic, next *transition.CompatibilityDiagnostic,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		_, record, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if record.Compatibility == nil || *record.Compatibility != expected {
			return transition.ErrConflict
		}
		if record.Compatibility.Component == transition.CompatibilityMigration {
			return transition.ErrInvalidHook
		}
		if next != nil && !validDiagnostic(record, *next) {
			return transition.ErrInvalidHook
		}
		record.Compatibility = next
		return saveExecution(ctx, tx, lease.ExecutionID, record)
	})
}
