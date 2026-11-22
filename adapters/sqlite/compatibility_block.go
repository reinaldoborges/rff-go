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

// ReadCompatibility inspects a diagnostic without decoding application state.
func (s *Store[EntityID, StateID]) ReadCompatibility(ctx context.Context, id string) (*transition.CompatibilityDiagnostic, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, transition.ErrExecutionNotActive
	}
	if err != nil {
		return nil, fmt.Errorf("read compatibility diagnostic: %w", err)
	}
	var record diskRecord
	if err := json.Unmarshal(data, &record); err != nil || record.SchemaVersion != transition.CurrentSchemaVersion || record.ID != id {
		return nil, fmt.Errorf("%w: invalid compatibility execution", ErrUnsupportedSchema)
	}
	if record.Compatibility != nil && !validDiagnostic(record, *record.Compatibility) {
		return nil, fmt.Errorf("%w: invalid compatibility diagnostic", ErrUnsupportedSchema)
	}
	return record.Compatibility, nil
}

// BlockUnreadableExecution records a decode failure without requiring decoded IDs.
func (s *Store[EntityID, StateID]) BlockUnreadableExecution(ctx context.Context, id string) error {
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
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil || record.ID != id || record.SchemaVersion != transition.CurrentSchemaVersion {
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
		work, err := loadClaim(ctx, tx, lease, clock())
		if err != nil {
			return err
		}
		if work.Started || work.Progress.Completed {
			return transition.ErrInvalidHook
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read incompatible execution: %w", err)
		}
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil || record.ID != lease.ExecutionID || record.SchemaVersion != transition.CurrentSchemaVersion {
			return fmt.Errorf("%w: invalid incompatible execution", ErrUnsupportedSchema)
		}
		if !validDiagnostic(record, diagnostic) {
			return transition.ErrInvalidHook
		}
		record.Compatibility = &diagnostic
		work.Owner, work.ExpiresAt = "", time.Time{}
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
