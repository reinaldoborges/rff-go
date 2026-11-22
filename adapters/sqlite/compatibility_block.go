package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func validDiagnostic(record diskRecord, diagnostic transition.CompatibilityDiagnostic) bool {
	if diagnostic.Name == "" || diagnostic.Version == "" {
		return false
	}
	switch diagnostic.Component {
	case transition.CompatibilityMachine, transition.CompatibilityClassifier, transition.CompatibilityHandler:
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
		if next != nil && !validDiagnostic(record, *next) {
			return transition.ErrInvalidHook
		}
		record.Compatibility = next
		return saveExecution(ctx, tx, lease.ExecutionID, record)
	})
}
