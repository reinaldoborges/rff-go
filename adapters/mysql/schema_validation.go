package mysql

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

func validateLeases(ctx context.Context, tx *sql.Tx) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT l.entity, l.execution_id, l.owner, l.generation, l.expires_at, e.entity
		FROM rff_transition_leases AS l LEFT JOIN rff_executions AS e ON e.id = l.execution_id`)
	if err != nil {
		return fmt.Errorf("%w: read transition leases: %w", ErrUnsupportedSchema, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close lease query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var entity, executionEntity []byte
		var executionID, owner, expiry string
		var generation int64
		if err := rows.Scan(&entity, &executionID, &owner, &generation, &expiry, &executionEntity); err != nil {
			return fmt.Errorf("%w: read transition lease: %w", ErrUnsupportedSchema, err)
		}
		_, parseErr := time.Parse(time.RFC3339Nano, expiry)
		if executionID == "" || owner == "" || generation <= 0 || parseErr != nil || executionEntity == nil || !bytes.Equal(entity, executionEntity) {
			return fmt.Errorf("%w: invalid transition lease", ErrUnsupportedSchema)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read transition leases: %w", ErrUnsupportedSchema, err)
	}
	return nil
}

func validateRecords(ctx context.Context, tx *sql.Tx) (err error) {
	active, err := tx.QueryContext(ctx, `SELECT entity, execution_id FROM rff_active LIMIT 0`)
	if err != nil {
		return fmt.Errorf("%w: read reservations: %w", ErrUnsupportedSchema, err)
	}
	if err := active.Close(); err != nil {
		return fmt.Errorf("%w: close reservation query: %w", ErrUnsupportedSchema, err)
	}
	var invalidReservation bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM rff_active AS a
		LEFT JOIN rff_executions AS e ON e.id = a.execution_id
		WHERE e.id IS NULL OR e.entity != a.entity
	)`).Scan(&invalidReservation); err != nil {
		return fmt.Errorf("%w: inspect reservations: %w", ErrUnsupportedSchema, err)
	}
	if invalidReservation {
		return fmt.Errorf("%w: inconsistent reservation", ErrUnsupportedSchema)
	}
	rows, err := tx.QueryContext(ctx, `SELECT e.id, e.entity, e.request_key, e.record, a.execution_id
		FROM rff_executions AS e LEFT JOIN rff_active AS a ON a.execution_id = e.id`)
	if err != nil {
		return fmt.Errorf("%w: read executions: %w", ErrUnsupportedSchema, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close execution query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id, key string
		var entity, data []byte
		var reservation sql.NullString
		if err := rows.Scan(&id, &entity, &key, &data, &reservation); err != nil {
			return fmt.Errorf("%w: read execution: %w", ErrUnsupportedSchema, err)
		}
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("%w: decode execution: %w", ErrUnsupportedSchema, err)
		}
		if record.SchemaVersion != transition.CurrentSchemaVersion || record.ID != id || record.Key != key || !bytes.Equal(record.Entity, entity) {
			return fmt.Errorf("%w: unsupported or inconsistent execution record", ErrUnsupportedSchema)
		}
		if record.Compatibility != nil && !validDiagnostic(record, *record.Compatibility) {
			return fmt.Errorf("%w: invalid compatibility diagnostic", ErrUnsupportedSchema)
		}
		if (record.OriginalRequest == nil) != (len(record.Migrations) == 0) {
			return fmt.Errorf("%w: inconsistent original request", ErrUnsupportedSchema)
		}
		seenMigrations := make(map[string]bool)
		for _, migration := range record.Migrations {
			if migration.Request.ID == "" || migration.Request.MachineID == "" || migration.Request.MachineVersion == "" ||
				len(migration.ParametersDigest) != 64 || len(migration.Request.Parameters) != 0 ||
				migration.AcceptedAt.IsZero() || seenMigrations[migration.Request.ID] {
				return fmt.Errorf("%w: invalid migration history", ErrUnsupportedSchema)
			}
			seenMigrations[migration.Request.ID] = true
		}
		terminal := record.Status == transition.Complete || record.Status == transition.Aborted
		if terminal == reservation.Valid {
			return fmt.Errorf("%w: execution and reservation disagree", ErrUnsupportedSchema)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read executions: %w", ErrUnsupportedSchema, err)
	}
	return nil
}
