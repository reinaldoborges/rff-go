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

const (
	adapterSchemaVersion = 4
	schemaVersionDDL     = `CREATE TABLE rff_schema_version (version INTEGER NOT NULL)`
	executionsDDL        = `CREATE TABLE rff_executions (id TEXT PRIMARY KEY NOT NULL, entity BLOB NOT NULL, request_key TEXT NOT NULL, record BLOB NOT NULL, UNIQUE(entity, request_key))`
	activeDDL            = `CREATE TABLE rff_active (entity BLOB PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL UNIQUE REFERENCES rff_executions(id))`
	leasesDDL            = `CREATE TABLE rff_transition_leases (entity BLOB PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL REFERENCES rff_executions(id), owner TEXT NOT NULL, generation INTEGER NOT NULL, expires_at TEXT NOT NULL)`
	hookWorkDDL          = `CREATE TABLE rff_hook_work (execution_id TEXT PRIMARY KEY NOT NULL REFERENCES rff_executions(id), work BLOB NOT NULL)`
	notificationWorkDDL  = `CREATE TABLE rff_notification_work (execution_id TEXT PRIMARY KEY NOT NULL REFERENCES rff_executions(id), work BLOB NOT NULL)`
)

func (s *Store[EntityID, StateID]) initialize(ctx context.Context) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema inspection: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback schema inspection: %w", rollbackErr))
		}
	}()

	var schemaExists, recordsExist, activeExists, leasesExist, hookWorkExists, notificationWorkExists bool
	for name, exists := range map[string]*bool{
		"rff_schema_version":    &schemaExists,
		"rff_executions":        &recordsExist,
		"rff_active":            &activeExists,
		"rff_transition_leases": &leasesExist,
		"rff_hook_work":         &hookWorkExists,
		"rff_notification_work": &notificationWorkExists,
	} {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`, name).Scan(exists); err != nil {
			return fmt.Errorf("inspect schema: %w", err)
		}
	}
	if !schemaExists {
		if recordsExist || activeExists || leasesExist || hookWorkExists || notificationWorkExists {
			return fmt.Errorf("%w: adapter tables exist without a schema version", ErrUnsupportedSchema)
		}
		statements := []string{
			schemaVersionDDL,
			`INSERT INTO rff_schema_version (version) VALUES (4)`,
			executionsDDL,
			activeDDL,
			leasesDDL,
			hookWorkDDL,
			notificationWorkDDL,
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("initialize schema: %w", err)
			}
		}
	} else {
		if !recordsExist || !activeExists {
			return fmt.Errorf("%w: adapter tables are incomplete", ErrUnsupportedSchema)
		}
		for name, expected := range map[string]string{
			"rff_schema_version": schemaVersionDDL,
			"rff_executions":     executionsDDL,
			"rff_active":         activeDDL,
		} {
			var definition string
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect table definition: %w", ErrUnsupportedSchema, err)
			}
			if definition != expected {
				return fmt.Errorf("%w: incompatible table %s", ErrUnsupportedSchema, name)
			}
		}
		rows, err := tx.QueryContext(ctx, `SELECT version FROM rff_schema_version`)
		if err != nil {
			return fmt.Errorf("%w: read adapter version: %w", ErrUnsupportedSchema, err)
		}
		count := 0
		var schemaVersion int
		for rows.Next() {
			var version int
			if err := rows.Scan(&version); err != nil {
				return fmt.Errorf("%w: read adapter version: %w", ErrUnsupportedSchema, errors.Join(err, rows.Close()))
			}
			count++
			schemaVersion = version
			if version < 1 || version > adapterSchemaVersion {
				return errors.Join(fmt.Errorf("%w: adapter version %d", ErrUnsupportedSchema, version), rows.Close())
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("%w: read adapter version: %w", ErrUnsupportedSchema, err)
		}
		if count != 1 {
			return fmt.Errorf("%w: expected one adapter version", ErrUnsupportedSchema)
		}
		if err := validateRecords(ctx, tx); err != nil {
			return err
		}
		if schemaVersion == 1 {
			if leasesExist {
				return fmt.Errorf("%w: unexpected transition leases", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, leasesDDL); err != nil {
				return fmt.Errorf("migrate transition leases: %w", err)
			}
		} else {
			var definition string
			if !leasesExist {
				return fmt.Errorf("%w: transition leases table missing", ErrUnsupportedSchema)
			}
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_transition_leases'`).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect transition leases: %w", ErrUnsupportedSchema, err)
			}
			if definition != leasesDDL {
				return fmt.Errorf("%w: incompatible transition leases table", ErrUnsupportedSchema)
			}
			if err := validateLeases(ctx, tx); err != nil {
				return err
			}
		}
		if schemaVersion < 3 {
			if hookWorkExists {
				return fmt.Errorf("%w: unexpected hook work table", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, hookWorkDDL); err != nil {
				return fmt.Errorf("migrate hook work: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 3`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		} else {
			var definition string
			if !hookWorkExists {
				return fmt.Errorf("%w: hook work table missing", ErrUnsupportedSchema)
			}
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_hook_work'`).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect hook work: %w", ErrUnsupportedSchema, err)
			}
			if definition != hookWorkDDL {
				return fmt.Errorf("%w: incompatible hook work table", ErrUnsupportedSchema)
			}
			if err := validateHookWork(ctx, tx); err != nil {
				return err
			}
		}
		if schemaVersion < 4 {
			if notificationWorkExists {
				return fmt.Errorf("%w: unexpected notification work table", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, notificationWorkDDL); err != nil {
				return fmt.Errorf("migrate notification work: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 4`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		} else {
			var definition string
			if !notificationWorkExists {
				return fmt.Errorf("%w: notification work table missing", ErrUnsupportedSchema)
			}
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_notification_work'`).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect notification work: %w", ErrUnsupportedSchema, err)
			}
			if definition != notificationWorkDDL {
				return fmt.Errorf("%w: incompatible notification work table", ErrUnsupportedSchema)
			}
			if err := validateNotificationWork(ctx, tx); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema initialization: %w", err)
	}
	return nil
}

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
