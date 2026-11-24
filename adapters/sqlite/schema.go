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
	adapterSchemaVersion = 12
	schemaVersionDDL     = `CREATE TABLE rff_schema_version (version INTEGER NOT NULL)`
	executionsDDL        = `CREATE TABLE rff_executions (id TEXT PRIMARY KEY NOT NULL, entity BLOB NOT NULL, request_key TEXT NOT NULL, record BLOB NOT NULL, UNIQUE(entity, request_key))`
	activeDDL            = `CREATE TABLE rff_active (entity BLOB PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL UNIQUE REFERENCES rff_executions(id))`
	leasesDDL            = `CREATE TABLE rff_transition_leases (entity BLOB PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL REFERENCES rff_executions(id), owner TEXT NOT NULL, generation INTEGER NOT NULL, expires_at TEXT NOT NULL)`
	hookWorkDDL          = `CREATE TABLE rff_hook_work (execution_id TEXT PRIMARY KEY NOT NULL REFERENCES rff_executions(id), work BLOB NOT NULL)`
	notificationWorkDDL  = `CREATE TABLE rff_notification_work (execution_id TEXT PRIMARY KEY NOT NULL REFERENCES rff_executions(id), work BLOB NOT NULL)`
	timersDDL            = `CREATE TABLE rff_timers (id TEXT PRIMARY KEY NOT NULL, record BLOB NOT NULL)`
	inputsDDL            = `CREATE TABLE rff_inputs (id TEXT PRIMARY KEY NOT NULL, record BLOB NOT NULL)`
	continuationsDDL     = `CREATE TABLE rff_continuations (digest BLOB PRIMARY KEY NOT NULL, record BLOB NOT NULL)`
	successorsDDL        = `CREATE TABLE rff_successors (digest BLOB PRIMARY KEY NOT NULL, entity BLOB NOT NULL, execution_id BLOB NOT NULL UNIQUE, phase TEXT NOT NULL, record BLOB NOT NULL)`
	operationsDDL        = `CREATE TABLE rff_operations (execution_id TEXT NOT NULL REFERENCES rff_executions(id), operation_key BLOB NOT NULL, record BLOB NOT NULL, PRIMARY KEY(execution_id, operation_key))`
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

	var schemaExists, recordsExist, activeExists, leasesExist, hookWorkExists, notificationWorkExists, timersExist, inputsExist, continuationsExist, successorsExist, operationsExist bool
	for name, exists := range map[string]*bool{
		"rff_schema_version":    &schemaExists,
		"rff_executions":        &recordsExist,
		"rff_active":            &activeExists,
		"rff_transition_leases": &leasesExist,
		"rff_hook_work":         &hookWorkExists,
		"rff_notification_work": &notificationWorkExists,
		"rff_timers":            &timersExist,
		"rff_inputs":            &inputsExist,
		"rff_continuations":     &continuationsExist,
		"rff_successors":        &successorsExist,
		"rff_operations":        &operationsExist,
	} {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`, name).Scan(exists); err != nil {
			return fmt.Errorf("inspect schema: %w", err)
		}
	}
	if !schemaExists {
		if recordsExist || activeExists || leasesExist || hookWorkExists || notificationWorkExists || timersExist || inputsExist || continuationsExist || successorsExist || operationsExist {
			return fmt.Errorf("%w: adapter tables exist without a schema version", ErrUnsupportedSchema)
		}
		statements := []string{
			schemaVersionDDL,
			`INSERT INTO rff_schema_version (version) VALUES (12)`,
			executionsDDL,
			activeDDL,
			leasesDDL,
			hookWorkDDL,
			notificationWorkDDL,
			timersDDL,
			inputsDDL,
			continuationsDDL,
			successorsDDL,
			operationsDDL,
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("initialize schema: %w", err)
			}
		}
	} else {
		if !operationsExist {
			return fmt.Errorf("%w: tracked operation table missing", ErrUnsupportedSchema)
		}
		var operationDefinition string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_operations'`).Scan(&operationDefinition); err != nil || operationDefinition != operationsDDL {
			return fmt.Errorf("%w: incompatible tracked operation table", ErrUnsupportedSchema)
		}
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
		if schemaVersion < 5 {
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 5`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		}
		if schemaVersion < 6 {
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 6`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		}
		if schemaVersion < 7 {
			if timersExist {
				return fmt.Errorf("%w: unexpected timers table", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, timersDDL); err != nil {
				return fmt.Errorf("migrate timers: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 7`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		} else {
			var definition string
			if !timersExist {
				return fmt.Errorf("%w: timers table missing", ErrUnsupportedSchema)
			}
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_timers'`).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect timers: %w", ErrUnsupportedSchema, err)
			}
			if definition != timersDDL {
				return fmt.Errorf("%w: incompatible timers table", ErrUnsupportedSchema)
			}
			if err := validateTimers(ctx, tx, schemaVersion); err != nil {
				return err
			}
		}
		if schemaVersion < 8 {
			if inputsExist {
				return fmt.Errorf("%w: unexpected inputs table", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, inputsDDL); err != nil {
				return fmt.Errorf("migrate inputs: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 8`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		} else {
			var definition string
			if !inputsExist {
				return fmt.Errorf("%w: inputs table missing", ErrUnsupportedSchema)
			}
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_inputs'`).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect inputs: %w", ErrUnsupportedSchema, err)
			}
			if definition != inputsDDL {
				return fmt.Errorf("%w: incompatible inputs table", ErrUnsupportedSchema)
			}
			if err := validateInputs(ctx, tx); err != nil {
				return err
			}
		}
		if schemaVersion < 9 {
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 9`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		}
		if !continuationsExist {
			if schemaVersion >= 11 {
				return fmt.Errorf("%w: continuations table missing", ErrUnsupportedSchema)
			}
			if _, err := tx.ExecContext(ctx, continuationsDDL); err != nil {
				return fmt.Errorf("create continuations table: %w", err)
			}
		} else {
			var definition string
			if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_continuations'`).Scan(&definition); err != nil {
				return fmt.Errorf("%w: inspect continuations: %w", ErrUnsupportedSchema, err)
			}
			if definition != continuationsDDL {
				return fmt.Errorf("%w: incompatible continuations table", ErrUnsupportedSchema)
			}
		}
		if err := validateOperations(ctx, tx); err != nil {
			return err
		}
		if schemaVersion < 11 {
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 11`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
			}
		}
		if !successorsExist {
			return fmt.Errorf("%w: successor table missing", ErrUnsupportedSchema)
		}
		var definition string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rff_successors'`).Scan(&definition); err != nil || definition != successorsDDL {
			return fmt.Errorf("%w: incompatible successor table", ErrUnsupportedSchema)
		}
		if err := s.validateSuccessors(ctx, tx); err != nil {
			return err
		}
		if schemaVersion < adapterSchemaVersion {
			if _, err := tx.ExecContext(ctx, `UPDATE rff_schema_version SET version = 12`); err != nil {
				return fmt.Errorf("update adapter version: %w", err)
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
		if !transition.SupportedExecutionSchema(record.SchemaVersion) || record.SchemaVersion == 1 && record.Continuation != nil ||
			transition.ValidateContinuationSnapshot(record.Continuation) != nil || record.ID != id || record.Key != key || !bytes.Equal(record.Entity, entity) {
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
