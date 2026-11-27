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
	adapterSchemaVersion = 16
	schemaVersionDDL     = `CREATE TABLE rff_schema_version (version INTEGER NOT NULL)`
	executionsDDL        = `CREATE TABLE rff_executions (row_id INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE CHECK(length(CAST(id AS BLOB)) BETWEEN 1 AND 256), entity BLOB NOT NULL, request_key TEXT NOT NULL, record BLOB NOT NULL, selection_status TEXT NOT NULL DEFAULT 'Waiting', selection_compatibility INTEGER NOT NULL DEFAULT 0 CHECK(selection_compatibility IN (0, 1)), revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0), UNIQUE(entity, request_key))`
	activeDDL            = `CREATE TABLE rff_active (row_id INTEGER PRIMARY KEY AUTOINCREMENT, entity BLOB NOT NULL UNIQUE, execution_id TEXT NOT NULL UNIQUE REFERENCES rff_executions(id) CHECK(length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256), revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0))`
	leasesDDL            = `CREATE TABLE rff_transition_leases (entity BLOB PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL REFERENCES rff_executions(id) CHECK(length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256), owner TEXT NOT NULL, generation INTEGER NOT NULL, expires_at TEXT NOT NULL)`
	hookWorkDDL          = `CREATE TABLE rff_hook_work (
		row_id INTEGER PRIMARY KEY AUTOINCREMENT,
		execution_id TEXT NOT NULL UNIQUE REFERENCES rff_executions(id) CHECK(length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256),
		work BLOB NOT NULL,
		dispatch_kind TEXT NOT NULL CHECK(dispatch_kind IN ('enter', 'transition', 'retry', 'recover')),
		started INTEGER NOT NULL CHECK(started IN (0, 1)),
		completed INTEGER NOT NULL CHECK(completed IN (0, 1)),
		available_at_seconds INTEGER NOT NULL,
		available_at_nanoseconds INTEGER NOT NULL CHECK(available_at_nanoseconds BETWEEN 0 AND 999999999),
		lease_owner_present INTEGER NOT NULL CHECK(lease_owner_present IN (0, 1)),
		lease_expires_at_seconds INTEGER,
		lease_expires_at_nanoseconds INTEGER,
		revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
		CHECK((lease_owner_present = 0 AND lease_expires_at_seconds IS NULL AND lease_expires_at_nanoseconds IS NULL) OR
			(lease_owner_present = 1 AND lease_expires_at_seconds IS NOT NULL AND lease_expires_at_nanoseconds IS NOT NULL AND lease_expires_at_nanoseconds BETWEEN 0 AND 999999999))
	)`
	executionHookIndexDDL     = `CREATE INDEX rff_execution_hook_state_lookup ON rff_executions(selection_status, selection_compatibility, id)`
	hookReadyIndexDDL         = `CREATE INDEX rff_hook_ready_lookup ON rff_hook_work(available_at_seconds, available_at_nanoseconds, execution_id) WHERE completed = 0 AND started = 0`
	timerDeadlineIndexDDL     = `CREATE INDEX rff_timer_deadline_lookup ON rff_timers(deadline_seconds, deadline_nanoseconds, id) WHERE completed = 0 AND compatibility_blocked = 0`
	executionRevisionDDL      = `CREATE TRIGGER rff_execution_revision AFTER UPDATE OF record, selection_status, selection_compatibility ON rff_executions BEGIN UPDATE rff_executions SET revision = OLD.revision + 1 WHERE row_id = NEW.row_id; END`
	executionRevisionGuardDDL = `CREATE TRIGGER rff_execution_revision_guard BEFORE UPDATE OF revision ON rff_executions WHEN NEW.revision != OLD.revision + 1 BEGIN SELECT RAISE(ABORT, 'execution revision is monotonic'); END`
	executionLocatorDDL       = `CREATE TRIGGER rff_execution_locator BEFORE UPDATE OF row_id ON rff_executions WHEN NEW.row_id != OLD.row_id BEGIN SELECT RAISE(ABORT, 'execution locator is immutable'); END`
	executionAllocationDDL    = `CREATE TRIGGER rff_execution_allocation AFTER INSERT ON rff_executions WHEN EXISTS (SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_history_lookup WHERE kind = 'execution' AND locator = NEW.row_id) BEGIN SELECT RAISE(ABORT, 'execution locator was previously used'); END`
	activeRevisionDDL         = `CREATE TRIGGER rff_active_revision AFTER UPDATE OF entity, execution_id ON rff_active BEGIN UPDATE rff_active SET revision = OLD.revision + 1 WHERE row_id = NEW.row_id; END`
	activeRevisionGuardDDL    = `CREATE TRIGGER rff_active_revision_guard BEFORE UPDATE OF revision ON rff_active WHEN NEW.revision != OLD.revision + 1 BEGIN SELECT RAISE(ABORT, 'reservation revision is monotonic'); END`
	activeLocatorDDL          = `CREATE TRIGGER rff_active_locator BEFORE UPDATE OF row_id ON rff_active WHEN NEW.row_id != OLD.row_id BEGIN SELECT RAISE(ABORT, 'reservation locator is immutable'); END`
	activeAllocationDDL       = `CREATE TRIGGER rff_active_allocation AFTER INSERT ON rff_active WHEN EXISTS (SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_history_lookup WHERE kind = 'reservation' AND locator = NEW.row_id) BEGIN SELECT RAISE(ABORT, 'reservation locator was previously used'); END`
	hookRevisionDDL           = `CREATE TRIGGER rff_hook_revision AFTER UPDATE OF work, dispatch_kind, started, completed, available_at_seconds, available_at_nanoseconds, lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds ON rff_hook_work BEGIN UPDATE rff_hook_work SET revision = OLD.revision + 1 WHERE row_id = NEW.row_id; END`
	hookRevisionGuardDDL      = `CREATE TRIGGER rff_hook_revision_guard BEFORE UPDATE OF revision ON rff_hook_work WHEN NEW.revision != OLD.revision + 1 BEGIN SELECT RAISE(ABORT, 'hook revision is monotonic'); END`
	hookLocatorDDL            = `CREATE TRIGGER rff_hook_locator BEFORE UPDATE OF row_id ON rff_hook_work WHEN NEW.row_id != OLD.row_id BEGIN SELECT RAISE(ABORT, 'hook locator is immutable'); END`
	hookAllocationDDL         = `CREATE TRIGGER rff_hook_allocation AFTER INSERT ON rff_hook_work WHEN EXISTS (SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_history_lookup WHERE kind = 'hook' AND locator = NEW.row_id) BEGIN SELECT RAISE(ABORT, 'hook locator was previously used'); END`
	notificationWorkDDL       = `CREATE TABLE rff_notification_work (execution_id TEXT PRIMARY KEY NOT NULL REFERENCES rff_executions(id) CHECK(length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256), work BLOB NOT NULL)`
	timersDDL                 = `CREATE TABLE rff_timers (row_id INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE CHECK(length(CAST(id AS BLOB)) BETWEEN 1 AND 256), deadline_seconds INTEGER NOT NULL, deadline_nanoseconds INTEGER NOT NULL CHECK(deadline_nanoseconds BETWEEN 0 AND 999999999), completed INTEGER NOT NULL CHECK(completed IN (0, 1)), compatibility_blocked INTEGER NOT NULL CHECK(compatibility_blocked IN (0, 1)), lease_expires_seconds INTEGER, lease_expires_nanoseconds INTEGER, record BLOB NOT NULL, revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0), CHECK((lease_expires_seconds IS NULL AND lease_expires_nanoseconds IS NULL) OR (lease_expires_seconds IS NOT NULL AND lease_expires_nanoseconds IS NOT NULL AND lease_expires_nanoseconds BETWEEN 0 AND 999999999)))`
	timerRevisionDDL          = `CREATE TRIGGER rff_timer_revision AFTER UPDATE OF id, deadline_seconds, deadline_nanoseconds, completed, compatibility_blocked, lease_expires_seconds, lease_expires_nanoseconds, record ON rff_timers BEGIN UPDATE rff_timers SET revision = OLD.revision + 1 WHERE row_id = NEW.row_id; END`
	timerRevisionGuardDDL     = `CREATE TRIGGER rff_timer_revision_guard BEFORE UPDATE OF revision ON rff_timers WHEN NEW.revision != OLD.revision + 1 BEGIN SELECT RAISE(ABORT, 'timer revision is monotonic'); END`
	timerLocatorDDL           = `CREATE TRIGGER rff_timer_locator BEFORE UPDATE OF row_id ON rff_timers WHEN NEW.row_id != OLD.row_id BEGIN SELECT RAISE(ABORT, 'timer locator is immutable'); END`
	timerAllocationDDL        = `CREATE TRIGGER rff_timer_allocation AFTER INSERT ON rff_timers WHEN EXISTS (SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_history_lookup WHERE kind = 'timer' AND locator = NEW.row_id) BEGIN SELECT RAISE(ABORT, 'timer locator was previously used'); END`
	integrityFindingsDDL      = `CREATE TABLE rff_integrity_findings (finding_id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN ('timer', 'hook', 'execution', 'reservation')), locator INTEGER NOT NULL CHECK(locator > 0), category TEXT NOT NULL CHECK(category IN ('invalid_timer', 'projection_mismatch', 'invalid_execution', 'invalid_reservation', 'invalid_hook')), field TEXT NOT NULL CHECK(field IN ('record', 'selection', 'active')), detected_at TEXT NOT NULL, resolved_at TEXT)`
	integrityIndexDDL         = `CREATE UNIQUE INDEX rff_integrity_open_lookup ON rff_integrity_findings(kind, locator) WHERE resolved_at IS NULL`
	integrityHistoryIndexDDL  = `CREATE INDEX rff_integrity_history_lookup ON rff_integrity_findings(kind, locator, finding_id)`
	integrityAuditDDL         = `CREATE TABLE rff_integrity_audit (audit_id INTEGER PRIMARY KEY AUTOINCREMENT, finding_id INTEGER NOT NULL, revision INTEGER NOT NULL CHECK(revision > 0), action TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, occurred_at TEXT NOT NULL)`
	integrityAuditIndexDDL    = `CREATE INDEX rff_integrity_audit_lookup ON rff_integrity_audit(finding_id, audit_id)`
	inputsDDL                 = `CREATE TABLE rff_inputs (id TEXT PRIMARY KEY NOT NULL CHECK(length(CAST(id AS BLOB)) BETWEEN 1 AND 256), record BLOB NOT NULL)`
	continuationsDDL          = `CREATE TABLE rff_continuations (digest BLOB PRIMARY KEY NOT NULL, record BLOB NOT NULL)`
	successorsDDL             = `CREATE TABLE rff_successors (digest BLOB PRIMARY KEY NOT NULL, entity BLOB NOT NULL, execution_id BLOB NOT NULL UNIQUE CHECK(length(execution_id) BETWEEN 1 AND 256), phase TEXT NOT NULL, record BLOB NOT NULL)`
	operationsDDL             = `CREATE TABLE rff_operations (execution_id TEXT NOT NULL REFERENCES rff_executions(id) CHECK(length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256), operation_key BLOB NOT NULL, record BLOB NOT NULL, PRIMARY KEY(execution_id, operation_key))`
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

	var schemaExists, recordsExist, activeExists, leasesExist, hookWorkExists, notificationWorkExists, timersExist, inputsExist, continuationsExist, successorsExist, operationsExist, findingsExist, auditExist bool
	for name, exists := range map[string]*bool{
		"rff_schema_version":     &schemaExists,
		"rff_executions":         &recordsExist,
		"rff_active":             &activeExists,
		"rff_transition_leases":  &leasesExist,
		"rff_hook_work":          &hookWorkExists,
		"rff_notification_work":  &notificationWorkExists,
		"rff_timers":             &timersExist,
		"rff_inputs":             &inputsExist,
		"rff_continuations":      &continuationsExist,
		"rff_successors":         &successorsExist,
		"rff_operations":         &operationsExist,
		"rff_integrity_findings": &findingsExist,
		"rff_integrity_audit":    &auditExist,
	} {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`, name).Scan(exists); err != nil {
			return fmt.Errorf("inspect schema: %w", err)
		}
	}
	if !schemaExists {
		if recordsExist || activeExists || leasesExist || hookWorkExists || notificationWorkExists || timersExist || inputsExist || continuationsExist || successorsExist || operationsExist || findingsExist || auditExist {
			return fmt.Errorf("%w: adapter tables exist without a schema version", ErrUnsupportedSchema)
		}
		statements := []string{
			schemaVersionDDL,
			fmt.Sprintf(`INSERT INTO rff_schema_version (version) VALUES (%d)`, adapterSchemaVersion),
			executionsDDL,
			executionRevisionDDL,
			executionRevisionGuardDDL,
			executionLocatorDDL,
			activeDDL,
			activeRevisionDDL,
			activeRevisionGuardDDL,
			activeLocatorDDL,
			leasesDDL,
			hookWorkDDL,
			hookRevisionDDL,
			hookRevisionGuardDDL,
			hookLocatorDDL,
			notificationWorkDDL,
			timersDDL,
			timerDeadlineIndexDDL,
			timerRevisionDDL,
			timerRevisionGuardDDL,
			integrityFindingsDDL,
			integrityIndexDDL,
			integrityHistoryIndexDDL,
			executionAllocationDDL,
			activeAllocationDDL,
			hookAllocationDDL,
			executionHookIndexDDL,
			hookReadyIndexDDL,
			integrityAuditDDL,
			integrityAuditIndexDDL,
			timerLocatorDDL,
			timerAllocationDDL,
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
		if schemaVersion != adapterSchemaVersion {
			return fmt.Errorf("%w: adapter version %d", ErrUnsupportedSchema, schemaVersion)
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
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return err
	}
	if err := validateIdentityIndexes(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema initialization: %w", err)
	}
	return nil
}

func validateIdentityIndexes(ctx context.Context, tx *sql.Tx) error {
	for table, column := range map[string]string{
		"rff_executions": "id", "rff_active": "execution_id",
		"rff_hook_work": "execution_id", "rff_notification_work": "execution_id",
		"rff_timers": "id", "rff_inputs": "id", "rff_successors": "execution_id",
	} {
		var count int
		err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list(?) AS i
			WHERE i."unique" = 1 AND i.partial = 0
			AND EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name = i.name)
			AND (SELECT COUNT(*) FROM pragma_index_info(i.name)) = 1
			AND EXISTS (SELECT 1 FROM pragma_index_info(i.name) WHERE name = ?)`, table, table, column).Scan(&count)
		if err != nil || count == 0 {
			return fmt.Errorf("%w: missing full-value unique identity index on %s: %v", ErrUnsupportedSchema, table, err)
		}
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
	rows, err := tx.QueryContext(ctx, `SELECT e.row_id, e.revision, e.selection_status, e.selection_compatibility,
		e.id, e.entity, e.request_key, e.record, a.execution_id
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
		var id, key, status string
		var entity, data []byte
		var reservation sql.NullString
		var rowID, revision int64
		var compatibility int
		if err := rows.Scan(&rowID, &revision, &status, &compatibility, &id, &entity, &key, &data, &reservation); err != nil {
			return fmt.Errorf("%w: read execution: %w", ErrUnsupportedSchema, err)
		}
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("%w: decode execution: %w", ErrUnsupportedSchema, err)
		}
		if rowID <= 0 || revision <= 0 || status != string(record.Status) || compatibility < 0 || compatibility > 1 ||
			(compatibility == 1) != (record.Compatibility != nil) ||
			transition.ValidateDurableID(id) != nil || !transition.SupportedExecutionSchema(record.SchemaVersion) || record.SchemaVersion == 1 && record.Continuation != nil ||
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
