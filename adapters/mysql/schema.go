package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const adapterSchemaVersion = 10

const (
	schemaVersionDDL = `CREATE TABLE rff_schema_version (
		version TINYINT UNSIGNED NOT NULL
	) ENGINE=InnoDB`
	executionsDDL = `CREATE TABLE rff_executions (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id VARBINARY(256) NOT NULL,
		entity LONGBLOB NOT NULL,
		request_key LONGBLOB NOT NULL,
		record LONGBLOB NOT NULL,
		selection_status VARBINARY(32) NOT NULL DEFAULT 'Waiting',
		selection_compatibility TINYINT UNSIGNED NOT NULL DEFAULT 0,
		revision BIGINT NOT NULL DEFAULT 1,
		UNIQUE KEY rff_execution_id_lookup (id),
		KEY rff_execution_identity_lookup (entity(256), request_key(256)),
		KEY rff_execution_hook_state_lookup (selection_status, selection_compatibility, id)
	) ENGINE=InnoDB`
	activeDDL = `CREATE TABLE rff_active (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		entity LONGBLOB NOT NULL,
		execution_id VARBINARY(256) NOT NULL,
		revision BIGINT NOT NULL DEFAULT 1,
		KEY rff_active_entity_lookup (entity(256)),
		UNIQUE KEY rff_active_execution_lookup (execution_id)
	) ENGINE=InnoDB`
	leasesDDL = `CREATE TABLE rff_transition_leases (
		entity LONGBLOB NOT NULL,
		execution_id VARBINARY(256) NOT NULL,
		owner LONGBLOB NOT NULL,
		generation BIGINT NOT NULL,
		expires_at VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
		KEY rff_lease_entity_lookup (entity(256))
	) ENGINE=InnoDB`
	hookWorkDDL = `CREATE TABLE rff_hook_work (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		execution_id VARBINARY(256) NOT NULL,
		work LONGBLOB NOT NULL,
		dispatch_kind VARBINARY(16) NOT NULL,
		started TINYINT UNSIGNED NOT NULL,
		completed TINYINT UNSIGNED NOT NULL,
		available_at_seconds BIGINT NOT NULL,
		available_at_nanoseconds INT UNSIGNED NOT NULL,
		lease_owner_present TINYINT UNSIGNED NOT NULL,
		lease_expires_at_seconds BIGINT,
		lease_expires_at_nanoseconds INT UNSIGNED,
		revision BIGINT NOT NULL DEFAULT 1,
		CONSTRAINT rff_hook_flags CHECK (started IN (0, 1) AND completed IN (0, 1) AND lease_owner_present IN (0, 1)),
		CONSTRAINT rff_hook_dispatch CHECK (dispatch_kind IN (_binary'enter', _binary'transition', _binary'retry', _binary'recover')),
		CONSTRAINT rff_hook_time CHECK (available_at_nanoseconds <= 999999999),
		CONSTRAINT rff_hook_lease CHECK ((lease_owner_present = 0 AND lease_expires_at_seconds IS NULL AND lease_expires_at_nanoseconds IS NULL) OR
			(lease_owner_present = 1 AND lease_expires_at_seconds IS NOT NULL AND lease_expires_at_nanoseconds IS NOT NULL AND lease_expires_at_nanoseconds <= 999999999)),
		UNIQUE KEY rff_hook_execution_lookup (execution_id),
		KEY rff_hook_ready_lookup (completed, started, available_at_seconds, available_at_nanoseconds, execution_id)
	) ENGINE=InnoDB`
	notificationWorkDDL = `CREATE TABLE rff_notification_work (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		execution_id VARBINARY(256) NOT NULL,
		work LONGBLOB NOT NULL,
		completed TINYINT UNSIGNED NOT NULL,
		due_at_seconds BIGINT NOT NULL,
		due_at_nanoseconds INT UNSIGNED NOT NULL,
		lease_owner_present TINYINT UNSIGNED NOT NULL,
		lease_expires_at_seconds BIGINT,
		lease_expires_at_nanoseconds INT UNSIGNED,
		revision BIGINT NOT NULL DEFAULT 1,
		CONSTRAINT rff_notification_flags CHECK (completed IN (0, 1) AND lease_owner_present IN (0, 1)),
		CONSTRAINT rff_notification_due CHECK (due_at_nanoseconds <= 999999999),
		CONSTRAINT rff_notification_lease CHECK ((lease_owner_present = 0 AND lease_expires_at_seconds IS NULL AND lease_expires_at_nanoseconds IS NULL) OR
			(lease_owner_present = 1 AND lease_expires_at_seconds IS NOT NULL AND lease_expires_at_nanoseconds IS NOT NULL AND lease_expires_at_nanoseconds <= 999999999)),
		UNIQUE KEY rff_notification_execution_lookup (execution_id),
		KEY rff_notification_ready_lookup (completed, execution_id, due_at_seconds, due_at_nanoseconds, lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds)
	) ENGINE=InnoDB`
	timersDDL = `CREATE TABLE rff_timers (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id VARBINARY(256) NOT NULL,
		deadline_seconds BIGINT NOT NULL,
		deadline_nanoseconds INT NOT NULL,
		completed TINYINT NOT NULL,
		compatibility_blocked TINYINT NOT NULL,
		lease_expires_seconds BIGINT NULL,
		lease_expires_nanoseconds INT NULL,
		record LONGBLOB NOT NULL,
		revision BIGINT NOT NULL DEFAULT 1,
		CONSTRAINT rff_timer_deadline_nanoseconds CHECK (deadline_nanoseconds BETWEEN 0 AND 999999999),
		CONSTRAINT rff_timer_completion CHECK (completed IN (0, 1)),
		CONSTRAINT rff_timer_compatibility CHECK (compatibility_blocked IN (0, 1)),
		CONSTRAINT rff_timer_lease_pair CHECK (
			(lease_expires_seconds IS NULL AND lease_expires_nanoseconds IS NULL) OR
			(lease_expires_seconds IS NOT NULL AND lease_expires_nanoseconds IS NOT NULL AND lease_expires_nanoseconds BETWEEN 0 AND 999999999)
		),
		UNIQUE KEY rff_timer_id_lookup (id),
		KEY rff_timer_deadline_lookup (completed, compatibility_blocked, deadline_seconds, deadline_nanoseconds, id)
	) ENGINE=InnoDB`
	timerRevisionAction          = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	timerRevisionDDL             = `CREATE TRIGGER rff_timer_revision BEFORE UPDATE ON rff_timers FOR EACH ROW ` + timerRevisionAction
	timerAllocationAction        = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'timer' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'timer locator was previously used'; END IF; END`
	timerAllocationDDL           = `CREATE TRIGGER rff_timer_allocation AFTER INSERT ON rff_timers FOR EACH ROW ` + timerAllocationAction
	executionRevisionAction      = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	executionRevisionDDL         = `CREATE TRIGGER rff_execution_revision BEFORE UPDATE ON rff_executions FOR EACH ROW ` + executionRevisionAction
	executionAllocationAction    = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'execution' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'execution locator was previously used'; END IF; END`
	executionAllocationDDL       = `CREATE TRIGGER rff_execution_allocation AFTER INSERT ON rff_executions FOR EACH ROW ` + executionAllocationAction
	activeRevisionAction         = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	activeRevisionDDL            = `CREATE TRIGGER rff_active_revision BEFORE UPDATE ON rff_active FOR EACH ROW ` + activeRevisionAction
	activeAllocationAction       = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'reservation' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'reservation locator was previously used'; END IF; END`
	activeAllocationDDL          = `CREATE TRIGGER rff_active_allocation AFTER INSERT ON rff_active FOR EACH ROW ` + activeAllocationAction
	hookRevisionAction           = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	hookRevisionDDL              = `CREATE TRIGGER rff_hook_revision BEFORE UPDATE ON rff_hook_work FOR EACH ROW ` + hookRevisionAction
	hookAllocationAction         = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'hook' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'hook locator was previously used'; END IF; END`
	hookAllocationDDL            = `CREATE TRIGGER rff_hook_allocation AFTER INSERT ON rff_hook_work FOR EACH ROW ` + hookAllocationAction
	notificationRevisionAction   = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	notificationRevisionDDL      = `CREATE TRIGGER rff_notification_revision BEFORE UPDATE ON rff_notification_work FOR EACH ROW ` + notificationRevisionAction
	notificationAllocationAction = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'notification' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'notification locator was previously used'; END IF; END`
	notificationAllocationDDL    = `CREATE TRIGGER rff_notification_allocation AFTER INSERT ON rff_notification_work FOR EACH ROW ` + notificationAllocationAction
	integrityFindingsDDL         = `CREATE TABLE rff_integrity_findings (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		kind VARBINARY(16) NOT NULL,
		locator BIGINT UNSIGNED NOT NULL,
		finding_revision BIGINT NOT NULL,
		category VARBINARY(32) NOT NULL,
		field VARBINARY(32) NOT NULL,
		scope_channel LONGBLOB NOT NULL,
		detected_at VARBINARY(64) NOT NULL,
		resolved_at VARBINARY(64) NOT NULL,
		open_marker BIGINT UNSIGNED NOT NULL,
		CONSTRAINT rff_finding_kind CHECK (kind IN (_binary'timer', _binary'hook', _binary'notification', _binary'execution', _binary'reservation', _binary'input')),
		CONSTRAINT rff_finding_locator CHECK (locator > 0),
		CONSTRAINT rff_finding_revision CHECK (finding_revision >= 0),
		CONSTRAINT rff_finding_category CHECK (category IN (_binary'invalid_timer', _binary'projection_mismatch', _binary'invalid_execution', _binary'invalid_reservation', _binary'invalid_hook', _binary'invalid_notification', _binary'invalid_input')),
		CONSTRAINT rff_finding_field CHECK (field IN (_binary'record', _binary'selection', _binary'active', _binary'projection')),
		CONSTRAINT rff_finding_scope CHECK (kind = _binary'input' OR (scope_channel = _binary'' AND finding_revision = 0)),
		CONSTRAINT rff_finding_status CHECK ((open_marker = 0 AND resolved_at = _binary'') OR (open_marker > 0 AND resolved_at <> _binary'')),
		UNIQUE KEY rff_integrity_open_lookup (kind, locator, open_marker),
		KEY rff_integrity_open_scope (kind, open_marker, locator),
		KEY rff_integrity_input_channel_lookup (kind, scope_channel(256), open_marker)
	) ENGINE=InnoDB`
	integrityAuditDDL = `CREATE TABLE rff_integrity_audit (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		finding_id BIGINT UNSIGNED NOT NULL,
		revision BIGINT NOT NULL,
		action VARBINARY(32) NOT NULL,
		actor LONGBLOB NOT NULL,
		reason LONGBLOB NOT NULL,
		occurred_at VARBINARY(64) NOT NULL,
		KEY rff_integrity_audit_lookup (finding_id, row_id)
	) ENGINE=InnoDB`
	integritySweepStateDDL = `CREATE TABLE IF NOT EXISTS rff_integrity_sweep_state (
		id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
		sweep_id BIGINT UNSIGNED NOT NULL,
		started_at VARBINARY(64) NOT NULL,
		updated_at VARBINARY(64) NOT NULL,
		complete TINYINT UNSIGNED NOT NULL,
		healthy TINYINT UNSIGNED NOT NULL,
		incomplete TINYINT UNSIGNED NOT NULL
	) ENGINE=InnoDB`
	integritySweepProgressDDL = `CREATE TABLE IF NOT EXISTS rff_integrity_sweep_progress (
		kind VARBINARY(16) NOT NULL PRIMARY KEY,
		high_water_mark BIGINT UNSIGNED NOT NULL,
		inspected_through BIGINT UNSIGNED NOT NULL,
		inspected BIGINT UNSIGNED NOT NULL
	) ENGINE=InnoDB`
	integrityExecutionArchiveDDL = `CREATE TABLE rff_integrity_execution_archive (
		finding_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		locator BIGINT UNSIGNED NOT NULL,
		revision BIGINT NOT NULL,
		record_digest BINARY(32) NOT NULL,
		dependency_digest BINARY(32) NOT NULL,
		record LONGBLOB NOT NULL,
		CONSTRAINT rff_integrity_execution_archive_finding_fk FOREIGN KEY (finding_id) REFERENCES rff_integrity_findings(row_id)
	) ENGINE=InnoDB`
	integrityExecutionArchiveUpdateAction = `BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'integrity evidence is immutable'; END`
	integrityExecutionArchiveDeleteAction = `BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'integrity evidence is immutable'; END`
	integrityExecutionArchiveUpdateDDL    = `CREATE TRIGGER rff_integrity_execution_archive_update_guard BEFORE UPDATE ON rff_integrity_execution_archive FOR EACH ROW ` + integrityExecutionArchiveUpdateAction
	integrityExecutionArchiveDeleteDDL    = `CREATE TRIGGER rff_integrity_execution_archive_delete_guard BEFORE DELETE ON rff_integrity_execution_archive FOR EACH ROW ` + integrityExecutionArchiveDeleteAction
	inputsDDL                             = `CREATE TABLE rff_inputs (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id VARBINARY(256) NOT NULL,
		record LONGBLOB NOT NULL,
		channel LONGBLOB NOT NULL,
		outcome_present TINYINT UNSIGNED NOT NULL,
		revision BIGINT NOT NULL DEFAULT 1,
		CONSTRAINT rff_input_outcome CHECK (outcome_present IN (0, 1)),
		CONSTRAINT rff_input_revision_value CHECK (revision > 0),
		UNIQUE KEY rff_input_id_lookup (id),
		KEY rff_input_selection_lookup (channel(256), outcome_present, id)
	) ENGINE=InnoDB`
	inputRevisionAction   = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	inputRevisionDDL      = `CREATE TRIGGER rff_input_revision BEFORE UPDATE ON rff_inputs FOR EACH ROW ` + inputRevisionAction
	inputAllocationAction = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'input' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'input locator was previously used'; END IF; END`
	inputAllocationDDL    = `CREATE TRIGGER rff_input_allocation AFTER INSERT ON rff_inputs FOR EACH ROW ` + inputAllocationAction
	continuationsDDL      = `CREATE TABLE rff_continuations (
		digest BINARY(32) NOT NULL PRIMARY KEY,
		record LONGBLOB NOT NULL
	) ENGINE=InnoDB`
	successorsDDL = `CREATE TABLE rff_successors (
		digest BINARY(32) NOT NULL PRIMARY KEY,
		entity LONGBLOB NOT NULL,
		execution_id VARBINARY(256) NOT NULL,
		phase VARBINARY(16) NOT NULL,
		record LONGBLOB NOT NULL,
		KEY rff_successor_entity_lookup (entity(256)),
		UNIQUE KEY rff_successor_execution_lookup (execution_id)
	) ENGINE=InnoDB`
	operationsDDL = `CREATE TABLE rff_operations (
		execution_id VARBINARY(256) NOT NULL,
		operation_key LONGBLOB NOT NULL,
		record LONGBLOB NOT NULL,
		KEY rff_operation_identity_lookup (execution_id, operation_key(256))
	) ENGINE=InnoDB`
	transactionLockDDL = `CREATE TABLE rff_transaction_lock (
		id TINYINT UNSIGNED NOT NULL PRIMARY KEY
	) ENGINE=InnoDB`
)

var adapterTables = []string{
	"rff_schema_version",
	"rff_executions",
	"rff_active",
	"rff_transition_leases",
	"rff_hook_work",
	"rff_notification_work",
	"rff_timers",
	"rff_integrity_findings",
	"rff_integrity_audit",
	"rff_integrity_sweep_state",
	"rff_integrity_sweep_progress",
	"rff_inputs",
	"rff_continuations",
	"rff_successors",
	"rff_operations",
	"rff_transaction_lock",
}

func (s *Store[EntityID, StateID]) initialize(ctx context.Context) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire MySQL schema connection: %w", err)
	}
	defer conn.Close()
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK('rff:mysql:schema', 30)`).Scan(&acquired); err != nil {
		return fmt.Errorf("lock MySQL schema initialization: %w", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return errors.New("timed out acquiring MySQL schema initialization lock")
	}
	defer func() {
		var released sql.NullInt64
		if releaseErr := conn.QueryRowContext(context.Background(), `SELECT RELEASE_LOCK('rff:mysql:schema')`).Scan(&released); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release MySQL schema initialization lock: %w", releaseErr))
		}
	}()

	existing := make(map[string]bool, len(adapterTables))
	for _, name := range adapterTables {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ?
		`, name).Scan(&count); err != nil {
			return fmt.Errorf("inspect MySQL adapter schema: %w", err)
		}
		existing[name] = count != 0
	}
	if !existing["rff_schema_version"] {
		for _, name := range adapterTables {
			if existing[name] {
				return fmt.Errorf("%w: adapter tables exist without a schema version", ErrUnsupportedSchema)
			}
		}
		for _, statement := range []string{
			executionsDDL,
			executionRevisionDDL,
			activeDDL,
			activeRevisionDDL,
			leasesDDL,
			hookWorkDDL,
			hookRevisionDDL,
			notificationWorkDDL,
			notificationRevisionDDL,
			timersDDL,
			timerRevisionDDL,
			integrityFindingsDDL,
			integrityAuditDDL,
			integritySweepStateDDL,
			integritySweepProgressDDL,
			executionAllocationDDL,
			activeAllocationDDL,
			hookAllocationDDL,
			notificationAllocationDDL,
			timerAllocationDDL,
			inputsDDL,
			inputRevisionDDL,
			inputAllocationDDL,
			continuationsDDL,
			successorsDDL,
			operationsDDL,
			transactionLockDDL,
			schemaVersionDDL,
		} {
			if _, err := s.db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("create MySQL adapter table: %w", err)
			}
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO rff_transaction_lock (id) VALUES (1)`); err != nil {
			return fmt.Errorf("initialize MySQL transaction lock: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO rff_schema_version (version) VALUES (?)`, adapterSchemaVersion); err != nil {
			return fmt.Errorf("initialize MySQL schema version: %w", err)
		}
	} else {
		for _, name := range adapterTables {
			if !existing[name] && name != "rff_integrity_sweep_state" && name != "rff_integrity_sweep_progress" {
				return fmt.Errorf("%w: adapter table %s is missing", ErrUnsupportedSchema, name)
			}
		}
		var version int
		var count int
		rows, err := s.db.QueryContext(ctx, `SELECT version FROM rff_schema_version`)
		if err != nil {
			return fmt.Errorf("%w: read adapter version: %w", ErrUnsupportedSchema, err)
		}
		for rows.Next() {
			if err := rows.Scan(&version); err != nil {
				return errors.Join(fmt.Errorf("%w: read adapter version: %w", ErrUnsupportedSchema, err), rows.Close())
			}
			count++
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("%w: read adapter version: %w", ErrUnsupportedSchema, err)
		}
		if count != 1 {
			return fmt.Errorf("%w: adapter version %d", ErrUnsupportedSchema, version)
		}
		if version == adapterSchemaVersion-1 {
			for _, statement := range []string{integritySweepStateDDL, integritySweepProgressDDL} {
				if _, err := s.db.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("migrate MySQL integrity sweep tables: %w", err)
				}
			}
			existing["rff_integrity_sweep_state"] = true
			existing["rff_integrity_sweep_progress"] = true
			if _, err := s.db.ExecContext(ctx, `UPDATE rff_schema_version SET version = ?`, adapterSchemaVersion); err != nil {
				return fmt.Errorf("update MySQL adapter version: %w", err)
			}
			version = adapterSchemaVersion
		}
		if version != adapterSchemaVersion {
			return fmt.Errorf("%w: adapter version %d", ErrUnsupportedSchema, version)
		}
		for _, name := range adapterTables {
			if !existing[name] {
				return fmt.Errorf("%w: adapter table %s is missing", ErrUnsupportedSchema, name)
			}
		}
	}

	if err := validateMySQLSchema(ctx, s.db, false); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin MySQL schema validation: %w", err)
	}
	defer tx.Rollback()
	if err := lockMySQLSchema(ctx, tx, false); err != nil {
		return err
	}
	if err := validateMySQLSchema(ctx, tx, false); err != nil {
		return err
	}
	for _, validate := range []func() error{
		func() error { return validateRecords(ctx, tx) },
		func() error { return validateLeases(ctx, tx) },
		func() error { return validateHookWork(ctx, tx) },
		func() error { return validateNotificationWork(ctx, tx) },
		func() error { return validateTimers(ctx, tx) },
		func() error { return validateInputs(ctx, tx) },
		func() error { return validateOperations(ctx, tx) },
		func() error { return s.validateSuccessors(ctx, tx) },
	} {
		if err := validate(); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finish MySQL schema validation: %w", err)
	}
	return nil
}

type schemaReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Hold metadata locks until commit so validation cannot race table DDL.
func timerIntegrityTable(table string) bool {
	switch table {
	case "rff_schema_version", "rff_transaction_lock", "rff_timers", "rff_integrity_findings", "rff_integrity_audit":
		return true
	default:
		return false
	}
}

func lockMySQLSchema(ctx context.Context, tx *sql.Tx, timerOnly bool) error {
	for _, table := range adapterTables {
		if timerOnly && !timerIntegrityTable(table) {
			continue
		}
		rows, err := tx.QueryContext(ctx, `SELECT 1 FROM `+table+` LIMIT 0`)
		if err != nil {
			return fmt.Errorf("%w: lock adapter schema: %w", ErrUnsupportedSchema, err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func validateMySQLSchema(ctx context.Context, db schemaReader, timerOnly bool) error {
	type column struct {
		name       string
		columnType string
	}
	expected := map[string][]column{
		"rff_schema_version":           {{"version", "tinyint unsigned"}},
		"rff_executions":               {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"entity", "longblob"}, {"request_key", "longblob"}, {"record", "longblob"}, {"selection_status", "varbinary(32)"}, {"selection_compatibility", "tinyint unsigned"}, {"revision", "bigint"}},
		"rff_active":                   {{"row_id", "bigint unsigned"}, {"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"revision", "bigint"}},
		"rff_transition_leases":        {{"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"owner", "longblob"}, {"generation", "bigint"}, {"expires_at", "varchar(64)"}},
		"rff_hook_work":                {{"row_id", "bigint unsigned"}, {"execution_id", "varbinary(256)"}, {"work", "longblob"}, {"dispatch_kind", "varbinary(16)"}, {"started", "tinyint unsigned"}, {"completed", "tinyint unsigned"}, {"available_at_seconds", "bigint"}, {"available_at_nanoseconds", "int unsigned"}, {"lease_owner_present", "tinyint unsigned"}, {"lease_expires_at_seconds", "bigint"}, {"lease_expires_at_nanoseconds", "int unsigned"}, {"revision", "bigint"}},
		"rff_notification_work":        {{"row_id", "bigint unsigned"}, {"execution_id", "varbinary(256)"}, {"work", "longblob"}, {"completed", "tinyint unsigned"}, {"due_at_seconds", "bigint"}, {"due_at_nanoseconds", "int unsigned"}, {"lease_owner_present", "tinyint unsigned"}, {"lease_expires_at_seconds", "bigint"}, {"lease_expires_at_nanoseconds", "int unsigned"}, {"revision", "bigint"}},
		"rff_timers":                   {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"deadline_seconds", "bigint"}, {"deadline_nanoseconds", "int"}, {"completed", "tinyint"}, {"compatibility_blocked", "tinyint"}, {"lease_expires_seconds", "bigint"}, {"lease_expires_nanoseconds", "int"}, {"record", "longblob"}, {"revision", "bigint"}},
		"rff_integrity_findings":       {{"row_id", "bigint unsigned"}, {"kind", "varbinary(16)"}, {"locator", "bigint unsigned"}, {"finding_revision", "bigint"}, {"category", "varbinary(32)"}, {"field", "varbinary(32)"}, {"scope_channel", "longblob"}, {"detected_at", "varbinary(64)"}, {"resolved_at", "varbinary(64)"}, {"open_marker", "bigint unsigned"}},
		"rff_integrity_audit":          {{"row_id", "bigint unsigned"}, {"finding_id", "bigint unsigned"}, {"revision", "bigint"}, {"action", "varbinary(32)"}, {"actor", "longblob"}, {"reason", "longblob"}, {"occurred_at", "varbinary(64)"}},
		"rff_integrity_sweep_state":    {{"id", "tinyint unsigned"}, {"sweep_id", "bigint unsigned"}, {"started_at", "varbinary(64)"}, {"updated_at", "varbinary(64)"}, {"complete", "tinyint unsigned"}, {"healthy", "tinyint unsigned"}, {"incomplete", "tinyint unsigned"}},
		"rff_integrity_sweep_progress": {{"kind", "varbinary(16)"}, {"high_water_mark", "bigint unsigned"}, {"inspected_through", "bigint unsigned"}, {"inspected", "bigint unsigned"}},
		"rff_inputs":                   {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"record", "longblob"}, {"channel", "longblob"}, {"outcome_present", "tinyint unsigned"}, {"revision", "bigint"}},
		"rff_continuations":            {{"digest", "binary(32)"}, {"record", "longblob"}},
		"rff_successors":               {{"digest", "binary(32)"}, {"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"phase", "varbinary(16)"}, {"record", "longblob"}},
		"rff_operations":               {{"execution_id", "varbinary(256)"}, {"operation_key", "longblob"}, {"record", "longblob"}},
		"rff_transaction_lock":         {{"id", "tinyint unsigned"}},
	}
	for table, wanted := range expected {
		if timerOnly && !timerIntegrityTable(table) {
			continue
		}
		var engine string
		if err := db.QueryRowContext(ctx, `SELECT engine FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ?`, table).Scan(&engine); err != nil {
			return fmt.Errorf("%w: inspect table %s: %w", ErrUnsupportedSchema, table, err)
		}
		if engine != "InnoDB" {
			return fmt.Errorf("%w: table %s must use InnoDB", ErrUnsupportedSchema, table)
		}
		rows, err := db.QueryContext(ctx, `SELECT column_name, column_type, is_nullable, extra FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`, table)
		if err != nil {
			return fmt.Errorf("%w: inspect columns for %s: %w", ErrUnsupportedSchema, table, err)
		}
		type columnDefinition struct {
			name       string
			columnType string
			nullable   string
			extra      string
		}
		var found []columnDefinition
		for rows.Next() {
			var definition columnDefinition
			if err := rows.Scan(&definition.name, &definition.columnType, &definition.nullable, &definition.extra); err != nil {
				return errors.Join(fmt.Errorf("%w: inspect columns for %s: %w", ErrUnsupportedSchema, table, err), rows.Close())
			}
			found = append(found, definition)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("%w: inspect columns for %s: %w", ErrUnsupportedSchema, table, err)
		}
		if len(found) != len(wanted) {
			return fmt.Errorf("%w: incompatible columns in %s", ErrUnsupportedSchema, table)
		}
		for i := range wanted {
			extra := ""
			if wanted[i].name == "row_id" {
				extra = "auto_increment"
			}
			nullable := "NO"
			if (table == "rff_hook_work" && (wanted[i].name == "lease_expires_at_seconds" || wanted[i].name == "lease_expires_at_nanoseconds")) ||
				(table == "rff_notification_work" && (wanted[i].name == "lease_expires_at_seconds" || wanted[i].name == "lease_expires_at_nanoseconds")) ||
				(table == "rff_timers" && (wanted[i].name == "lease_expires_seconds" || wanted[i].name == "lease_expires_nanoseconds")) {
				nullable = "YES"
			}
			if found[i].name != wanted[i].name || found[i].columnType != wanted[i].columnType || found[i].nullable != nullable || found[i].extra != extra {
				return fmt.Errorf("%w: incompatible column %s in %s", ErrUnsupportedSchema, found[i].name, table)
			}
		}
	}
	if err := validateMySQLIndexes(ctx, db, timerOnly); err != nil {
		return err
	}
	var revisionDefault sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'rff_timers' AND column_name = 'revision'`).Scan(&revisionDefault); err != nil || !revisionDefault.Valid || revisionDefault.String != "1" {
		return fmt.Errorf("%w: incompatible timer revision default", ErrUnsupportedSchema)
	}
	if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'rff_inputs' AND column_name = 'revision'`).Scan(&revisionDefault); err != nil || !revisionDefault.Valid || revisionDefault.String != "1" {
		return fmt.Errorf("%w: incompatible input revision default", ErrUnsupportedSchema)
	}
	var lockRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rff_transaction_lock WHERE id = 1`).Scan(&lockRows); err != nil {
		return fmt.Errorf("%w: inspect transaction lock: %w", ErrUnsupportedSchema, err)
	}
	if lockRows != 1 {
		return fmt.Errorf("%w: transaction lock row is missing", ErrUnsupportedSchema)
	}
	var version, count int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0), COUNT(*) FROM rff_schema_version`).Scan(&version, &count); err != nil || version != adapterSchemaVersion || count != 1 {
		return fmt.Errorf("%w: incompatible adapter version", ErrUnsupportedSchema)
	}
	for _, trigger := range []struct{ name, action, timing, event, table string }{
		{"rff_timer_revision", timerRevisionAction, "BEFORE", "UPDATE", "rff_timers"},
		{"rff_timer_allocation", timerAllocationAction, "AFTER", "INSERT", "rff_timers"},
		{"rff_input_revision", inputRevisionAction, "BEFORE", "UPDATE", "rff_inputs"},
		{"rff_input_allocation", inputAllocationAction, "AFTER", "INSERT", "rff_inputs"},
		{"rff_execution_revision", executionRevisionAction, "BEFORE", "UPDATE", "rff_executions"},
		{"rff_execution_allocation", executionAllocationAction, "AFTER", "INSERT", "rff_executions"},
		{"rff_active_revision", activeRevisionAction, "BEFORE", "UPDATE", "rff_active"},
		{"rff_active_allocation", activeAllocationAction, "AFTER", "INSERT", "rff_active"},
		{"rff_hook_revision", hookRevisionAction, "BEFORE", "UPDATE", "rff_hook_work"},
		{"rff_hook_allocation", hookAllocationAction, "AFTER", "INSERT", "rff_hook_work"},
		{"rff_notification_revision", notificationRevisionAction, "BEFORE", "UPDATE", "rff_notification_work"},
		{"rff_notification_allocation", notificationAllocationAction, "AFTER", "INSERT", "rff_notification_work"},
	} {
		var statement, timing, event, table string
		if err := db.QueryRowContext(ctx, `SELECT action_statement, action_timing, event_manipulation, event_object_table
			FROM information_schema.triggers WHERE trigger_schema = DATABASE() AND trigger_name = ?`, trigger.name).Scan(&statement, &timing, &event, &table); err != nil ||
			statement != trigger.action || timing != trigger.timing || event != trigger.event || table != trigger.table {
			return fmt.Errorf("%w: incompatible adapter trigger", ErrUnsupportedSchema)
		}
	}
	type checkConstraint struct {
		table  string
		clause string
	}
	for name, expected := range map[string]checkConstraint{
		"rff_timer_deadline_nanoseconds": {table: "rff_timers", clause: "(`deadline_nanoseconds` between 0 and 999999999)"},
		"rff_timer_completion":           {table: "rff_timers", clause: "(`completed` in (0,1))"},
		"rff_timer_compatibility":        {table: "rff_timers", clause: "(`compatibility_blocked` in (0,1))"},
		"rff_timer_lease_pair":           {table: "rff_timers", clause: "(((`lease_expires_seconds` is null) and (`lease_expires_nanoseconds` is null)) or ((`lease_expires_seconds` is not null) and (`lease_expires_nanoseconds` is not null) and (`lease_expires_nanoseconds` between 0 and 999999999)))"},
		"rff_finding_kind":               {table: "rff_integrity_findings", clause: "(`kind` in (_binary\\'timer\\',_binary\\'hook\\',_binary\\'notification\\',_binary\\'execution\\',_binary\\'reservation\\',_binary\\'input\\'))"},
		"rff_finding_locator":            {table: "rff_integrity_findings", clause: "(`locator` > 0)"},
		"rff_finding_category":           {table: "rff_integrity_findings", clause: "(`category` in (_binary\\'invalid_timer\\',_binary\\'projection_mismatch\\',_binary\\'invalid_execution\\',_binary\\'invalid_reservation\\',_binary\\'invalid_hook\\',_binary\\'invalid_notification\\',_binary\\'invalid_input\\'))"},
		"rff_finding_field":              {table: "rff_integrity_findings", clause: "(`field` in (_binary\\'record\\',_binary\\'selection\\',_binary\\'active\\',_binary\\'projection\\'))"},
		"rff_finding_status":             {table: "rff_integrity_findings", clause: "(((`open_marker` = 0) and (`resolved_at` = _binary\\'\\')) or ((`open_marker` > 0) and (`resolved_at` <> _binary\\'\\')))"},
		"rff_hook_flags":                 {table: "rff_hook_work", clause: "((`started` in (0,1)) and (`completed` in (0,1)) and (`lease_owner_present` in (0,1)))"},
		"rff_hook_dispatch":              {table: "rff_hook_work", clause: "(`dispatch_kind` in (_binary\\'enter\\',_binary\\'transition\\',_binary\\'retry\\',_binary\\'recover\\'))"},
		"rff_hook_time":                  {table: "rff_hook_work", clause: "(`available_at_nanoseconds` <= 999999999)"},
		"rff_hook_lease":                 {table: "rff_hook_work", clause: "(((`lease_owner_present` = 0) and (`lease_expires_at_seconds` is null) and (`lease_expires_at_nanoseconds` is null)) or ((`lease_owner_present` = 1) and (`lease_expires_at_seconds` is not null) and (`lease_expires_at_nanoseconds` is not null) and (`lease_expires_at_nanoseconds` <= 999999999)))"},
		"rff_notification_flags":         {table: "rff_notification_work", clause: "((`completed` in (0,1)) and (`lease_owner_present` in (0,1)))"},
		"rff_notification_due":           {table: "rff_notification_work", clause: "(`due_at_nanoseconds` <= 999999999)"},
		"rff_notification_lease":         {table: "rff_notification_work", clause: "(((`lease_owner_present` = 0) and (`lease_expires_at_seconds` is null) and (`lease_expires_at_nanoseconds` is null)) or ((`lease_owner_present` = 1) and (`lease_expires_at_seconds` is not null) and (`lease_expires_at_nanoseconds` is not null) and (`lease_expires_at_nanoseconds` <= 999999999)))"},
		"rff_finding_revision":           {table: "rff_integrity_findings", clause: "(`finding_revision` >= 0)"},
		"rff_finding_scope":              {table: "rff_integrity_findings", clause: "((`kind` = _binary\\'input\\') or ((`scope_channel` = _binary\\'\\') and (`finding_revision` = 0)))"},
		"rff_input_outcome":              {table: "rff_inputs", clause: "(`outcome_present` in (0,1))"},
		"rff_input_revision_value":       {table: "rff_inputs", clause: "(`revision` > 0)"},
	} {
		var clause, enforced string
		err := db.QueryRowContext(ctx, `SELECT c.check_clause, t.enforced FROM information_schema.check_constraints c
			JOIN information_schema.table_constraints t ON t.constraint_schema = c.constraint_schema AND t.constraint_name = c.constraint_name
			WHERE t.table_schema = DATABASE() AND t.table_name = ? AND t.constraint_name = ?`, expected.table, name).Scan(&clause, &enforced)
		if err != nil || clause != expected.clause || enforced != "YES" {
			return fmt.Errorf("%w: incompatible integrity constraint %s: clause=%q enforced=%q", ErrUnsupportedSchema, name, clause, enforced)
		}
	}
	for _, table := range []string{"rff_timers", "rff_executions", "rff_active", "rff_hook_work", "rff_notification_work"} {
		var revisionDefault sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'revision'`, table).Scan(&revisionDefault); err != nil ||
			!revisionDefault.Valid || revisionDefault.String != "1" {
			return fmt.Errorf("%w: incompatible revision default on %s", ErrUnsupportedSchema, table)
		}
	}
	if err := validateMySQLExecutionArchiveStructure(ctx, db); err != nil {
		return err
	}
	return nil
}

func validateMySQLExecutionArchiveStructure(ctx context.Context, db schemaReader) error {
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'rff_integrity_execution_archive'`).Scan(&exists); err != nil {
		return fmt.Errorf("%w: inspect execution evidence archive: %w", ErrUnsupportedSchema, err)
	}
	if exists == 0 {
		return nil
	}
	var engine string
	if err := db.QueryRowContext(ctx, `SELECT engine FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'rff_integrity_execution_archive'`).Scan(&engine); err != nil || engine != "InnoDB" {
		return fmt.Errorf("%w: incompatible execution evidence archive engine", ErrUnsupportedSchema)
	}
	type columnDefinition struct {
		name, columnType, nullable, extra string
	}
	rows, err := db.QueryContext(ctx, `SELECT column_name, column_type, is_nullable, extra
		FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'rff_integrity_execution_archive'
		ORDER BY ordinal_position`)
	if err != nil {
		return fmt.Errorf("%w: inspect execution evidence archive columns: %w", ErrUnsupportedSchema, err)
	}
	var columns []columnDefinition
	for rows.Next() {
		var definition columnDefinition
		if err := rows.Scan(&definition.name, &definition.columnType, &definition.nullable, &definition.extra); err != nil {
			return errors.Join(ErrUnsupportedSchema, rows.Close())
		}
		columns = append(columns, definition)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("%w: inspect execution evidence archive columns: %w", ErrUnsupportedSchema, err)
	}
	expected := []columnDefinition{
		{name: "finding_id", columnType: "bigint unsigned", nullable: "NO"},
		{name: "locator", columnType: "bigint unsigned", nullable: "NO"},
		{name: "revision", columnType: "bigint", nullable: "NO"},
		{name: "record_digest", columnType: "binary(32)", nullable: "NO"},
		{name: "dependency_digest", columnType: "binary(32)", nullable: "NO"},
		{name: "record", columnType: "longblob", nullable: "NO"},
	}
	if len(columns) != len(expected) {
		return fmt.Errorf("%w: incompatible execution evidence archive columns", ErrUnsupportedSchema)
	}
	for i := range expected {
		if columns[i] != expected[i] {
			return fmt.Errorf("%w: incompatible execution evidence archive column %s", ErrUnsupportedSchema, columns[i].name)
		}
	}
	var primaryCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'rff_integrity_execution_archive'
		AND index_name = 'PRIMARY' AND seq_in_index = 1 AND column_name = 'finding_id' AND non_unique = 0`).Scan(&primaryCount); err != nil || primaryCount != 1 {
		return fmt.Errorf("%w: incompatible execution evidence archive key", ErrUnsupportedSchema)
	}
	var foreignKeyCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.key_column_usage
		WHERE constraint_schema = DATABASE() AND table_name = 'rff_integrity_execution_archive'
		AND constraint_name = 'rff_integrity_execution_archive_finding_fk' AND column_name = 'finding_id'
		AND referenced_table_name = 'rff_integrity_findings' AND referenced_column_name = 'row_id'`).Scan(&foreignKeyCount); err != nil || foreignKeyCount != 1 {
		return fmt.Errorf("%w: incompatible execution evidence archive reference", ErrUnsupportedSchema)
	}
	for _, trigger := range []struct {
		name, action, timing, event string
	}{
		{"rff_integrity_execution_archive_update_guard", integrityExecutionArchiveUpdateAction, "BEFORE", "UPDATE"},
		{"rff_integrity_execution_archive_delete_guard", integrityExecutionArchiveDeleteAction, "BEFORE", "DELETE"},
	} {
		var statement, timing, event string
		if err := db.QueryRowContext(ctx, `SELECT action_statement, action_timing, event_manipulation
			FROM information_schema.triggers WHERE trigger_schema = DATABASE() AND trigger_name = ?`, trigger.name).
			Scan(&statement, &timing, &event); err != nil || statement != trigger.action || timing != trigger.timing || event != trigger.event {
			return fmt.Errorf("%w: incompatible execution evidence archive trigger", ErrUnsupportedSchema)
		}
	}
	return nil
}

func validateMySQLIndexes(ctx context.Context, db schemaReader, timerOnly bool) error {
	type index struct {
		columns       []string
		prefixLengths []int
		nonUnique     bool
	}
	expected := map[string]map[string]index{
		"rff_schema_version": {},
		"rff_executions": {
			"PRIMARY":                         {columns: []string{"row_id"}, prefixLengths: []int{0}},
			"rff_execution_id_lookup":         {columns: []string{"id"}, prefixLengths: []int{0}},
			"rff_execution_identity_lookup":   {columns: []string{"entity", "request_key"}, prefixLengths: []int{256, 256}, nonUnique: true},
			"rff_execution_hook_state_lookup": {columns: []string{"selection_status", "selection_compatibility", "id"}, prefixLengths: []int{0, 0, 0}, nonUnique: true},
		},
		"rff_active": {
			"PRIMARY":                     {columns: []string{"row_id"}, prefixLengths: []int{0}},
			"rff_active_entity_lookup":    {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true},
			"rff_active_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}},
		},
		"rff_transition_leases": {"rff_lease_entity_lookup": {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true}},
		"rff_hook_work":         {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_hook_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}}, "rff_hook_ready_lookup": {columns: []string{"completed", "started", "available_at_seconds", "available_at_nanoseconds", "execution_id"}, prefixLengths: []int{0, 0, 0, 0, 0}, nonUnique: true}},
		"rff_notification_work": {
			"PRIMARY":                           {columns: []string{"row_id"}, prefixLengths: []int{0}},
			"rff_notification_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}},
			"rff_notification_ready_lookup":     {columns: []string{"completed", "execution_id", "due_at_seconds", "due_at_nanoseconds", "lease_owner_present", "lease_expires_at_seconds", "lease_expires_at_nanoseconds"}, prefixLengths: []int{0, 0, 0, 0, 0, 0, 0}, nonUnique: true},
		},
		"rff_timers":                   {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_timer_id_lookup": {columns: []string{"id"}, prefixLengths: []int{0}}, "rff_timer_deadline_lookup": {columns: []string{"completed", "compatibility_blocked", "deadline_seconds", "deadline_nanoseconds", "id"}, prefixLengths: []int{0, 0, 0, 0, 0}, nonUnique: true}},
		"rff_integrity_findings":       {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_integrity_open_lookup": {columns: []string{"kind", "locator", "open_marker"}, prefixLengths: []int{0, 0, 0}}, "rff_integrity_open_scope": {columns: []string{"kind", "open_marker", "locator"}, prefixLengths: []int{0, 0, 0}, nonUnique: true}, "rff_integrity_input_channel_lookup": {columns: []string{"kind", "scope_channel", "open_marker"}, prefixLengths: []int{0, 256, 0}, nonUnique: true}},
		"rff_integrity_audit":          {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_integrity_audit_lookup": {columns: []string{"finding_id", "row_id"}, prefixLengths: []int{0, 0}, nonUnique: true}},
		"rff_integrity_sweep_state":    {"PRIMARY": {columns: []string{"id"}, prefixLengths: []int{0}}},
		"rff_integrity_sweep_progress": {"PRIMARY": {columns: []string{"kind"}, prefixLengths: []int{0}}},
		"rff_inputs":                   {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_input_id_lookup": {columns: []string{"id"}, prefixLengths: []int{0}}, "rff_input_selection_lookup": {columns: []string{"channel", "outcome_present", "id"}, prefixLengths: []int{256, 0, 0}, nonUnique: true}},
		"rff_continuations":            {"PRIMARY": {columns: []string{"digest"}, prefixLengths: []int{0}}},
		"rff_successors": {
			"PRIMARY":                        {columns: []string{"digest"}, prefixLengths: []int{0}},
			"rff_successor_entity_lookup":    {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true},
			"rff_successor_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}},
		},
		"rff_operations":       {"rff_operation_identity_lookup": {columns: []string{"execution_id", "operation_key"}, prefixLengths: []int{0, 256}, nonUnique: true}},
		"rff_transaction_lock": {"PRIMARY": {columns: []string{"id"}, prefixLengths: []int{0}}},
	}
	for table, wanted := range expected {
		if timerOnly && !timerIntegrityTable(table) {
			continue
		}
		rows, err := db.QueryContext(ctx, `SELECT index_name, seq_in_index, column_name, non_unique, sub_part
			FROM information_schema.statistics
			WHERE table_schema = DATABASE() AND table_name = ?
			ORDER BY index_name, seq_in_index`, table)
		if err != nil {
			return fmt.Errorf("%w: inspect indexes for %s: %w", ErrUnsupportedSchema, table, err)
		}
		found := make(map[string]index)
		for rows.Next() {
			var name, column string
			var sequence, nonUnique int
			var prefix sql.NullInt64
			if err := rows.Scan(&name, &sequence, &column, &nonUnique, &prefix); err != nil {
				return errors.Join(fmt.Errorf("%w: inspect indexes for %s: %w", ErrUnsupportedSchema, table, err), rows.Close())
			}
			current := found[name]
			if sequence != len(current.columns)+1 {
				return errors.Join(fmt.Errorf("%w: malformed index %s on %s", ErrUnsupportedSchema, name, table), rows.Close())
			}
			current.columns = append(current.columns, column)
			prefixLength := 0
			if prefix.Valid {
				prefixLength = int(prefix.Int64)
			}
			current.prefixLengths = append(current.prefixLengths, prefixLength)
			current.nonUnique = nonUnique != 0
			found[name] = current
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("%w: inspect indexes for %s: %w", ErrUnsupportedSchema, table, err)
		}
		for name, want := range wanted {
			got, ok := found[name]
			if !ok || got.nonUnique != want.nonUnique || len(got.columns) != len(want.columns) ||
				len(got.prefixLengths) != len(want.prefixLengths) {
				return fmt.Errorf("%w: required index %s is missing or incompatible on %s", ErrUnsupportedSchema, name, table)
			}
			for i := range want.columns {
				if got.columns[i] != want.columns[i] || got.prefixLengths[i] != want.prefixLengths[i] {
					return fmt.Errorf("%w: incompatible index %s on %s", ErrUnsupportedSchema, name, table)
				}
			}
		}
	}

	return nil
}
