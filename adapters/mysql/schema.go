package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const adapterSchemaVersion = 7

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
		UNIQUE KEY rff_execution_id_lookup (id),
		KEY rff_execution_identity_lookup (entity(256), request_key(256))
	) ENGINE=InnoDB`
	activeDDL = `CREATE TABLE rff_active (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		entity LONGBLOB NOT NULL,
		execution_id VARBINARY(256) NOT NULL,
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
		UNIQUE KEY rff_hook_execution_lookup (execution_id)
	) ENGINE=InnoDB`
	notificationWorkDDL = `CREATE TABLE rff_notification_work (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		execution_id VARBINARY(256) NOT NULL,
		work LONGBLOB NOT NULL,
		UNIQUE KEY rff_notification_execution_lookup (execution_id)
	) ENGINE=InnoDB`
	timersDDL = `CREATE TABLE rff_timers (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id VARBINARY(256) NOT NULL,
		record LONGBLOB NOT NULL,
		revision BIGINT NOT NULL DEFAULT 1,
		UNIQUE KEY rff_timer_id_lookup (id)
	) ENGINE=InnoDB`
	timerRevisionAction   = `SET NEW.revision = IF(NEW.row_id = OLD.row_id, OLD.revision + 1, NULL)`
	timerRevisionDDL      = `CREATE TRIGGER rff_timer_revision BEFORE UPDATE ON rff_timers FOR EACH ROW ` + timerRevisionAction
	timerAllocationAction = `BEGIN IF EXISTS (SELECT 1 FROM rff_integrity_findings WHERE kind = 'timer' AND locator = NEW.row_id) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'timer locator was previously used'; END IF; END`
	timerAllocationDDL    = `CREATE TRIGGER rff_timer_allocation AFTER INSERT ON rff_timers FOR EACH ROW ` + timerAllocationAction
	integrityFindingsDDL  = `CREATE TABLE rff_integrity_findings (
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
		CONSTRAINT rff_finding_kind CHECK (kind IN (_binary'timer', _binary'input')),
		CONSTRAINT rff_finding_locator CHECK (locator > 0),
		CONSTRAINT rff_finding_revision CHECK (finding_revision >= 0),
		CONSTRAINT rff_finding_category CHECK ((kind = _binary'timer' AND category = _binary'invalid_timer') OR (kind = _binary'input' AND category = _binary'invalid_input')),
		CONSTRAINT rff_finding_field CHECK ((kind = _binary'timer' AND field = _binary'record') OR (kind = _binary'input' AND field IN (_binary'record', _binary'projection'))),
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
		action VARBINARY(16) NOT NULL,
		actor LONGBLOB NOT NULL,
		reason LONGBLOB NOT NULL,
		occurred_at VARBINARY(64) NOT NULL,
		KEY rff_integrity_audit_lookup (finding_id, row_id)
	) ENGINE=InnoDB`
	inputsDDL = `CREATE TABLE rff_inputs (
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
			activeDDL,
			leasesDDL,
			hookWorkDDL,
			notificationWorkDDL,
			timersDDL,
			timerRevisionDDL,
			integrityFindingsDDL,
			integrityAuditDDL,
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
			if !existing[name] {
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
		if count != 1 || version != adapterSchemaVersion {
			return fmt.Errorf("%w: adapter version %d", ErrUnsupportedSchema, version)
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
		"rff_schema_version":     {{"version", "tinyint unsigned"}},
		"rff_executions":         {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"entity", "longblob"}, {"request_key", "longblob"}, {"record", "longblob"}},
		"rff_active":             {{"row_id", "bigint unsigned"}, {"entity", "longblob"}, {"execution_id", "varbinary(256)"}},
		"rff_transition_leases":  {{"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"owner", "longblob"}, {"generation", "bigint"}, {"expires_at", "varchar(64)"}},
		"rff_hook_work":          {{"row_id", "bigint unsigned"}, {"execution_id", "varbinary(256)"}, {"work", "longblob"}},
		"rff_notification_work":  {{"row_id", "bigint unsigned"}, {"execution_id", "varbinary(256)"}, {"work", "longblob"}},
		"rff_timers":             {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"record", "longblob"}, {"revision", "bigint"}},
		"rff_integrity_findings": {{"row_id", "bigint unsigned"}, {"kind", "varbinary(16)"}, {"locator", "bigint unsigned"}, {"finding_revision", "bigint"}, {"category", "varbinary(32)"}, {"field", "varbinary(32)"}, {"scope_channel", "longblob"}, {"detected_at", "varbinary(64)"}, {"resolved_at", "varbinary(64)"}, {"open_marker", "bigint unsigned"}},
		"rff_integrity_audit":    {{"row_id", "bigint unsigned"}, {"finding_id", "bigint unsigned"}, {"revision", "bigint"}, {"action", "varbinary(16)"}, {"actor", "longblob"}, {"reason", "longblob"}, {"occurred_at", "varbinary(64)"}},
		"rff_inputs":             {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"record", "longblob"}, {"channel", "longblob"}, {"outcome_present", "tinyint unsigned"}, {"revision", "bigint"}},
		"rff_continuations":      {{"digest", "binary(32)"}, {"record", "longblob"}},
		"rff_successors":         {{"digest", "binary(32)"}, {"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"phase", "varbinary(16)"}, {"record", "longblob"}},
		"rff_operations":         {{"execution_id", "varbinary(256)"}, {"operation_key", "longblob"}, {"record", "longblob"}},
		"rff_transaction_lock":   {{"id", "tinyint unsigned"}},
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
			if found[i].name != wanted[i].name || found[i].columnType != wanted[i].columnType || found[i].nullable != "NO" || found[i].extra != extra {
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
	} {
		var statement, timing, event, table string
		if err := db.QueryRowContext(ctx, `SELECT action_statement, action_timing, event_manipulation, event_object_table
			FROM information_schema.triggers WHERE trigger_schema = DATABASE() AND trigger_name = ?`, trigger.name).Scan(&statement, &timing, &event, &table); err != nil ||
			statement != trigger.action || timing != trigger.timing || event != trigger.event || table != trigger.table {
			return fmt.Errorf("%w: incompatible durable record trigger", ErrUnsupportedSchema)
		}
	}
	for name, expected := range map[string]string{
		"rff_finding_kind":     "(`kind` in (_binary\\'timer\\',_binary\\'input\\'))",
		"rff_finding_locator":  "(`locator` > 0)",
		"rff_finding_revision": "(`finding_revision` >= 0)",
		"rff_finding_category": "(((`kind` = _binary\\'timer\\') and (`category` = _binary\\'invalid_timer\\')) or ((`kind` = _binary\\'input\\') and (`category` = _binary\\'invalid_input\\')))",
		"rff_finding_field":    "(((`kind` = _binary\\'timer\\') and (`field` = _binary\\'record\\')) or ((`kind` = _binary\\'input\\') and (`field` in (_binary\\'record\\',_binary\\'projection\\'))))",
		"rff_finding_scope":    "((`kind` = _binary\\'input\\') or ((`scope_channel` = _binary\\'\\') and (`finding_revision` = 0)))",
		"rff_finding_status":   "(((`open_marker` = 0) and (`resolved_at` = _binary\\'\\')) or ((`open_marker` > 0) and (`resolved_at` <> _binary\\'\\')))",
	} {
		var clause, enforced string
		err := db.QueryRowContext(ctx, `SELECT c.check_clause, t.enforced FROM information_schema.check_constraints c
			JOIN information_schema.table_constraints t ON t.constraint_schema = c.constraint_schema AND t.constraint_name = c.constraint_name
			WHERE t.table_schema = DATABASE() AND t.table_name = 'rff_integrity_findings' AND t.constraint_name = ?`, name).Scan(&clause, &enforced)
		if err != nil || clause != expected || enforced != "YES" {
			return fmt.Errorf("%w: incompatible integrity constraint %s: %q", ErrUnsupportedSchema, name, clause)
		}
	}
	for name, expected := range map[string]string{
		"rff_input_outcome":        "(`outcome_present` in (0,1))",
		"rff_input_revision_value": "(`revision` > 0)",
	} {
		var clause, enforced string
		err := db.QueryRowContext(ctx, `SELECT c.check_clause, t.enforced FROM information_schema.check_constraints c
			JOIN information_schema.table_constraints t ON t.constraint_schema = c.constraint_schema AND t.constraint_name = c.constraint_name
			WHERE t.table_schema = DATABASE() AND t.table_name = 'rff_inputs' AND t.constraint_name = ?`, name).Scan(&clause, &enforced)
		if err != nil || clause != expected || enforced != "YES" {
			return fmt.Errorf("%w: incompatible input constraint %s: %q", ErrUnsupportedSchema, name, clause)
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
			"PRIMARY":                       {columns: []string{"row_id"}, prefixLengths: []int{0}},
			"rff_execution_id_lookup":       {columns: []string{"id"}, prefixLengths: []int{0}},
			"rff_execution_identity_lookup": {columns: []string{"entity", "request_key"}, prefixLengths: []int{256, 256}, nonUnique: true},
		},
		"rff_active": {
			"PRIMARY":                     {columns: []string{"row_id"}, prefixLengths: []int{0}},
			"rff_active_entity_lookup":    {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true},
			"rff_active_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}},
		},
		"rff_transition_leases":  {"rff_lease_entity_lookup": {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true}},
		"rff_hook_work":          {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_hook_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}}},
		"rff_notification_work":  {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_notification_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}}},
		"rff_timers":             {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_timer_id_lookup": {columns: []string{"id"}, prefixLengths: []int{0}}},
		"rff_integrity_findings": {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_integrity_open_lookup": {columns: []string{"kind", "locator", "open_marker"}, prefixLengths: []int{0, 0, 0}}, "rff_integrity_open_scope": {columns: []string{"kind", "open_marker", "locator"}, prefixLengths: []int{0, 0, 0}, nonUnique: true}, "rff_integrity_input_channel_lookup": {columns: []string{"kind", "scope_channel", "open_marker"}, prefixLengths: []int{0, 256, 0}, nonUnique: true}},
		"rff_integrity_audit":    {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_integrity_audit_lookup": {columns: []string{"finding_id", "row_id"}, prefixLengths: []int{0, 0}, nonUnique: true}},
		"rff_inputs":             {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_input_id_lookup": {columns: []string{"id"}, prefixLengths: []int{0}}, "rff_input_selection_lookup": {columns: []string{"channel", "outcome_present", "id"}, prefixLengths: []int{256, 0, 0}, nonUnique: true}},
		"rff_continuations":      {"PRIMARY": {columns: []string{"digest"}, prefixLengths: []int{0}}},
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
