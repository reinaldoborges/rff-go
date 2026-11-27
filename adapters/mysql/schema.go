package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const adapterSchemaVersion = 5

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
		UNIQUE KEY rff_timer_id_lookup (id)
	) ENGINE=InnoDB`
	inputsDDL = `CREATE TABLE rff_inputs (
		row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id VARBINARY(256) NOT NULL,
		record LONGBLOB NOT NULL,
		UNIQUE KEY rff_input_id_lookup (id)
	) ENGINE=InnoDB`
	continuationsDDL = `CREATE TABLE rff_continuations (
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
			inputsDDL,
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

	if err := validateMySQLSchema(ctx, s.db); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin MySQL schema validation: %w", err)
	}
	defer tx.Rollback()
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

func validateMySQLSchema(ctx context.Context, db *sql.DB) error {
	type column struct {
		name       string
		columnType string
	}
	expected := map[string][]column{
		"rff_schema_version":    {{"version", "tinyint unsigned"}},
		"rff_executions":        {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"entity", "longblob"}, {"request_key", "longblob"}, {"record", "longblob"}},
		"rff_active":            {{"row_id", "bigint unsigned"}, {"entity", "longblob"}, {"execution_id", "varbinary(256)"}},
		"rff_transition_leases": {{"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"owner", "longblob"}, {"generation", "bigint"}, {"expires_at", "varchar(64)"}},
		"rff_hook_work":         {{"row_id", "bigint unsigned"}, {"execution_id", "varbinary(256)"}, {"work", "longblob"}},
		"rff_notification_work": {{"row_id", "bigint unsigned"}, {"execution_id", "varbinary(256)"}, {"work", "longblob"}},
		"rff_timers":            {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"record", "longblob"}},
		"rff_inputs":            {{"row_id", "bigint unsigned"}, {"id", "varbinary(256)"}, {"record", "longblob"}},
		"rff_continuations":     {{"digest", "binary(32)"}, {"record", "longblob"}},
		"rff_successors":        {{"digest", "binary(32)"}, {"entity", "longblob"}, {"execution_id", "varbinary(256)"}, {"phase", "varbinary(16)"}, {"record", "longblob"}},
		"rff_operations":        {{"execution_id", "varbinary(256)"}, {"operation_key", "longblob"}, {"record", "longblob"}},
		"rff_transaction_lock":  {{"id", "tinyint unsigned"}},
	}
	for table, wanted := range expected {
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
	if err := validateMySQLIndexes(ctx, db); err != nil {
		return err
	}
	var lockRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rff_transaction_lock WHERE id = 1`).Scan(&lockRows); err != nil {
		return fmt.Errorf("%w: inspect transaction lock: %w", ErrUnsupportedSchema, err)
	}
	if lockRows != 1 {
		return fmt.Errorf("%w: transaction lock row is missing", ErrUnsupportedSchema)
	}
	return nil
}

func validateMySQLIndexes(ctx context.Context, db *sql.DB) error {
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
		"rff_transition_leases": {"rff_lease_entity_lookup": {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true}},
		"rff_hook_work":         {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_hook_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}}},
		"rff_notification_work": {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_notification_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}}},
		"rff_timers":            {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_timer_id_lookup": {columns: []string{"id"}, prefixLengths: []int{0}}},
		"rff_inputs":            {"PRIMARY": {columns: []string{"row_id"}, prefixLengths: []int{0}}, "rff_input_id_lookup": {columns: []string{"id"}, prefixLengths: []int{0}}},
		"rff_continuations":     {"PRIMARY": {columns: []string{"digest"}, prefixLengths: []int{0}}},
		"rff_successors": {
			"PRIMARY":                        {columns: []string{"digest"}, prefixLengths: []int{0}},
			"rff_successor_entity_lookup":    {columns: []string{"entity"}, prefixLengths: []int{256}, nonUnique: true},
			"rff_successor_execution_lookup": {columns: []string{"execution_id"}, prefixLengths: []int{0}},
		},
		"rff_operations":       {"rff_operation_identity_lookup": {columns: []string{"execution_id", "operation_key"}, prefixLengths: []int{0, 256}, nonUnique: true}},
		"rff_transaction_lock": {"PRIMARY": {columns: []string{"id"}, prefixLengths: []int{0}}},
	}
	for table, wanted := range expected {
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
