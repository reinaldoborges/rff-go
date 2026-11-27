package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	driver "github.com/go-sql-driver/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// IntegrityMaintenance is a worker-free, host-authorized maintenance handle.
type IntegrityMaintenance struct {
	store     *Store[struct{}, struct{}]
	authorize transition.IntegrityAuthorizer
}

// OpenIntegrityMaintenance connects to an existing store without initialization.
func OpenIntegrityMaintenance(dsn string, authorize transition.IntegrityAuthorizer) (*IntegrityMaintenance, error) {
	if dsn == "" || authorize == nil {
		return nil, transition.ErrInvalidRequest
	}
	config, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if config.DBName == "" {
		return nil, transition.ErrInvalidRequest
	}
	connector, err := driver.NewConnector(config)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(strictConnector{Connector: connector})
	db.SetMaxOpenConns(4)
	if err := db.PingContext(context.Background()); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := validateServerVersion(context.Background(), db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &IntegrityMaintenance{store: &Store[struct{}, struct{}]{db: db}, authorize: authorize}, nil
}

// Close releases the maintenance connection pool.
func (m *IntegrityMaintenance) Close() error { return m.store.db.Close() }

func (m *IntegrityMaintenance) checkAccess(ctx context.Context, action, actor, reason string, findingID, locator int64) error {
	if actor == "" || reason == "" {
		return transition.ErrInvalidRequest
	}
	return m.authorize(ctx, action, actor, reason, findingID, locator)
}

// Findings returns safe finding history after explicit authorization.
func (m *IntegrityMaintenance) Findings(ctx context.Context, actor, reason string) ([]transition.IntegrityFinding, error) {
	result, err := m.findings(ctx, actor, reason, 0, transition.MaxIntegrityPageSize+1)
	if len(result) > transition.MaxIntegrityPageSize {
		return nil, transition.ErrIntegrityPageRequired
	}
	return result, err
}

// FindingsPage reads at most limit findings after the supplied finding ID.
func (m *IntegrityMaintenance) FindingsPage(ctx context.Context, actor, reason string, afterID int64, limit int) ([]transition.IntegrityFinding, error) {
	if afterID < 0 || limit <= 0 || limit > transition.MaxIntegrityPageSize {
		return nil, transition.ErrInvalidRequest
	}
	return m.findings(ctx, actor, reason, afterID, limit)
}

func (m *IntegrityMaintenance) findings(ctx context.Context, actor, reason string, afterID int64, limit int) ([]transition.IntegrityFinding, error) {
	if err := m.checkAccess(ctx, "inspect", actor, reason, 0, 0); err != nil {
		return nil, err
	}
	tx, err := m.store.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockMySQLSchema(ctx, tx, false); err != nil {
		return nil, err
	}
	if err := validateMySQLSchema(ctx, tx, false); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT f.row_id, f.kind, f.locator, f.category, f.field, f.detected_at,
		f.resolved_at, f.open_marker,
		CASE f.kind WHEN 'timer' THEN COALESCE(t.revision, 0) WHEN 'hook' THEN COALESCE(h.revision, 0)
			WHEN 'execution' THEN COALESCE(e.revision, 0) WHEN 'reservation' THEN COALESCE(a.revision, 0) ELSE 0 END
		FROM rff_integrity_findings f
		LEFT JOIN rff_timers t ON f.kind = 'timer' AND t.row_id = f.locator
		LEFT JOIN rff_hook_work h ON f.kind = 'hook' AND h.row_id = f.locator
		LEFT JOIN rff_executions e ON f.kind = 'execution' AND e.row_id = f.locator
		LEFT JOIN rff_active a ON f.kind = 'reservation' AND a.row_id = f.locator
		WHERE f.row_id > ? ORDER BY f.row_id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, err
	}
	var result []transition.IntegrityFinding
	for rows.Next() {
		var finding transition.IntegrityFinding
		var detected, resolved string
		var marker int64
		if err := rows.Scan(&finding.ID, &finding.Kind, &finding.Locator, &finding.Category, &finding.Field, &detected, &resolved, &marker, &finding.Revision); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		finding.DetectedAt, err = time.Parse(time.RFC3339Nano, detected)
		if err != nil || finding.ID <= 0 || finding.Locator <= 0 || finding.Revision < 0 || !validIntegrityFinding(finding) ||
			(resolved == "" && marker != 0) || (resolved != "" && marker != finding.ID) {
			return nil, errors.Join(ErrUnsupportedSchema, rows.Close())
		}

		if resolved != "" {
			t, err := time.Parse(time.RFC3339Nano, resolved)
			if err != nil || t.Before(finding.DetectedAt) {
				return nil, errors.Join(ErrUnsupportedSchema, rows.Close())
			}
			finding.ResolvedAt = &t
		}
		result = append(result, finding)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func validIntegrityFinding(finding transition.IntegrityFinding) bool {
	switch finding.Kind {
	case "timer":
		return finding.Category == "invalid_timer" && finding.Field == "record"
	case "hook":
		return (finding.Category == "projection_mismatch" && finding.Field == "selection") ||
			(finding.Category == "invalid_hook" && finding.Field == "record")
	case "execution":
		return (finding.Category == "projection_mismatch" && finding.Field == "selection") ||
			(finding.Category == "invalid_execution" && finding.Field == "record")
	case "reservation":
		return finding.Category == "invalid_reservation" && finding.Field == "active"
	default:
		return false
	}
}

// Audit returns authorized safe audit history for one finding.
func (m *IntegrityMaintenance) Audit(ctx context.Context, actor, reason string, findingID int64) ([]transition.IntegrityAudit, error) {
	result, err := m.audit(ctx, actor, reason, findingID, 0, transition.MaxIntegrityPageSize+1)
	if len(result) > transition.MaxIntegrityPageSize {
		return nil, transition.ErrIntegrityPageRequired
	}
	return result, err
}

// AuditPage reads at most limit audit entries after the supplied audit ID.
func (m *IntegrityMaintenance) AuditPage(ctx context.Context, actor, reason string, findingID, afterID int64, limit int) ([]transition.IntegrityAudit, error) {
	if afterID < 0 || limit <= 0 || limit > transition.MaxIntegrityPageSize {
		return nil, transition.ErrInvalidRequest
	}
	return m.audit(ctx, actor, reason, findingID, afterID, limit)
}

func (m *IntegrityMaintenance) audit(ctx context.Context, actor, reason string, findingID, afterID int64, limit int) ([]transition.IntegrityAudit, error) {
	if err := m.checkAccess(ctx, "audit", actor, reason, findingID, 0); err != nil {
		return nil, err
	}
	if findingID <= 0 {
		return nil, transition.ErrInvalidRequest
	}
	tx, err := m.store.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockMySQLSchema(ctx, tx, false); err != nil {
		return nil, err
	}
	if err := validateMySQLSchema(ctx, tx, false); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT row_id, finding_id, revision, action, actor, reason, occurred_at
		FROM rff_integrity_audit FORCE INDEX (rff_integrity_audit_lookup) WHERE finding_id = ? AND row_id > ? ORDER BY row_id LIMIT ?`, findingID, afterID, limit)
	if err != nil {
		return nil, err
	}
	var result []transition.IntegrityAudit
	for rows.Next() {
		var audit transition.IntegrityAudit
		var occurred string
		if err := rows.Scan(&audit.ID, &audit.FindingID, &audit.Revision, &audit.Action, &audit.Actor, &audit.Reason, &occurred); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		audit.OccurredAt, err = time.Parse(time.RFC3339Nano, occurred)
		if err != nil || audit.ID <= 0 || audit.FindingID != findingID || audit.Revision <= 0 ||
			(audit.Action != "resolve" && audit.Action != "rebuild_hook") || audit.Actor == "" || audit.Reason == "" {
			return nil, errors.Join(ErrUnsupportedSchema, rows.Close())
		}
		result = append(result, audit)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// Resolve revalidates the observed timer and atomically records its resolution.
func (m *IntegrityMaintenance) Resolve(ctx context.Context, actor, reason string, observed transition.IntegrityFinding) error {
	if err := m.checkAccess(ctx, "resolve", actor, reason, observed.ID, observed.Locator); err != nil {
		return err
	}
	if observed.ID <= 0 || observed.Locator <= 0 || observed.Revision <= 0 || observed.Kind != "timer" ||
		observed.Category != "invalid_timer" || observed.Field != "record" || observed.ResolvedAt != nil || observed.DetectedAt.IsZero() {
		return transition.ErrInvalidRequest
	}
	tx, err := m.store.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockMySQLSchema(ctx, tx, false); err != nil {
		return err
	}
	if err := validateMySQLSchema(ctx, tx, false); err != nil {
		return err
	}
	var id, kind, category, field, detected, resolved string
	var data []byte
	var locator, revision, marker int64
	err = tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at, f.open_marker,
		t.id, t.record, t.revision FROM rff_integrity_findings f JOIN rff_timers t ON t.row_id = f.locator
		WHERE f.row_id = ? FOR UPDATE`, observed.ID).Scan(&kind, &locator, &category, &field, &detected, &resolved, &marker, &id, &data, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrConflict
	}
	if err != nil {
		return err
	}
	if kind != observed.Kind || locator != observed.Locator || category != observed.Category || field != observed.Field ||
		detected != observed.DetectedAt.Format(time.RFC3339Nano) {
		return transition.ErrConflict
	}
	if resolved != "" {
		if marker != observed.ID {
			return ErrUnsupportedSchema
		}
		var replay bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_audit FORCE INDEX (rff_integrity_audit_lookup)
			WHERE finding_id = ? AND revision = ? AND action = 'resolve' AND actor = ? AND reason = ?)`,
			observed.ID, observed.Revision, actor, reason).Scan(&replay); err != nil {
			return err
		}
		if !replay {
			return transition.ErrConflict
		}
		return tx.Commit()
	}
	if marker != 0 || revision != observed.Revision {
		return transition.ErrConflict
	}
	record, err := decodeTimer(id, data)
	if err != nil {
		return transition.ErrIntegrity
	}
	projection, err := timerProjectionFor(record)
	if err != nil {
		return transition.ErrIntegrity
	}
	projectionArgs := timerProjectionArgs(projection)
	projectionArgs = append(projectionArgs, locator)
	if _, err := tx.ExecContext(ctx, `UPDATE rff_timers SET deadline_seconds = ?, deadline_nanoseconds = ?,
		completed = ?, compatibility_blocked = ?, lease_expires_seconds = ?, lease_expires_nanoseconds = ?
		WHERE row_id = ?`, projectionArgs...); err != nil {
		return fmt.Errorf("rebuild timer projection: %w", err)
	}
	now := time.Now().UTC()
	if now.Before(observed.DetectedAt) {
		now = observed.DetectedAt
	}
	stamp := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_audit(finding_id, revision, action, actor, reason, occurred_at) VALUES (?, ?, 'resolve', ?, ?, ?)`, observed.ID, observed.Revision, actor, reason, stamp); err != nil {
		return fmt.Errorf("audit integrity resolution: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ?, open_marker = row_id WHERE row_id = ? AND open_marker = 0`, stamp, observed.ID)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return transition.ErrConflict
	}
	return tx.Commit()
}

type timerFindingError struct{}

func (timerFindingError) Error() string { return transition.ErrIntegrity.Error() }
func (timerFindingError) Unwrap() error { return transition.ErrIntegrity }

func recordTimerFinding(ctx context.Context, tx *sql.Tx, locator int64) error {
	if locator <= 0 {
		return fmt.Errorf("%w: invalid timer locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, category, field, detected_at, resolved_at, open_marker)
		SELECT 'timer', ?, 'invalid_timer', 'record', ?, '', 0 WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup) WHERE kind = 'timer' AND locator = ? AND open_marker = 0)`,
		locator, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist integrity finding: %w", err)
	}
	return timerFindingError{}
}

func timerQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
		WHERE kind = 'timer' AND locator = ? AND open_marker = 0)`, locator).Scan(&quarantined)
	return quarantined, err
}

func loadIntegrityTimer(ctx context.Context, tx *sql.Tx, id string) (transition.TimerRecord, error) {
	var locator, revision int64
	var data []byte
	var actual timerProjection
	err := tx.QueryRowContext(ctx, `SELECT row_id, record, revision, deadline_seconds, deadline_nanoseconds,
		completed, compatibility_blocked, lease_expires_seconds, lease_expires_nanoseconds
		FROM rff_timers WHERE id = ? FOR UPDATE`, id).Scan(&locator, &data, &revision, &actual.deadlineSeconds,
		&actual.deadlineNanoseconds, &actual.completed, &actual.compatibilityBlocked,
		&actual.leaseExpiresSeconds, &actual.leaseExpiresNanoseconds)
	if err != nil {
		return transition.TimerRecord{}, err
	}
	if locator <= 0 || revision <= 0 {
		return transition.TimerRecord{}, ErrUnsupportedSchema
	}
	quarantined, err := timerQuarantined(ctx, tx, locator)
	if err != nil {
		return transition.TimerRecord{}, err
	}
	if quarantined {
		return transition.TimerRecord{}, transition.ErrIntegrity
	}
	record, err := decodeTimer(id, data)
	if err != nil || !timerProjectionMatches(record, actual) {
		return transition.TimerRecord{}, recordTimerFinding(ctx, tx, locator)
	}
	return record, nil
}

func (s *Store[EntityID, StateID]) timerTransaction(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err := lockMySQLSchema(ctx, tx, true); err != nil {
		return err
	}
	if err := validateMySQLSchema(ctx, tx, true); err != nil {
		return err
	}
	var missing bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings f FORCE INDEX (rff_integrity_open_scope)
		WHERE f.kind = 'timer' AND f.open_marker = 0
		AND NOT EXISTS (SELECT 1 FROM rff_timers t WHERE t.row_id = f.locator))`).Scan(&missing); err != nil {
		return err
	}
	if missing {
		return transition.ErrIntegrity
	}
	result := fn(tx)
	var detected timerFindingError
	if result != nil && !errors.As(result, &detected) {
		return result
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit timer transaction: %w", err)
	}
	return result
}

// RebuildHookProjection restores hook selection facts from a valid durable record.
func (m *IntegrityMaintenance) RebuildHookProjection(ctx context.Context, actor, reason string, observed transition.IntegrityFinding) error {
	if err := m.checkAccess(ctx, "repair", actor, reason, observed.ID, observed.Locator); err != nil {
		return err
	}
	if observed.ID <= 0 || observed.Locator <= 0 || observed.Revision <= 0 ||
		observed.Kind != "hook" || observed.Category != "projection_mismatch" ||
		observed.Field != "selection" || observed.ResolvedAt != nil || observed.DetectedAt.IsZero() {
		return transition.ErrInvalidRequest
	}
	tx, err := m.store.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockMySQLSchema(ctx, tx, false); err != nil {
		return err
	}
	if err := validateMySQLSchema(ctx, tx, false); err != nil {
		return err
	}
	var kind, category, field, detected, resolved, executionID string
	var locator, revision, marker int64
	var data []byte
	err = tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at,
		f.open_marker, h.execution_id, h.work, h.revision FROM rff_integrity_findings f
		JOIN rff_hook_work h ON f.kind = 'hook' AND h.row_id = f.locator
		WHERE f.row_id = ? FOR UPDATE`, observed.ID).Scan(
		&kind, &locator, &category, &field, &detected, &resolved, &marker, &executionID, &data, &revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrConflict
	}
	if err != nil {
		return err
	}
	if kind != observed.Kind || locator != observed.Locator || category != observed.Category ||
		field != observed.Field || detected != observed.DetectedAt.Format(time.RFC3339Nano) ||
		resolved != "" || marker != 0 || revision != observed.Revision {
		return transition.ErrConflict
	}
	var work hookWork
	if err := json.Unmarshal(data, &work); err != nil {
		return transition.ErrIntegrity
	}
	if err := validateWork(executionID, work); err != nil {
		return transition.ErrIntegrity
	}
	projection := projectHookSelection(work)
	result, err := tx.ExecContext(ctx, `UPDATE rff_hook_work SET
		dispatch_kind = ?, started = ?, completed = ?, available_at_seconds = ?, available_at_nanoseconds = ?,
		lease_owner_present = ?, lease_expires_at_seconds = ?, lease_expires_at_nanoseconds = ?
		WHERE row_id = ? AND revision = ?`,
		projection.dispatchKind, projection.started, projection.completed, projection.availableSeconds,
		projection.availableNanoseconds, projection.leaseOwnerPresent, nullableInt64(projection.leaseExpiresSeconds),
		nullableInt64(projection.leaseExpiresNanoseconds), observed.Locator, observed.Revision)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return transition.ErrConflict
	}
	now := time.Now().UTC()
	if now.Before(observed.DetectedAt) {
		now = observed.DetectedAt
	}
	stamp := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_audit(finding_id, revision, action, actor, reason, occurred_at)
		VALUES (?, ?, 'rebuild_hook', ?, ?, ?)`, observed.ID, observed.Revision, actor, reason, stamp); err != nil {
		return fmt.Errorf("audit hook projection rebuild: %w", err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ?, open_marker = row_id
		WHERE row_id = ? AND open_marker = 0`, stamp, observed.ID)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return transition.ErrConflict
	}
	return tx.Commit()
}

func nullableInt64(value sql.NullInt64) any {
	if value.Valid {
		return value.Int64
	}
	return nil
}

type hookFindingError struct{}

func (hookFindingError) Error() string { return transition.ErrIntegrity.Error() }
func (hookFindingError) Unwrap() error { return transition.ErrIntegrity }

func recordHookFinding(ctx context.Context, tx *sql.Tx, locator int64, category, field string) error {
	if locator <= 0 || !((category == "projection_mismatch" && field == "selection") || (category == "invalid_hook" && field == "record")) {
		return fmt.Errorf("%w: invalid hook locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, category, field, detected_at, resolved_at, open_marker)
		SELECT 'hook', ?, ?, ?, ?, '', 0 WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
			WHERE kind = 'hook' AND locator = ? AND open_marker = 0)`,
		locator, category, field, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist hook integrity finding: %w", err)
	}
	return hookFindingError{}
}

func recordExecutionFinding(ctx context.Context, tx *sql.Tx, id, category, field string) error {
	var locator int64
	if err := tx.QueryRowContext(ctx, `SELECT row_id FROM rff_executions WHERE id = ? FOR UPDATE`, id).Scan(&locator); err != nil {
		return fmt.Errorf("locate inconsistent execution: %w", err)
	}
	if locator <= 0 {
		return fmt.Errorf("%w: invalid execution locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, category, field, detected_at, resolved_at, open_marker)
		SELECT 'execution', ?, ?, ?, ?, '', 0 WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
			WHERE kind = 'execution' AND locator = ? AND open_marker = 0)`,
		locator, category, field, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist execution integrity finding: %w", err)
	}
	return hookFindingError{}
}

func recordReservationFinding(ctx context.Context, tx *sql.Tx, locator int64) error {
	if locator <= 0 {
		return fmt.Errorf("%w: invalid reservation locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, category, field, detected_at, resolved_at, open_marker)
		SELECT 'reservation', ?, 'invalid_reservation', 'active', ?, '', 0 WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
			WHERE kind = 'reservation' AND locator = ? AND open_marker = 0)`,
		locator, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist reservation integrity finding: %w", err)
	}
	return hookFindingError{}
}

func hookQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
		WHERE kind = 'hook' AND locator = ? AND open_marker = 0)`, locator).Scan(&quarantined)
	return quarantined, err
}

func executionQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
		WHERE kind = 'execution' AND locator = ? AND open_marker = 0)`, locator).Scan(&quarantined)
	return quarantined, err
}

func reservationQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
		WHERE kind = 'reservation' AND locator = ? AND open_marker = 0)`, locator).Scan(&quarantined)
	return quarantined, err
}
