package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// IntegrityMaintenance is a worker-free, host-authorized maintenance handle.
type IntegrityMaintenance struct {
	db        *sql.DB
	authorize transition.IntegrityAuthorizer
}

// OpenIntegrityMaintenance opens an existing store without initializing workers
// or reading authoritative records. The authorizer is invoked on every action.
func OpenIntegrityMaintenance(path string, authorize transition.IntegrityAuthorizer) (*IntegrityMaintenance, error) {
	if path == "" || path == ":memory:" || authorize == nil {
		return nil, transition.ErrInvalidRequest
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	uri := url.URL{Scheme: "file", Path: absolute}
	query := uri.Query()
	query.Set("mode", "rw")
	query.Set("_txlock", "immediate")
	query.Set("_busy_timeout", "5000")
	query.Set("_foreign_keys", "on")
	query.Set("_synchronous", "FULL")
	uri.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &IntegrityMaintenance{db: db, authorize: authorize}, nil
}

// Close releases the maintenance connection.
func (m *IntegrityMaintenance) Close() error { return m.db.Close() }

func validateIntegrityStructure(ctx context.Context, tx *sql.Tx, full bool) error {
	definitions := map[string]string{
		"rff_schema_version": schemaVersionDDL, "rff_timers": timersDDL, "rff_timer_revision": timerRevisionDDL, "rff_timer_revision_guard": timerRevisionGuardDDL, "rff_timer_locator": timerLocatorDDL,
		"rff_timer_allocation": timerAllocationDDL, "rff_timer_deadline_lookup": timerDeadlineIndexDDL,
		"rff_integrity_findings":    integrityFindingsDDL,
		"rff_integrity_open_lookup": integrityIndexDDL, "rff_integrity_history_lookup": integrityHistoryIndexDDL,
		"rff_integrity_input_channel_lookup": integrityChannelIndexDDL,
		"rff_integrity_audit":                integrityAuditDDL, "rff_integrity_audit_lookup": integrityAuditIndexDDL,
	}
	if full {
		for name, ddl := range map[string]string{
			"rff_executions": executionsDDL,
			"rff_active":     activeDDL, "rff_transition_leases": leasesDDL,
			"rff_hook_work": hookWorkDDL, "rff_notification_work": notificationWorkDDL,
			"rff_inputs": inputsDDL, "rff_continuations": continuationsDDL,
			"rff_successors": successorsDDL, "rff_operations": operationsDDL,
			"rff_input_selection_lookup": inputSelectionIndexDDL,
			"rff_input_revision":         inputRevisionDDL, "rff_input_revision_guard": inputRevisionGuardDDL,
			"rff_input_locator": inputLocatorDDL, "rff_input_allocation": inputAllocationDDL,
			"rff_execution_revision": executionRevisionDDL, "rff_execution_revision_guard": executionRevisionGuardDDL,
			"rff_execution_locator": executionLocatorDDL, "rff_execution_allocation": executionAllocationDDL,
			"rff_active_revision": activeRevisionDDL, "rff_active_revision_guard": activeRevisionGuardDDL,
			"rff_active_locator": activeLocatorDDL, "rff_active_allocation": activeAllocationDDL,
			"rff_hook_revision": hookRevisionDDL, "rff_hook_revision_guard": hookRevisionGuardDDL,
			"rff_hook_locator": hookLocatorDDL, "rff_hook_allocation": hookAllocationDDL,
			"rff_execution_hook_state_lookup": executionHookIndexDDL, "rff_hook_ready_lookup": hookReadyIndexDDL,
		} {
			definitions[name] = ddl
		}
	}
	for name, expected := range definitions {
		var actual string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE name = ?`, name).Scan(&actual); err != nil || actual != expected {
			return fmt.Errorf("%w: incompatible integrity structure", ErrUnsupportedSchema)
		}
	}
	var version, count int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0), COUNT(*) FROM rff_schema_version`).Scan(&version, &count); err != nil || version != adapterSchemaVersion || count != 1 {
		return fmt.Errorf("%w: incompatible schema version", ErrUnsupportedSchema)
	}
	if full {
		return validateIdentityIndexes(ctx, tx)
	}
	return nil
}

func (m *IntegrityMaintenance) checkAccess(ctx context.Context, action, actor, reason string, findingID, locator int64) error {
	if actor == "" || reason == "" {
		return transition.ErrInvalidRequest
	}
	if err := m.authorize(ctx, action, actor, reason, findingID, locator); err != nil {
		return err
	}
	return nil
}

func validIntegrityFinding(finding transition.IntegrityFinding) bool {
	switch finding.Kind {
	case "input":
		return finding.Category == "invalid_input" && (finding.Field == "record" || finding.Field == "projection")
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

func (m *IntegrityMaintenance) findings(ctx context.Context, actor, reason string, afterID int64, limit int) (result []transition.IntegrityFinding, err error) {
	if err := m.checkAccess(ctx, "inspect", actor, reason, 0, 0); err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT f.finding_id, f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at,
		CASE f.kind WHEN 'input' THEN f.finding_revision WHEN 'timer' THEN COALESCE(t.revision, 0) WHEN 'hook' THEN COALESCE(h.revision, 0)
			WHEN 'execution' THEN COALESCE(e.revision, 0) WHEN 'reservation' THEN COALESCE(a.revision, 0) ELSE 0 END
		FROM rff_integrity_findings AS f
		LEFT JOIN rff_timers AS t ON f.kind = 'timer' AND t.row_id = f.locator
		LEFT JOIN rff_hook_work AS h ON f.kind = 'hook' AND h.row_id = f.locator
		LEFT JOIN rff_executions AS e ON f.kind = 'execution' AND e.row_id = f.locator
		LEFT JOIN rff_active AS a ON f.kind = 'reservation' AND a.row_id = f.locator
		WHERE f.finding_id > ? ORDER BY f.finding_id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("read integrity findings: %w", err)
	}
	for rows.Next() {
		var finding transition.IntegrityFinding
		var detected string
		var resolved sql.NullString
		if err := rows.Scan(&finding.ID, &finding.Kind, &finding.Locator, &finding.Category, &finding.Field, &detected, &resolved, &finding.Revision); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		finding.DetectedAt, err = time.Parse(time.RFC3339Nano, detected)
		if err != nil || finding.ID <= 0 || finding.Locator <= 0 || finding.Revision < 0 || !validIntegrityFinding(finding) {
			return nil, errors.Join(ErrUnsupportedSchema, rows.Close())
		}
		if resolved.Valid {
			t, parseErr := time.Parse(time.RFC3339Nano, resolved.String)
			if parseErr != nil || t.Before(finding.DetectedAt) {
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
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT audit_id, finding_id, revision, action, actor, reason, occurred_at
		FROM rff_integrity_audit INDEXED BY rff_integrity_audit_lookup WHERE finding_id = ? AND audit_id > ? ORDER BY audit_id LIMIT ?`, findingID, afterID, limit)
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

// Resolve revalidates the observed durable record and atomically records its resolution.
func (m *IntegrityMaintenance) Resolve(ctx context.Context, actor, reason string, observed transition.IntegrityFinding) error {
	if err := m.checkAccess(ctx, "resolve", actor, reason, observed.ID, observed.Locator); err != nil {
		return err
	}
	validTimer := observed.Kind == "timer" && observed.Category == "invalid_timer" && observed.Field == "record"
	validInput := observed.Kind == "input" && observed.Category == "invalid_input" &&
		(observed.Field == "record" || observed.Field == "projection")
	if observed.ID <= 0 || observed.Locator <= 0 || observed.Revision <= 0 || (!validTimer && !validInput) ||
		observed.ResolvedAt != nil || observed.DetectedAt.IsZero() {
		return transition.ErrInvalidRequest
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return err
	}
	if validInput {
		return m.resolveInput(ctx, tx, actor, reason, observed)
	}
	var id string
	var data []byte
	var locator, revision int64
	var kind, category, field, detected string
	var resolved sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at, t.id, t.record, t.revision
		FROM rff_integrity_findings f JOIN rff_timers t ON t.row_id = f.locator
		WHERE f.finding_id = ?`, observed.ID).Scan(&kind, &locator, &category, &field, &detected, &resolved, &id, &data, &revision)
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
	if resolved.Valid {
		var replay bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_audit INDEXED BY rff_integrity_audit_lookup
			WHERE finding_id = ? AND revision = ? AND action = 'resolve' AND actor = ? AND reason = ?)`,
			observed.ID, observed.Revision, actor, reason).Scan(&replay); err != nil {
			return err
		}
		if !replay {
			return transition.ErrConflict
		}
		return tx.Commit()
	}

	if revision != observed.Revision {
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
	result, err := tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ? WHERE finding_id = ? AND resolved_at IS NULL`, stamp, observed.ID)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return transition.ErrConflict
	}
	return tx.Commit()
}

func (m *IntegrityMaintenance) resolveInput(ctx context.Context, tx *sql.Tx, actor, reason string, observed transition.IntegrityFinding) error {
	var id, kind, category, field, detected string
	var resolved sql.NullString
	var data []byte
	var locator, findingRevision, revision, outcomePresent int64
	err := tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at, f.finding_revision,
		i.id, i.record, i.outcome_present, i.revision
		FROM rff_integrity_findings f JOIN rff_inputs i ON i.row_id = f.locator WHERE f.finding_id = ?`, observed.ID).
		Scan(&kind, &locator, &category, &field, &detected, &resolved, &findingRevision, &id, &data, &outcomePresent, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrConflict
	}
	if err != nil {
		return err
	}
	if kind != observed.Kind || locator != observed.Locator || category != observed.Category || field != observed.Field ||
		detected != observed.DetectedAt.Format(time.RFC3339Nano) || findingRevision != observed.Revision {
		return transition.ErrConflict
	}
	if resolved.Valid {
		var replay bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_audit INDEXED BY rff_integrity_audit_lookup
			WHERE finding_id = ? AND revision = ? AND action = 'resolve' AND actor = ? AND reason = ?)`,
			observed.ID, observed.Revision, actor, reason).Scan(&replay); err != nil {
			return err
		}
		if !replay {
			return transition.ErrConflict
		}
		return tx.Commit()
	}
	if revision != observed.Revision {
		return transition.ErrConflict
	}
	record, err := decodeInput(id, data)
	if err != nil {
		return transition.ErrIntegrity
	}
	presence := 0
	if record.Outcome != nil {
		presence = 1
	}
	if outcomePresent != 0 && outcomePresent != 1 {
		return ErrUnsupportedSchema
	}
	result, err := tx.ExecContext(ctx, `UPDATE rff_inputs SET channel = ?, outcome_present = ? WHERE row_id = ? AND revision = ?`,
		[]byte(record.Channel), presence, locator, observed.Revision)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return transition.ErrConflict
	}
	now := time.Now().UTC()
	if now.Before(observed.DetectedAt) {
		now = observed.DetectedAt
	}
	stamp := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_audit(finding_id, revision, action, actor, reason, occurred_at) VALUES (?, ?, 'resolve', ?, ?, ?)`,
		observed.ID, observed.Revision, actor, reason, stamp); err != nil {
		return fmt.Errorf("audit input integrity resolution: %w", err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ? WHERE finding_id = ? AND resolved_at IS NULL`, stamp, observed.ID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
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

	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at, resolved_at)
		SELECT 'timer', ?, 0, 'invalid_timer', 'record', X'', ?, NULL WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup WHERE kind = 'timer' AND locator = ? AND resolved_at IS NULL)`,
		locator, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist integrity finding: %w", err)
	}
	return timerFindingError{}
}

func timerQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
		WHERE kind = 'timer' AND locator = ? AND resolved_at IS NULL)`, locator).Scan(&quarantined)
	return quarantined, err
}

func loadIntegrityTimer(ctx context.Context, tx *sql.Tx, id string) (transition.TimerRecord, error) {
	var locator, revision int64
	var data []byte
	var actual timerProjection
	err := tx.QueryRowContext(ctx, `SELECT row_id, record, revision, deadline_seconds, deadline_nanoseconds,
		completed, compatibility_blocked, lease_expires_seconds, lease_expires_nanoseconds
		FROM rff_timers WHERE id = ?`, id).Scan(&locator, &data, &revision, &actual.deadlineSeconds,
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err := validateIntegrityStructure(ctx, tx, false); err != nil {
		return err
	}
	var missing bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings f INDEXED BY rff_integrity_open_lookup
		WHERE f.kind = 'timer' AND f.resolved_at IS NULL
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
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return err
	}
	var kind, category, field, detected, executionID string
	var resolved sql.NullString
	var locator, revision int64
	var data []byte
	err = tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at,
		h.execution_id, h.work, h.revision FROM rff_integrity_findings f
		JOIN rff_hook_work h ON f.kind = 'hook' AND h.row_id = f.locator
		WHERE f.finding_id = ?`, observed.ID).Scan(
		&kind, &locator, &category, &field, &detected, &resolved, &executionID, &data, &revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrConflict
	}
	if err != nil {
		return err
	}
	if kind != observed.Kind || locator != observed.Locator || category != observed.Category ||
		field != observed.Field || detected != observed.DetectedAt.Format(time.RFC3339Nano) ||
		resolved.Valid || revision != observed.Revision {
		return transition.ErrConflict
	}
	var work hookWork
	if err := json.Unmarshal(data, &work); err != nil || validateWork(executionID, work) != nil {
		return transition.ErrIntegrity
	}
	projection := projectHookSelection(work)
	if _, err := tx.ExecContext(ctx, `UPDATE rff_hook_work SET
		dispatch_kind = ?, started = ?, completed = ?, available_at_seconds = ?, available_at_nanoseconds = ?,
		lease_owner_present = ?, lease_expires_at_seconds = ?, lease_expires_at_nanoseconds = ?
		WHERE row_id = ? AND revision = ?`,
		projection.dispatchKind, projection.started, projection.completed, projection.availableSeconds,
		projection.availableNanoseconds, projection.leaseOwnerPresent, nullableInt64(projection.leaseExpiresSeconds),
		nullableInt64(projection.leaseExpiresNanoseconds), observed.Locator, observed.Revision); err != nil {
		return err
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
	result, err := tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ?
		WHERE finding_id = ? AND resolved_at IS NULL`, stamp, observed.ID)
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at)
		SELECT 'hook', ?, 0, ?, ?, X'', ? WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
			WHERE kind = 'hook' AND locator = ? AND resolved_at IS NULL)`,
		locator, category, field, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist hook integrity finding: %w", err)
	}
	return hookFindingError{}
}

func recordExecutionFinding(ctx context.Context, tx *sql.Tx, id, category, field string) error {
	var locator int64
	if err := tx.QueryRowContext(ctx, `SELECT row_id FROM rff_executions WHERE id = ?`, id).Scan(&locator); err != nil {
		return fmt.Errorf("locate inconsistent execution: %w", err)
	}
	if locator <= 0 {
		return fmt.Errorf("%w: invalid execution locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at)
		SELECT 'execution', ?, 0, ?, ?, X'', ? WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
			WHERE kind = 'execution' AND locator = ? AND resolved_at IS NULL)`,
		locator, category, field, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist execution integrity finding: %w", err)
	}
	return hookFindingError{}
}

func recordReservationFinding(ctx context.Context, tx *sql.Tx, locator int64) error {
	if locator <= 0 {
		return fmt.Errorf("%w: invalid reservation locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at)
		SELECT 'reservation', ?, 0, 'invalid_reservation', 'active', X'', ? WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
			WHERE kind = 'reservation' AND locator = ? AND resolved_at IS NULL)`,
		locator, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist reservation integrity finding: %w", err)
	}
	return hookFindingError{}
}

func hookQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
		WHERE kind = 'hook' AND locator = ? AND resolved_at IS NULL)`, locator).Scan(&quarantined)
	return quarantined, err
}

func executionQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
		WHERE kind = 'execution' AND locator = ? AND resolved_at IS NULL)`, locator).Scan(&quarantined)
	return quarantined, err
}

func reservationQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
		WHERE kind = 'reservation' AND locator = ? AND resolved_at IS NULL)`, locator).Scan(&quarantined)
	return quarantined, err
}
