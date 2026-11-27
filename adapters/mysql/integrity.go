package mysql

import (
	"context"
	"database/sql"
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
		f.resolved_at, f.open_marker, CASE WHEN f.kind = 'input' THEN f.finding_revision ELSE COALESCE(t.revision, 0) END
		FROM rff_integrity_findings f LEFT JOIN rff_timers t ON f.kind = 'timer' AND t.row_id = f.locator
		LEFT JOIN rff_inputs i ON f.kind = 'input' AND i.row_id = f.locator
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
		validTimer := finding.Kind == "timer" && finding.Category == "invalid_timer" && finding.Field == "record"
		validInput := finding.Kind == "input" && finding.Category == "invalid_input" &&
			(finding.Field == "record" || finding.Field == "projection")
		if err != nil || finding.ID <= 0 || finding.Locator <= 0 || finding.Revision < 0 || (!validTimer && !validInput) ||
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
		if err != nil || audit.ID <= 0 || audit.FindingID != findingID || audit.Revision <= 0 || audit.Action != "resolve" || audit.Actor == "" || audit.Reason == "" {
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
	if validInput {
		return m.resolveInput(ctx, tx, actor, reason, observed)
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
	if _, err := decodeTimer(id, data); err != nil {
		return transition.ErrIntegrity
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

func (m *IntegrityMaintenance) resolveInput(ctx context.Context, tx *sql.Tx, actor, reason string, observed transition.IntegrityFinding) error {
	var id, kind, category, field, detected, resolved string
	var data []byte
	var locator, findingRevision, revision, marker, outcomePresent int64
	err := tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at, f.open_marker, f.finding_revision,
		i.id, i.record, i.outcome_present, i.revision
		FROM rff_integrity_findings f JOIN rff_inputs i ON i.row_id = f.locator
		WHERE f.row_id = ? FOR UPDATE`, observed.ID).
		Scan(&kind, &locator, &category, &field, &detected, &resolved, &marker, &findingRevision, &id, &data, &outcomePresent, &revision)
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
	result, err = tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ?, open_marker = row_id WHERE row_id = ? AND open_marker = 0`, stamp, observed.ID)
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at, resolved_at, open_marker)
		SELECT 'timer', ?, 0, 'invalid_timer', 'record', _binary'', ?, '', 0 WHERE NOT EXISTS
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
	err := tx.QueryRowContext(ctx, `SELECT row_id, record, revision FROM rff_timers WHERE id = ? FOR UPDATE`, id).Scan(&locator, &data, &revision)
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
	if err != nil {
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
