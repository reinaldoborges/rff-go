package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type executionRecoverySnapshot struct {
	RowID         int64
	ID            string
	Entity        []byte
	RequestKey    string
	Record        []byte
	Revision      int64
	Status        string
	Compatibility int
	Reservations  []recoveryReservation
	Leases        []recoveryLease
	Hooks         []recoveryHook
	Notifications []recoveryNotification
	Operations    []recoveryOperation
}

type recoveryReservation struct {
	RowID     int64
	Entity    []byte
	Execution string
	Revision  int64
}

type recoveryLease struct {
	Entity     []byte
	Execution  string
	Owner      string
	Generation int64
	ExpiresAt  string
}

type recoveryHook struct {
	RowID              int64
	Execution          string
	Work               []byte
	DispatchKind       string
	Started            bool
	Completed          bool
	AvailableSeconds   int64
	AvailableNanos     int64
	LeaseOwnerPresent  bool
	LeaseExpiresSecond sql.NullInt64
	LeaseExpiresNanos  sql.NullInt64
	Revision           int64
}

type recoveryNotification struct {
	RowID              int64
	Execution          string
	Work               []byte
	Completed          int64
	DueSeconds         int64
	DueNanos           int64
	LeaseOwnerPresent  int64
	LeaseExpiresSecond sql.NullInt64
	LeaseExpiresNanos  sql.NullInt64
	Revision           int64
}

type recoveryOperation struct {
	Key    []byte
	Record []byte
}

func loadExecutionRecoverySnapshot(ctx context.Context, tx *sql.Tx, locator int64) (executionRecoverySnapshot, error) {
	var snapshot executionRecoverySnapshot
	err := tx.QueryRowContext(ctx, `SELECT row_id, id, entity, request_key, record, revision, selection_status, selection_compatibility
		FROM rff_executions WHERE row_id = ?`, locator).Scan(&snapshot.RowID, &snapshot.ID, &snapshot.Entity,
		&snapshot.RequestKey, &snapshot.Record, &snapshot.Revision, &snapshot.Status, &snapshot.Compatibility)
	if errors.Is(err, sql.ErrNoRows) {
		return snapshot, transition.ErrConflict
	}
	if err != nil {
		return snapshot, err
	}
	if snapshot.RowID != locator || snapshot.Revision <= 0 || transition.ValidateDurableID(snapshot.ID) != nil ||
		len(snapshot.Entity) == 0 || snapshot.RequestKey == "" || snapshot.Compatibility < 0 || snapshot.Compatibility > 1 {
		return snapshot, transition.ErrIntegrity
	}

	rows, err := tx.QueryContext(ctx, `SELECT row_id, entity, execution_id, revision FROM rff_active
		WHERE execution_id = ? OR entity = ? ORDER BY row_id`, snapshot.ID, snapshot.Entity)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var reservation recoveryReservation
		if err := rows.Scan(&reservation.RowID, &reservation.Entity, &reservation.Execution, &reservation.Revision); err != nil {
			return snapshot, errors.Join(err, rows.Close())
		}
		if reservation.RowID <= 0 || reservation.Revision <= 0 || !bytes.Equal(reservation.Entity, snapshot.Entity) ||
			reservation.Execution != snapshot.ID || reservationCount(snapshot.Reservations, reservation.RowID) {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		snapshot.Reservations = append(snapshot.Reservations, reservation)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return snapshot, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT entity, execution_id, owner, generation, expires_at FROM rff_transition_leases
		WHERE execution_id = ? OR entity = ? ORDER BY entity, execution_id`, snapshot.ID, snapshot.Entity)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var lease recoveryLease
		if err := rows.Scan(&lease.Entity, &lease.Execution, &lease.Owner, &lease.Generation, &lease.ExpiresAt); err != nil {
			return snapshot, errors.Join(err, rows.Close())
		}
		if !bytes.Equal(lease.Entity, snapshot.Entity) || lease.Execution != snapshot.ID || lease.Owner == "" ||
			lease.Generation <= 0 || !validRecoveryTime(lease.ExpiresAt) {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		snapshot.Leases = append(snapshot.Leases, lease)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return snapshot, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT row_id, execution_id, work, dispatch_kind, started, completed,
		available_at_seconds, available_at_nanoseconds, lease_owner_present, lease_expires_at_seconds,
		lease_expires_at_nanoseconds, revision FROM rff_hook_work WHERE execution_id = ? ORDER BY row_id`, snapshot.ID)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var hook recoveryHook
		if err := rows.Scan(&hook.RowID, &hook.Execution, &hook.Work, &hook.DispatchKind, &hook.Started, &hook.Completed,
			&hook.AvailableSeconds, &hook.AvailableNanos, &hook.LeaseOwnerPresent, &hook.LeaseExpiresSecond,
			&hook.LeaseExpiresNanos, &hook.Revision); err != nil {
			return snapshot, errors.Join(err, rows.Close())
		}
		if hook.RowID <= 0 || hook.Revision <= 0 || hook.Execution != snapshot.ID ||
			!hook.LeaseExpiresSecond.Valid != !hook.LeaseExpiresNanos.Valid ||
			hook.LeaseOwnerPresent != hook.LeaseExpiresSecond.Valid {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		var work hookWork
		if json.Unmarshal(hook.Work, &work) != nil || validateWork(snapshot.ID, work) != nil {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		projection := projectHookSelection(work)
		if projection.dispatchKind != hook.DispatchKind || projection.started != hook.Started ||
			projection.completed != hook.Completed || projection.availableSeconds != hook.AvailableSeconds ||
			projection.availableNanoseconds != hook.AvailableNanos || projection.leaseOwnerPresent != hook.LeaseOwnerPresent ||
			projection.leaseExpiresSeconds != hook.LeaseExpiresSecond || projection.leaseExpiresNanoseconds != hook.LeaseExpiresNanos {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		snapshot.Hooks = append(snapshot.Hooks, hook)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return snapshot, err
	}
	if len(snapshot.Hooks) > 1 {
		return snapshot, transition.ErrIntegrity
	}

	rows, err = tx.QueryContext(ctx, `SELECT row_id, execution_id, work, completed, due_at_seconds,
		due_at_nanoseconds, lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds, revision
		FROM rff_notification_work WHERE execution_id = ? ORDER BY row_id`, snapshot.ID)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var notification recoveryNotification
		if err := rows.Scan(&notification.RowID, &notification.Execution, &notification.Work, &notification.Completed,
			&notification.DueSeconds, &notification.DueNanos, &notification.LeaseOwnerPresent,
			&notification.LeaseExpiresSecond, &notification.LeaseExpiresNanos, &notification.Revision); err != nil {
			return snapshot, errors.Join(err, rows.Close())
		}
		if notification.RowID <= 0 || notification.Revision <= 0 || notification.Execution != snapshot.ID ||
			!notification.LeaseExpiresSecond.Valid != !notification.LeaseExpiresNanos.Valid ||
			(notification.LeaseOwnerPresent != 0) != notification.LeaseExpiresSecond.Valid {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		var work notificationWork
		if json.Unmarshal(notification.Work, &work) != nil {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		if !notificationProjectionMatches(work, notificationProjection{
			completed: notification.Completed, dueSeconds: notification.DueSeconds, dueNanoseconds: notification.DueNanos,
			leaseOwnerPresent: notification.LeaseOwnerPresent, leaseExpiresAtSeconds: notification.LeaseExpiresSecond,
			leaseExpiresAtNanosecs: notification.LeaseExpiresNanos,
		}) {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		snapshot.Notifications = append(snapshot.Notifications, notification)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return snapshot, err
	}
	if len(snapshot.Notifications) > 1 {
		return snapshot, transition.ErrIntegrity
	}

	rows, err = tx.QueryContext(ctx, `SELECT operation_key, record FROM rff_operations WHERE execution_id = ? ORDER BY operation_key`, snapshot.ID)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var operation recoveryOperation
		if err := rows.Scan(&operation.Key, &operation.Record); err != nil {
			return snapshot, errors.Join(err, rows.Close())
		}
		if len(operation.Key) == 0 {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		if _, err := decodeOperation(snapshot.ID, operation.Key, operation.Record); err != nil {
			return snapshot, errors.Join(transition.ErrIntegrity, rows.Close())
		}
		snapshot.Operations = append(snapshot.Operations, operation)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return snapshot, err
	}

	for _, dependency := range append(
		append([]struct {
			kind    string
			locator int64
		}{}, recoveryFindingLocators(snapshot)...),
		struct {
			kind    string
			locator int64
		}{kind: "execution", locator: locator},
	) {
		if dependency.kind == "execution" {
			continue
		}
		var open bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
			WHERE kind = ? AND locator = ? AND resolved_at IS NULL)`, dependency.kind, dependency.locator).Scan(&open); err != nil {
			return snapshot, err
		}
		if open {
			return snapshot, transition.ErrIntegrity
		}
	}
	return snapshot, nil
}

func reservationCount(reservations []recoveryReservation, rowID int64) bool {
	for _, reservation := range reservations {
		if reservation.RowID == rowID {
			return true
		}
	}
	return false
}

func recoveryFindingLocators(snapshot executionRecoverySnapshot) []struct {
	kind    string
	locator int64
} {
	result := make([]struct {
		kind    string
		locator int64
	}, 0, len(snapshot.Reservations)+len(snapshot.Hooks)+len(snapshot.Notifications))
	for _, reservation := range snapshot.Reservations {
		result = append(result, struct {
			kind    string
			locator int64
		}{kind: "reservation", locator: reservation.RowID})
	}
	for _, hook := range snapshot.Hooks {
		result = append(result, struct {
			kind    string
			locator int64
		}{kind: "hook", locator: hook.RowID})
	}
	for _, notification := range snapshot.Notifications {
		result = append(result, struct {
			kind    string
			locator int64
		}{kind: "notification", locator: notification.RowID})
	}
	return result
}

func validRecoveryTime(value string) bool {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && !parsed.IsZero()
}

func executionRecoveryDigest(snapshot executionRecoverySnapshot) ([32]byte, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func validateRestoredExecution(record diskRecord, id string, entity []byte, key string) error {
	if transition.ValidateDurableID(record.ID) != nil || record.ID != id || !bytes.Equal(record.Entity, entity) ||
		record.Key != key || !transition.SupportedExecutionSchema(record.SchemaVersion) ||
		(record.SchemaVersion == 1 && record.Continuation != nil) || transition.ValidateContinuationSnapshot(record.Continuation) != nil {
		return transition.ErrIntegrity
	}
	if record.Compatibility != nil && !validDiagnostic(record, *record.Compatibility) {
		return transition.ErrIntegrity
	}
	if (record.OriginalRequest == nil) != (len(record.Migrations) == 0) {
		return transition.ErrIntegrity
	}
	seen := make(map[string]bool)
	for _, migration := range record.Migrations {
		if migration.Request.ID == "" || migration.Request.MachineID == "" || migration.Request.MachineVersion == "" ||
			len(migration.ParametersDigest) != 64 || len(migration.Request.Parameters) != 0 ||
			migration.AcceptedAt.IsZero() || seen[migration.Request.ID] {
			return transition.ErrIntegrity
		}
		seen[migration.Request.ID] = true
	}
	switch record.Status {
	case transition.Waiting, transition.Running, transition.Notifying, transition.NotificationRepair,
		transition.NeedsIntervention, transition.CommitRejected, transition.Aborting, transition.Complete, transition.Aborted:
	default:
		return transition.ErrIntegrity
	}
	return nil
}

// RestoreExecution replaces unreadable authoritative bytes only with a
// host-supplied trusted record that matches the immutable execution identity.
// The original bytes, restoration, audit, and finding resolution commit together.
func (m *IntegrityMaintenance) RestoreExecution(
	ctx context.Context, actor, reason string, observed transition.IntegrityFinding, trustedRecord []byte,
) error {
	if err := m.checkAccess(ctx, "restore_execution", actor, reason, observed.ID, observed.Locator); err != nil {
		return err
	}
	if observed.ID <= 0 || observed.Locator <= 0 || observed.Revision <= 0 ||
		observed.Kind != "execution" || observed.Category != "invalid_execution" || observed.Field != "record" ||
		observed.ResolvedAt != nil || observed.DetectedAt.IsZero() || len(trustedRecord) == 0 {
		return transition.ErrInvalidRequest
	}
	if !observed.DependenciesKnown || observed.DependencyDigest == ([32]byte{}) {
		return transition.ErrIntegrity
	}
	var restored diskRecord
	if err := json.Unmarshal(trustedRecord, &restored); err != nil {
		return transition.ErrIntegrity
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return err
	}

	var kind, category, field, detected string
	var resolved sql.NullString
	var id, key string
	var entity, currentRecord []byte
	var locator, revision, compatibility int64
	var status string
	err = tx.QueryRowContext(ctx, `SELECT f.kind, f.locator, f.category, f.field, f.detected_at, f.resolved_at,
		e.id, e.entity, e.request_key, e.record, e.revision, e.selection_status, e.selection_compatibility
		FROM rff_integrity_findings f JOIN rff_executions e ON e.row_id = f.locator
		WHERE f.finding_id = ?`, observed.ID).Scan(&kind, &locator, &category, &field, &detected, &resolved,
		&id, &entity, &key, &currentRecord, &revision, &status, &compatibility)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrConflict
	}
	if err != nil {
		return err
	}
	if kind != observed.Kind || locator != observed.Locator || category != observed.Category ||
		field != observed.Field || detected != observed.DetectedAt.Format(time.RFC3339Nano) {
		return transition.ErrConflict
	}
	if resolved.Valid {
		return m.restoreReplay(ctx, tx, actor, reason, observed, trustedRecord, currentRecord)
	}
	if revision != observed.Revision {
		return transition.ErrConflict
	}
	if err := validateRestoredExecution(restored, id, entity, key); err != nil {
		return err
	}
	snapshot, err := loadExecutionRecoverySnapshot(ctx, tx, locator)
	if err != nil {
		return err
	}
	digest, err := executionRecoveryDigest(snapshot)
	if err != nil {
		return err
	}
	if digest != observed.DependencyDigest {
		return transition.ErrConflict
	}
	if err := validateExecutionRecoveryDependencies(restored, snapshot); err != nil {
		return err
	}
	if err := ensureExecutionArchive(ctx, tx); err != nil {
		return err
	}
	recordDigest := sha256.Sum256(currentRecord)
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_execution_archive
		(finding_id, locator, revision, record_digest, dependency_digest, record)
		VALUES (?, ?, ?, ?, ?, ?)`, observed.ID, locator, revision, recordDigest[:], digest[:], currentRecord); err != nil {
		return fmt.Errorf("archive unreadable execution evidence: %w", err)
	}
	compatibilityValue := 0
	if restored.Compatibility != nil {
		compatibilityValue = 1
	}
	result, err := tx.ExecContext(ctx, `UPDATE rff_executions SET record = ?, selection_status = ?,
		selection_compatibility = ? WHERE row_id = ? AND revision = ?`,
		trustedRecord, restored.Status, compatibilityValue, locator, revision)
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_audit
		(finding_id, revision, action, actor, reason, occurred_at)
		VALUES (?, ?, 'restore_execution', ?, ?, ?)`, observed.ID, observed.Revision, actor, reason, stamp); err != nil {
		return fmt.Errorf("audit execution restoration: %w", err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE rff_integrity_findings SET resolved_at = ?
		WHERE finding_id = ? AND resolved_at IS NULL`, stamp, observed.ID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return transition.ErrConflict
	}
	return tx.Commit()
}

func validateExecutionRecoveryDependencies(record diskRecord, snapshot executionRecoverySnapshot) error {
	terminal := record.Status == transition.Complete || record.Status == transition.Aborted
	if terminal == (len(snapshot.Reservations) != 0) {
		return transition.ErrIntegrity
	}
	for _, reservation := range snapshot.Reservations {
		if reservation.Execution != record.ID || !bytes.Equal(reservation.Entity, record.Entity) {
			return transition.ErrIntegrity
		}
	}
	if len(snapshot.Hooks) > 0 {
		var work hookWork
		if err := json.Unmarshal(snapshot.Hooks[0].Work, &work); err != nil {
			return transition.ErrIntegrity
		}
		if work.Started && !work.Progress.Completed {
			if work.Progress.Dispatch.Kind == transition.HookRecover {
				if record.Status != transition.Aborting {
					return transition.ErrIntegrity
				}
			} else if record.Status != transition.Running {
				return transition.ErrIntegrity
			}
		}
	}
	for _, notification := range snapshot.Notifications {
		var work notificationWork
		if err := json.Unmarshal(notification.Work, &work); err != nil || validateNotification(record, work) != nil {
			return transition.ErrIntegrity
		}
	}
	return nil
}

func ensureExecutionArchive(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		integrityExecutionArchiveDDL,
		integrityExecutionArchiveUpdateGuardDDL,
		integrityExecutionArchiveDeleteGuardDDL,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			var exists int
			name := "rff_integrity_execution_archive"
			switch statement {
			case integrityExecutionArchiveUpdateGuardDDL:
				name = "rff_integrity_execution_archive_update_guard"
			case integrityExecutionArchiveDeleteGuardDDL:
				name = "rff_integrity_execution_archive_delete_guard"
			}
			if checkErr := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, name).Scan(&exists); checkErr != nil || exists != 1 {
				return fmt.Errorf("create execution evidence archive: %w", err)
			}
		}
	}
	return validateIntegrityExecutionArchiveStructure(ctx, tx)
}

func (m *IntegrityMaintenance) restoreReplay(
	ctx context.Context, tx *sql.Tx, actor, reason string, observed transition.IntegrityFinding, trustedRecord, currentRecord []byte,
) error {
	if !bytes.Equal(trustedRecord, currentRecord) {
		return transition.ErrConflict
	}
	var replay bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_audit INDEXED BY rff_integrity_audit_lookup
		WHERE finding_id = ? AND revision = ? AND action = 'restore_execution' AND actor = ? AND reason = ?)`,
		observed.ID, observed.Revision, actor, reason).Scan(&replay); err != nil {
		return err
	}
	if !replay {
		return transition.ErrConflict
	}
	return tx.Commit()
}

// ArchivedExecutionEvidence returns original bytes only through an authorized
// worker-free maintenance action; ordinary runtime APIs never expose this data.
func (m *IntegrityMaintenance) ArchivedExecutionEvidence(
	ctx context.Context, actor, reason string, findingID int64,
) (transition.ArchivedExecutionEvidence, error) {
	var evidence transition.ArchivedExecutionEvidence
	if findingID <= 0 {
		return evidence, transition.ErrInvalidRequest
	}
	if err := m.checkAccess(ctx, "evidence", actor, reason, findingID, 0); err != nil {
		return evidence, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return evidence, err
	}
	defer tx.Rollback()
	if err := validateIntegrityStructure(ctx, tx, true); err != nil {
		return evidence, err
	}
	if err := validateIntegrityExecutionArchiveStructure(ctx, tx); err != nil {
		return evidence, err
	}
	var archiveExists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'rff_integrity_execution_archive'`).Scan(&archiveExists); err != nil {
		return evidence, err
	}
	if archiveExists == 0 {
		return evidence, transition.ErrConflict
	}
	var recordDigest, dependencyDigest []byte
	err = tx.QueryRowContext(ctx, `SELECT a.finding_id, a.locator, a.revision, a.record_digest, a.dependency_digest, a.record
		FROM rff_integrity_execution_archive a JOIN rff_integrity_findings f ON f.finding_id = a.finding_id
		WHERE a.finding_id = ? AND f.kind = 'execution' AND f.category = 'invalid_execution' AND f.field = 'record'`,
		findingID).Scan(
		&evidence.FindingID, &evidence.Locator, &evidence.Revision, &recordDigest, &dependencyDigest, &evidence.Record,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return evidence, transition.ErrConflict
	}
	if err != nil {
		return evidence, err
	}
	if evidence.FindingID != findingID || evidence.Locator <= 0 || evidence.Revision <= 0 ||
		len(recordDigest) != sha256.Size || len(dependencyDigest) != sha256.Size {
		return transition.ArchivedExecutionEvidence{}, ErrUnsupportedSchema
	}
	evidence.RecordDigest = sha256.Sum256(evidence.Record)
	if !bytes.Equal(evidence.RecordDigest[:], recordDigest) {
		return transition.ArchivedExecutionEvidence{}, ErrUnsupportedSchema
	}
	copy(evidence.DependencyDigest[:], dependencyDigest)
	evidence.Record = bytes.Clone(evidence.Record)
	if err := tx.Commit(); err != nil {
		return transition.ArchivedExecutionEvidence{}, err
	}
	return evidence, nil
}
