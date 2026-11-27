package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type notificationWork struct {
	Version       int
	IntentID      string
	DueAt         time.Time
	Owner         string
	Generation    uint64
	ExpiresAt     time.Time
	Attempts      uint64
	Completed     bool
	Compatibility *transition.CompatibilityDiagnostic
}

type notificationProjection struct {
	completed              int64
	dueSeconds             int64
	dueNanoseconds         int64
	leaseOwnerPresent      int64
	leaseExpiresAtSeconds  sql.NullInt64
	leaseExpiresAtNanosecs sql.NullInt64
}

func notificationProjectionFor(work notificationWork) (notificationProjection, error) {
	if work.DueAt.IsZero() || (work.Owner == "") != work.ExpiresAt.IsZero() {
		return notificationProjection{}, ErrUnsupportedSchema
	}
	projection := notificationProjection{
		completed:         boolInt(work.Completed),
		dueSeconds:        work.DueAt.Unix(),
		dueNanoseconds:    int64(work.DueAt.Nanosecond()),
		leaseOwnerPresent: boolInt(work.Owner != ""),
	}
	if work.Owner != "" {
		projection.leaseExpiresAtSeconds = sql.NullInt64{Int64: work.ExpiresAt.Unix(), Valid: true}
		projection.leaseExpiresAtNanosecs = sql.NullInt64{Int64: int64(work.ExpiresAt.Nanosecond()), Valid: true}
	}
	return projection, nil
}

func notificationProjectionMatches(work notificationWork, actual notificationProjection) bool {
	expected, err := notificationProjectionFor(work)
	return err == nil && expected == actual
}

func notificationProjectionArgs(projection notificationProjection) []any {
	return []any{
		projection.completed, projection.dueSeconds, projection.dueNanoseconds, projection.leaseOwnerPresent,
		nullableInt64(projection.leaseExpiresAtSeconds), nullableInt64(projection.leaseExpiresAtNanosecs),
	}
}

func validateNotification(execution diskRecord, work notificationWork) error {
	if work.Version != 1 || work.IntentID == "" || work.IntentID != execution.NotificationIntentID ||
		!execution.Notify || execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded ||
		work.DueAt.IsZero() || (work.Owner == "") != work.ExpiresAt.IsZero() ||
		(work.Owner != "" && (work.Generation == 0 || work.Completed)) || work.Generation != work.Attempts {
		return fmt.Errorf("%w: invalid notification work", ErrUnsupportedSchema)
	}
	if work.Compatibility != nil &&
		(!validDiagnostic(execution, *work.Compatibility) ||
			work.Compatibility.Component != transition.CompatibilityEntity && work.Compatibility.Component != transition.CompatibilityState) {
		return fmt.Errorf("%w: invalid notification compatibility", ErrUnsupportedSchema)
	}
	return nil
}

func validateNotificationWork(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT w.execution_id, w.work, e.record, w.completed, w.due_at_seconds,
		w.due_at_nanoseconds, w.lease_owner_present, w.lease_expires_at_seconds, w.lease_expires_at_nanoseconds
		FROM rff_notification_work AS w
		LEFT JOIN rff_executions AS e ON e.id = w.execution_id`)
	if err != nil {
		return fmt.Errorf("%w: read notification work: %w", ErrUnsupportedSchema, err)
	}
	for rows.Next() {
		var id string
		var workData, executionData []byte
		var projection notificationProjection
		if err := rows.Scan(&id, &workData, &executionData, &projection.completed, &projection.dueSeconds,
			&projection.dueNanoseconds, &projection.leaseOwnerPresent, &projection.leaseExpiresAtSeconds,
			&projection.leaseExpiresAtNanosecs); err != nil {
			return errors.Join(fmt.Errorf("%w: read notification work: %w", ErrUnsupportedSchema, err), rows.Close())
		}
		var work notificationWork
		var execution diskRecord
		if err := json.Unmarshal(workData, &work); err != nil {
			return errors.Join(fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err), rows.Close())
		}
		if err := json.Unmarshal(executionData, &execution); err != nil || execution.ID != id {
			return errors.Join(fmt.Errorf("%w: invalid notification execution", ErrUnsupportedSchema), err, rows.Close())
		}
		if err := validateNotification(execution, work); err != nil {
			return errors.Join(err, rows.Close())
		}
		if !notificationProjectionMatches(work, projection) {
			return errors.Join(fmt.Errorf("%w: notification selection projection mismatch", ErrUnsupportedSchema), rows.Close())
		}
	}
	return errors.Join(rows.Err(), rows.Close())
}

func loadNotificationExecution(ctx context.Context, tx *sql.Tx, id string, notificationLocator int64) ([]byte, diskRecord, error) {
	var data, entity, reservationEntity []byte
	var record diskRecord
	var executionLocator, revision int64
	var status string
	var compatibility int64
	var reservationLocator sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT e.row_id, e.revision, e.record, e.selection_status, e.selection_compatibility,
		e.entity, a.row_id, a.entity FROM rff_executions e LEFT JOIN rff_active a ON a.execution_id = e.id
		WHERE e.id = ? FOR UPDATE`, id).Scan(&executionLocator, &revision, &data, &status, &compatibility,
		&entity, &reservationLocator, &reservationEntity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, record, recordNotificationFinding(ctx, tx, notificationLocator, "invalid_notification", "record")
	}
	if err != nil {
		return nil, record, fmt.Errorf("read notification execution: %w", err)
	}
	if executionLocator <= 0 || revision <= 0 {
		return nil, record, ErrUnsupportedSchema
	}
	quarantined, err := executionQuarantined(ctx, tx, executionLocator)
	if err != nil {
		return nil, record, fmt.Errorf("check notification execution quarantine: %w", err)
	}
	if quarantined {
		return nil, record, transition.ErrIntegrity
	}
	if err := json.Unmarshal(data, &record); err != nil ||
		!transition.SupportedExecutionSchema(record.SchemaVersion) || record.ID != id ||
		transition.ValidateDurableID(record.ID) != nil || !bytes.Equal(entity, record.Entity) {
		return nil, record, errors.Join(err, recordExecutionFinding(ctx, tx, id, "invalid_execution", "record"))
	}
	if status != string(record.Status) || (compatibility == 1) != (record.Compatibility != nil) || compatibility < 0 || compatibility > 1 {
		return nil, record, recordExecutionFinding(ctx, tx, id, "projection_mismatch", "selection")
	}
	if record.Status != transition.Notifying && record.Status != transition.Complete {
		return nil, record, recordNotificationFinding(ctx, tx, notificationLocator, "invalid_notification", "record")
	}
	if reservationLocator.Valid {
		quarantined, err := reservationQuarantined(ctx, tx, reservationLocator.Int64)
		if err != nil {
			return nil, record, fmt.Errorf("check notification reservation quarantine: %w", err)
		}
		if quarantined {
			return nil, record, transition.ErrIntegrity
		}
		if record.Status != transition.Notifying || !bytes.Equal(entity, reservationEntity) {
			return nil, record, recordReservationFinding(ctx, tx, reservationLocator.Int64)
		}
	} else if record.Status == transition.Notifying {
		return nil, record, recordExecutionFinding(ctx, tx, id, "invalid_execution", "record")
	}
	return data, record, nil
}

// NotificationOutbox persists local handoff and independent delivery claims.
type NotificationOutbox[EntityID comparable, StateID comparable] struct {
	store *Store[EntityID, StateID]
	clock func() time.Time
}

// CheckNotificationActivities validates the durable inventory without a claim.
func (o *NotificationOutbox[EntityID, StateID]) CheckNotificationActivities(ctx context.Context) error {
	return o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT execution_id FROM rff_notification_work ORDER BY execution_id`)
		if err != nil {
			return fmt.Errorf("list notification work for integrity check: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return errors.Join(err, rows.Close())
			}
			ids = append(ids, id)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, id := range ids {
			_, err := notificationClaim(ctx, tx, transition.NotificationLease{ExecutionID: id}, now)
			if err != nil && !errors.Is(err, transition.ErrStaleNotificationLease) {
				return err
			}
		}
		return nil
	})
}

// NewNotificationOutbox binds the outbox to a store and fence clock.
func NewNotificationOutbox[EntityID comparable, StateID comparable](
	store *Store[EntityID, StateID], clock func() time.Time,
) (*NotificationOutbox[EntityID, StateID], error) {
	if store == nil || clock == nil {
		return nil, errors.New("notification store and clock are required")
	}
	return &NotificationOutbox[EntityID, StateID]{store: store, clock: clock}, nil
}

func intentFromExecution[EntityID comparable, StateID comparable](execution transition.Execution[EntityID, StateID]) transition.NotificationIntent[EntityID, StateID] {
	return transition.NotificationIntent[EntityID, StateID]{
		ID: execution.NotificationIntentID, ExecutionID: execution.ID, CommitOperationID: execution.CommitOperationID,
		Entity: execution.Entity, Operation: execution.Operation, Previous: execution.Previous,
		HasPrevious: execution.HasPrevious, Target: execution.Target,
	}
}

// Handoff idempotently creates a local notification intent under the transition fence.
func (o *NotificationOutbox[EntityID, StateID]) Handoff(
	ctx context.Context, lease transition.TransitionLease[EntityID], intent transition.NotificationIntent[EntityID, StateID],
) error {
	if err := transition.ValidateDurableID(lease.ExecutionID); err != nil {
		return errors.Join(err, transition.ErrInvalidHook)
	}
	if err := transition.ValidateDurableID(intent.ExecutionID); err != nil {
		return errors.Join(err, transition.ErrInvalidHook)
	}
	if intent.ID == "" || intent.ExecutionID != lease.ExecutionID || intent.Entity != lease.Entity {
		return transition.ErrInvalidHook
	}
	return o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		var executionData []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, intent.ExecutionID).Scan(&executionData); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return transition.ErrStaleLease
			}
			return fmt.Errorf("read notification execution: %w", err)
		}
		execution, err := o.store.decodeRecord(executionData)
		if err != nil {
			return err
		}
		if intentFromExecution(execution) != intent {
			return transition.ErrConflict
		}
		var data []byte
		var locator, revision int64
		var projection notificationProjection
		err = tx.QueryRowContext(ctx, `SELECT row_id, work, revision, completed, due_at_seconds, due_at_nanoseconds,
			lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds
			FROM rff_notification_work WHERE execution_id = ? FOR UPDATE`, intent.ExecutionID).Scan(
			&locator, &data, &revision, &projection.completed, &projection.dueSeconds, &projection.dueNanoseconds,
			&projection.leaseOwnerPresent, &projection.leaseExpiresAtSeconds, &projection.leaseExpiresAtNanosecs,
		)
		if err == nil {
			if locator <= 0 || revision <= 0 {
				return ErrUnsupportedSchema
			}
			quarantined, err := notificationQuarantined(ctx, tx, locator)
			if err != nil {
				return err
			}
			if quarantined {
				return transition.ErrIntegrity
			}
			var work notificationWork
			if err := json.Unmarshal(data, &work); err != nil {
				return errors.Join(recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"), err)
			}
			if !notificationProjectionMatches(work, projection) {
				return recordNotificationFinding(ctx, tx, locator, "projection_mismatch", "selection")
			}
			_, record, err := loadNotificationExecution(ctx, tx, intent.ExecutionID, locator)
			if err != nil {
				return err
			}
			return validateNotification(record, work)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read notification work: %w", err)
		}
		now, record, err := o.store.fencedExecutionForProgress(ctx, tx, lease, o.clock)
		if err != nil {
			return err
		}
		if record.Status != transition.Notifying || !record.Notify || record.NotificationIntentID != intent.ID ||
			record.CommitReceipt == nil || record.CommitReceipt.Outcome != transition.CommitSucceeded {
			return transition.ErrInvalidHook
		}
		work := notificationWork{Version: 1, IntentID: intent.ID, DueAt: now.UTC()}
		projection, err = notificationProjectionFor(work)
		if err != nil {
			return err
		}
		data, err = json.Marshal(work)
		if err != nil {
			return fmt.Errorf("encode notification work: %w", err)
		}
		args := append([]any{intent.ExecutionID, data}, notificationProjectionArgs(projection)...)
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_notification_work
			(execution_id, work, completed, due_at_seconds, due_at_nanoseconds, lease_owner_present,
			 lease_expires_at_seconds, lease_expires_at_nanoseconds)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, args...); err != nil {
			return fmt.Errorf("create notification work: %w", err)
		}
		return nil
	})
}

func saveNotification(ctx context.Context, tx *sql.Tx, id string, work notificationWork) error {
	projection, err := notificationProjectionFor(work)
	if err != nil {
		return err
	}
	data, err := json.Marshal(work)
	if err != nil {
		return fmt.Errorf("encode notification work: %w", err)
	}
	args := notificationProjectionArgs(projection)
	args = append(args, data, id)
	if _, err := tx.ExecContext(ctx, `UPDATE rff_notification_work SET completed = ?, due_at_seconds = ?, due_at_nanoseconds = ?,
		lease_owner_present = ?, lease_expires_at_seconds = ?, lease_expires_at_nanoseconds = ?, work = ? WHERE execution_id = ?`, args...); err != nil {
		return fmt.Errorf("save notification work: %w", err)
	}
	return nil
}

// ReadNotificationCompatibility returns a delivery diagnostic without decoding the intent.
func (o *NotificationOutbox[EntityID, StateID]) ReadNotificationCompatibility(ctx context.Context, id string) (*transition.CompatibilityDiagnostic, error) {
	var diagnostic *transition.CompatibilityDiagnostic
	err := o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		var locator, revision int64
		var projection notificationProjection
		err := tx.QueryRowContext(ctx, `SELECT row_id, work, revision, completed, due_at_seconds, due_at_nanoseconds,
			lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds
			FROM rff_notification_work WHERE execution_id = ? FOR UPDATE`, id).Scan(
			&locator, &data, &revision, &projection.completed, &projection.dueSeconds, &projection.dueNanoseconds,
			&projection.leaseOwnerPresent, &projection.leaseExpiresAtSeconds, &projection.leaseExpiresAtNanosecs,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrNoNotificationActivity
		}
		if err != nil {
			return fmt.Errorf("read notification compatibility: %w", err)
		}
		if locator <= 0 || revision <= 0 {
			return ErrUnsupportedSchema
		}
		quarantined, err := notificationQuarantined(ctx, tx, locator)
		if err != nil {
			return err
		}
		if quarantined {
			return transition.ErrIntegrity
		}
		var work notificationWork
		if err := json.Unmarshal(data, &work); err != nil {
			return errors.Join(recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"), err)
		}
		if !notificationProjectionMatches(work, projection) {
			return recordNotificationFinding(ctx, tx, locator, "projection_mismatch", "selection")
		}
		_, record, err := loadNotificationExecution(ctx, tx, id, locator)
		if err != nil {
			return err
		}
		if err := validateNotification(record, work); err != nil {
			return errors.Join(recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"), err)
		}
		diagnostic = work.Compatibility
		return nil
	})
	return diagnostic, err
}

// ClaimNotification claims a due notification, counting this delivery attempt.
func (o *NotificationOutbox[EntityID, StateID]) ClaimNotification(
	ctx context.Context, owner string, clock func() time.Time, ttl time.Duration,
) (transition.NotificationDelivery[EntityID, StateID], error) {
	var delivery transition.NotificationDelivery[EntityID, StateID]
	var unavailable error
	if owner == "" || clock == nil || ttl <= 0 {
		return delivery, transition.ErrInvalidLease
	}
	err := o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		expiry, err := leaseExpiry(now, ttl)
		if err != nil {
			return err
		}
		const candidates = `SELECT w.row_id, w.execution_id, w.work, w.revision,
			w.completed, w.due_at_seconds, w.due_at_nanoseconds, w.lease_owner_present,
			w.lease_expires_at_seconds, w.lease_expires_at_nanoseconds
			FROM rff_notification_work AS w
			WHERE w.completed = 0 AND w.execution_id > ?
				AND (w.due_at_seconds < ? OR (w.due_at_seconds = ? AND w.due_at_nanoseconds <= ?))
				AND (w.lease_owner_present = 0 OR w.lease_expires_at_seconds < ? OR
					(w.lease_expires_at_seconds = ? AND w.lease_expires_at_nanoseconds <= ?))
			ORDER BY w.execution_id LIMIT 1 FOR UPDATE`
		var id string
		var work notificationWork
		var execution transition.Execution[EntityID, StateID]
		type blockedWork struct {
			id   string
			work notificationWork
		}
		var blocked []blockedWork
		lastID := ""
		for {
			var candidateID string
			var data []byte
			var locator, revision int64
			var projection notificationProjection
			err := tx.QueryRowContext(ctx, candidates, lastID,
				now.Unix(), now.Unix(), now.Nanosecond(), now.Unix(), now.Unix(), now.Nanosecond(),
			).Scan(&locator, &candidateID, &data, &revision, &projection.completed,
				&projection.dueSeconds, &projection.dueNanoseconds, &projection.leaseOwnerPresent,
				&projection.leaseExpiresAtSeconds, &projection.leaseExpiresAtNanosecs)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return fmt.Errorf("select eligible notification work: %w", err)
			}
			if locator <= 0 || revision <= 0 {
				return ErrUnsupportedSchema
			}
			quarantined, err := notificationQuarantined(ctx, tx, locator)
			if err != nil {
				return fmt.Errorf("check notification quarantine: %w", err)
			}
			if quarantined {
				unavailable = errors.Join(unavailable, transition.ErrIntegrity)
				lastID = candidateID
				continue
			}
			var candidate notificationWork
			if err := json.Unmarshal(data, &candidate); err != nil {
				return errors.Join(recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"),
					fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err))
			}
			if !notificationProjectionMatches(candidate, projection) {
				return recordNotificationFinding(ctx, tx, locator, "projection_mismatch", "selection")
			}
			candidateExecution, record, err := loadNotificationExecution(ctx, tx, candidateID, locator)
			if err != nil {
				if errors.Is(err, transition.ErrIntegrity) {
					unavailable = errors.Join(unavailable, err)
					lastID = candidateID
					continue
				}
				return err
			}
			if err := validateNotification(record, candidate); err != nil {
				return errors.Join(recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"), err)
			}
			decoded, decodeErr := o.store.decodeRecord(candidateExecution)
			if decodeErr != nil {
				var compatibility *transition.CompatibilityError
				if !errors.As(decodeErr, &compatibility) {
					return errors.Join(recordExecutionFinding(ctx, tx, candidateID, "invalid_execution", "record"), decodeErr)
				}
				if !validDiagnostic(record, compatibility.Diagnostic) {
					return errors.Join(recordExecutionFinding(ctx, tx, candidateID, "invalid_execution", "record"), decodeErr)
				}
				candidate.Compatibility = &compatibility.Diagnostic
				blocked = append(blocked, blockedWork{id: candidateID, work: candidate})
				unavailable = errors.Join(unavailable, decodeErr)
				lastID = candidateID
				continue
			}
			id, work, execution = candidateID, candidate, decoded
			break
		}
		for _, candidate := range blocked {
			if err := saveNotification(ctx, tx, candidate.id, candidate.work); err != nil {
				return err
			}
		}
		if id == "" {
			if unavailable == nil {
				unavailable = transition.ErrNoNotificationActivity
			}
			return nil
		}
		if work.Generation == math.MaxUint64 {
			return errors.New("notification lease generation exhausted")
		}
		work.Generation++
		work.Attempts++
		work.Owner, work.ExpiresAt = owner, expiry
		work.Compatibility = nil
		if err := saveNotification(ctx, tx, id, work); err != nil {
			return err
		}
		delivery = transition.NotificationDelivery[EntityID, StateID]{
			Intent: intentFromExecution(execution), Attempt: work.Attempts,
			Lease: transition.NotificationLease{ExecutionID: id, IntentID: work.IntentID, Owner: owner, Generation: work.Generation, ExpiresAt: expiry},
		}
		return nil
	})
	if err != nil {
		return transition.NotificationDelivery[EntityID, StateID]{}, err
	}
	if delivery.Intent.ID == "" {
		return transition.NotificationDelivery[EntityID, StateID]{}, unavailable
	}
	return delivery, nil
}

func notificationClaim(ctx context.Context, tx *sql.Tx, lease transition.NotificationLease, now time.Time) (notificationWork, error) {
	if err := transition.ValidateDurableID(lease.ExecutionID); err != nil {
		return notificationWork{}, errors.Join(err, transition.ErrStaleNotificationLease)
	}
	var work notificationWork
	var data []byte
	var locator, revision int64
	var projection notificationProjection
	err := tx.QueryRowContext(ctx, `SELECT row_id, work, revision, completed, due_at_seconds, due_at_nanoseconds,
		lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds
		FROM rff_notification_work WHERE execution_id = ? FOR UPDATE`, lease.ExecutionID).Scan(
		&locator, &data, &revision, &projection.completed, &projection.dueSeconds, &projection.dueNanoseconds,
		&projection.leaseOwnerPresent, &projection.leaseExpiresAtSeconds, &projection.leaseExpiresAtNanosecs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return work, transition.ErrStaleNotificationLease
	}
	if err != nil {
		return work, fmt.Errorf("read notification claim: %w", err)
	}
	if locator <= 0 || revision <= 0 {
		return work, ErrUnsupportedSchema
	}
	quarantined, err := notificationQuarantined(ctx, tx, locator)
	if err != nil {
		return work, fmt.Errorf("check notification quarantine: %w", err)
	}
	if quarantined {
		return work, transition.ErrIntegrity
	}
	if err := json.Unmarshal(data, &work); err != nil {
		return work, fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err)
	}
	if !notificationProjectionMatches(work, projection) {
		return work, recordNotificationFinding(ctx, tx, locator, "projection_mismatch", "selection")
	}
	_, execution, err := loadNotificationExecution(ctx, tx, lease.ExecutionID, locator)
	if err != nil {
		return work, err
	}
	if err := validateNotification(execution, work); err != nil {
		return work, errors.Join(recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"), err)
	}
	if now.IsZero() || lease.Owner == "" || lease.Generation == 0 || work.Completed || work.Owner != lease.Owner ||
		work.IntentID != lease.IntentID || work.Generation != lease.Generation || !now.Before(work.ExpiresAt) {
		return work, transition.ErrStaleNotificationLease
	}
	return work, nil
}

// RenewNotification extends an unexpired notification claim.
func (o *NotificationOutbox[EntityID, StateID]) RenewNotification(ctx context.Context, lease transition.NotificationLease, clock func() time.Time, ttl time.Duration) (transition.NotificationLease, error) {
	if clock == nil || ttl <= 0 {
		return transition.NotificationLease{}, transition.ErrInvalidLease
	}
	err := o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		expiry, err := leaseExpiry(now, ttl)
		if err != nil {
			return err
		}
		work, err := notificationClaim(ctx, tx, lease, now)
		if err != nil {
			return err
		}
		work.ExpiresAt = expiry
		if err := saveNotification(ctx, tx, lease.ExecutionID, work); err != nil {
			return err
		}
		lease.ExpiresAt = expiry
		return nil
	})
	if err != nil {
		return transition.NotificationLease{}, err
	}
	return lease, nil
}

// CompleteNotification marks a successfully delivered intent under its current claim.
func (o *NotificationOutbox[EntityID, StateID]) CompleteNotification(ctx context.Context, lease transition.NotificationLease, clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		work, err := notificationClaim(ctx, tx, lease, clock())
		if err != nil {
			return err
		}
		work.Completed, work.Owner, work.ExpiresAt = true, "", time.Time{}
		return saveNotification(ctx, tx, lease.ExecutionID, work)
	})
}

// RetryNotificationDelivery schedules another independent delivery attempt.
func (o *NotificationOutbox[EntityID, StateID]) RetryNotificationDelivery(ctx context.Context, lease transition.NotificationLease, clock func() time.Time, due time.Time) error {
	if clock == nil || due.IsZero() {
		return transition.ErrInvalidLease
	}
	return o.store.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		work, err := notificationClaim(ctx, tx, lease, now)
		if err != nil {
			return err
		}
		if due.Before(now) {
			return transition.ErrInvalidLease
		}
		work.DueAt, work.Owner, work.ExpiresAt = due.UTC(), "", time.Time{}
		return saveNotification(ctx, tx, lease.ExecutionID, work)
	})
}
