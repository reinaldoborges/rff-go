package mysql

import (
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
	rows, err := tx.QueryContext(ctx, `SELECT w.execution_id, w.work, e.record FROM rff_notification_work AS w
		LEFT JOIN rff_executions AS e ON e.id = w.execution_id`)
	if err != nil {
		return fmt.Errorf("%w: read notification work: %w", ErrUnsupportedSchema, err)
	}
	for rows.Next() {
		var id string
		var workData, executionData []byte
		if err := rows.Scan(&id, &workData, &executionData); err != nil {
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
	}
	return errors.Join(rows.Err(), rows.Close())
}

// NotificationOutbox persists local handoff and independent delivery claims.
type NotificationOutbox[EntityID comparable, StateID comparable] struct {
	store *Store[EntityID, StateID]
	clock func() time.Time
}

// CheckNotificationActivities validates the durable inventory without a claim.
func (o *NotificationOutbox[EntityID, StateID]) CheckNotificationActivities(ctx context.Context) error {
	return o.store.hookTransaction(ctx, func(tx *sql.Tx) error { return validateNotificationWork(ctx, tx) })
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
		err = tx.QueryRowContext(ctx, `SELECT work FROM rff_notification_work WHERE execution_id = ?`, intent.ExecutionID).Scan(&data)
		if err == nil {
			var work notificationWork
			var record diskRecord
			if err := json.Unmarshal(data, &work); err != nil {
				return fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err)
			}
			if err := json.Unmarshal(executionData, &record); err != nil {
				return fmt.Errorf("%w: decode notification execution: %w", ErrUnsupportedSchema, err)
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
		data, err = json.Marshal(work)
		if err != nil {
			return fmt.Errorf("encode notification work: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_notification_work (execution_id, work) VALUES (?, ?)`, intent.ExecutionID, data); err != nil {
			return fmt.Errorf("create notification work: %w", err)
		}
		return nil
	})
}

func saveNotification(ctx context.Context, tx *sql.Tx, id string, work notificationWork) error {
	data, err := json.Marshal(work)
	if err != nil {
		return fmt.Errorf("encode notification work: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rff_notification_work SET work = ? WHERE execution_id = ?`, data, id); err != nil {
		return fmt.Errorf("save notification work: %w", err)
	}
	return nil
}

// ReadNotificationCompatibility returns a delivery diagnostic without decoding the intent.
func (o *NotificationOutbox[EntityID, StateID]) ReadNotificationCompatibility(ctx context.Context, id string) (*transition.CompatibilityDiagnostic, error) {
	var data, executionData []byte
	err := o.store.db.QueryRowContext(ctx, `SELECT w.work, e.record FROM rff_notification_work AS w
		JOIN rff_executions AS e ON e.id = w.execution_id WHERE w.execution_id = ?`, id).Scan(&data, &executionData)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, transition.ErrNoNotificationActivity
	}
	if err != nil {
		return nil, fmt.Errorf("read notification compatibility: %w", err)
	}
	var work notificationWork
	var record diskRecord
	if err := json.Unmarshal(data, &work); err != nil {
		return nil, fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err)
	}
	if err := json.Unmarshal(executionData, &record); err != nil || record.ID != id {
		return nil, fmt.Errorf("%w: invalid notification execution", ErrUnsupportedSchema)
	}
	if err := validateNotification(record, work); err != nil {
		return nil, err
	}
	return work.Compatibility, nil
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
		rows, err := tx.QueryContext(ctx, `SELECT w.execution_id, w.work, e.record FROM rff_notification_work AS w
			JOIN rff_executions AS e ON e.id = w.execution_id ORDER BY w.execution_id`)
		if err != nil {
			return fmt.Errorf("list notification work: %w", err)
		}
		var id string
		var work notificationWork
		var execution transition.Execution[EntityID, StateID]
		blocked := make(map[string]notificationWork)
		for rows.Next() {
			var candidateID string
			var data, candidateExecution []byte
			if err := rows.Scan(&candidateID, &data, &candidateExecution); err != nil {
				return errors.Join(err, rows.Close())
			}
			var candidate notificationWork
			var record diskRecord
			if err := json.Unmarshal(data, &candidate); err != nil {
				return errors.Join(fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err), rows.Close())
			}
			if err := json.Unmarshal(candidateExecution, &record); err != nil || record.ID != candidateID {
				return errors.Join(fmt.Errorf("%w: invalid notification execution", ErrUnsupportedSchema), err, rows.Close())
			}
			if err := validateNotification(record, candidate); err != nil {
				return errors.Join(err, rows.Close())
			}
			if candidate.Completed || now.Before(candidate.DueAt) || candidate.Owner != "" && now.Before(candidate.ExpiresAt) {
				continue
			}
			decoded, decodeErr := o.store.decodeRecord(candidateExecution)
			if decodeErr != nil {
				var compatibility *transition.CompatibilityError
				if !errors.As(decodeErr, &compatibility) || !validDiagnostic(record, compatibility.Diagnostic) {
					return errors.Join(decodeErr, rows.Close())
				}
				candidate.Compatibility = &compatibility.Diagnostic
				blocked[candidateID] = candidate
				unavailable = errors.Join(unavailable, decodeErr)
				continue
			}
			id, work, execution = candidateID, candidate, decoded
			break
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("list notification work: %w", err)
		}
		for blockedID, blockedWork := range blocked {
			if err := saveNotification(ctx, tx, blockedID, blockedWork); err != nil {
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
	var work notificationWork
	var data, executionData []byte
	err := tx.QueryRowContext(ctx, `SELECT w.work, e.record FROM rff_notification_work AS w
		JOIN rff_executions AS e ON e.id = w.execution_id WHERE w.execution_id = ?`, lease.ExecutionID).Scan(&data, &executionData)
	if errors.Is(err, sql.ErrNoRows) {
		return work, transition.ErrStaleNotificationLease
	}
	if err != nil {
		return work, fmt.Errorf("read notification claim: %w", err)
	}
	var execution diskRecord
	if err := json.Unmarshal(data, &work); err != nil {
		return work, fmt.Errorf("%w: decode notification work: %w", ErrUnsupportedSchema, err)
	}
	if err := json.Unmarshal(executionData, &execution); err != nil || execution.ID != lease.ExecutionID {
		return work, errors.Join(fmt.Errorf("%w: invalid notification execution", ErrUnsupportedSchema), err)
	}
	if err := validateNotification(execution, work); err != nil {
		return work, err
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
