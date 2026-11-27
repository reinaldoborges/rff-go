package sqlite

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

type timerProjection struct {
	deadlineSeconds         int64
	deadlineNanoseconds     int64
	completed               int64
	compatibilityBlocked    int64
	leaseExpiresSeconds     sql.NullInt64
	leaseExpiresNanoseconds sql.NullInt64
}

func timerProjectionFor(record transition.TimerRecord) (timerProjection, error) {
	if record.Deadline.IsZero() {
		return timerProjection{}, ErrUnsupportedSchema
	}
	projection := timerProjection{
		deadlineSeconds:      record.Deadline.Unix(),
		deadlineNanoseconds:  int64(record.Deadline.Nanosecond()),
		completed:            boolInt(record.Completed),
		compatibilityBlocked: boolInt(record.Compatibility != nil),
	}
	if record.Generation > 0 {
		if record.LeaseExpiresAt.IsZero() {
			return timerProjection{}, ErrUnsupportedSchema
		}
		projection.leaseExpiresSeconds = sql.NullInt64{Int64: record.LeaseExpiresAt.Unix(), Valid: true}
		projection.leaseExpiresNanoseconds = sql.NullInt64{Int64: int64(record.LeaseExpiresAt.Nanosecond()), Valid: true}
	}
	return projection, nil
}

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func timerProjectionMatches(record transition.TimerRecord, actual timerProjection) bool {
	expected, err := timerProjectionFor(record)
	if err != nil {
		return false
	}
	return expected.deadlineSeconds == actual.deadlineSeconds &&
		expected.deadlineNanoseconds == actual.deadlineNanoseconds &&
		expected.completed == actual.completed &&
		expected.compatibilityBlocked == actual.compatibilityBlocked &&
		expected.leaseExpiresSeconds == actual.leaseExpiresSeconds &&
		expected.leaseExpiresNanoseconds == actual.leaseExpiresNanoseconds
}

func timerProjectionArgs(projection timerProjection) []any {
	return []any{
		projection.deadlineSeconds, projection.deadlineNanoseconds, projection.completed,
		projection.compatibilityBlocked, projection.leaseExpiresSeconds, projection.leaseExpiresNanoseconds,
	}
}

func updateTimerRecord(ctx context.Context, tx *sql.Tx, record transition.TimerRecord) error {
	projection, err := timerProjectionFor(record)
	if err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	args := timerProjectionArgs(projection)
	args = append(args, data, record.ID)
	_, err = tx.ExecContext(ctx, `UPDATE rff_timers SET deadline_seconds = ?, deadline_nanoseconds = ?,
		completed = ?, compatibility_blocked = ?, lease_expires_seconds = ?, lease_expires_nanoseconds = ?, record = ?
		WHERE id = ?`, args...)
	return err
}

func decodeTimer(id string, data []byte) (transition.TimerRecord, error) {
	var record transition.TimerRecord
	if err := json.Unmarshal(data, &record); err != nil ||
		(record.SchemaVersion != 1 && record.SchemaVersion != transition.CurrentTimerSchemaVersion) || record.ID != id ||
		transition.ValidateDurableID(record.ID) != nil || record.WorkflowID == "" || record.Deadline.IsZero() ||
		record.Event.Codec.Name == "" || record.Event.Codec.Version == "" ||
		(record.Generation == 0 && (record.Owner != "" || !record.LeaseExpiresAt.IsZero() || record.Completed)) ||
		(record.Generation > 0 && (record.Generation > math.MaxInt64 || record.Owner == "" || record.LeaseExpiresAt.IsZero())) {
		return transition.TimerRecord{}, fmt.Errorf("%w: invalid timer", ErrUnsupportedSchema)
	}
	if record.Compatibility != nil && (record.SchemaVersion != transition.CurrentTimerSchemaVersion ||
		record.Completed || record.Generation == 0 ||
		record.Compatibility.Component != transition.CompatibilityEventCodec ||
		record.Compatibility.Name != record.Event.Codec.Name ||
		record.Compatibility.Version != record.Event.Codec.Version || record.Compatibility.Check != "") {
		return transition.TimerRecord{}, fmt.Errorf("%w: invalid timer compatibility", ErrUnsupportedSchema)
	}
	return record, nil
}

// ScheduleTimer atomically records or reconciles a timer by its global ID.
func (s *Store[EntityID, StateID]) ScheduleTimer(ctx context.Context, id string,
	decide func(*transition.TimerRecord) (*transition.TimerRecord, error),
) (record transition.TimerRecord, created bool, err error) {
	if err := transition.ValidateDurableID(id); err != nil {
		return record, false, err
	}
	if decide == nil {
		return record, false, transition.ErrInvalidRequest
	}
	err = s.timerTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		var locator, revision int64
		var actual timerProjection
		err := tx.QueryRowContext(ctx, `SELECT row_id, record, revision, deadline_seconds, deadline_nanoseconds,
			completed, compatibility_blocked, lease_expires_seconds, lease_expires_nanoseconds
			FROM rff_timers WHERE id = ?`, id).Scan(&locator, &data, &revision, &actual.deadlineSeconds,
			&actual.deadlineNanoseconds, &actual.completed, &actual.compatibilityBlocked,
			&actual.leaseExpiresSeconds, &actual.leaseExpiresNanoseconds)
		var existing *transition.TimerRecord
		if err == nil {
			if locator <= 0 || revision <= 0 {
				return ErrUnsupportedSchema
			}
			quarantined, err := timerQuarantined(ctx, tx, locator)
			if err != nil {
				return err
			}
			if quarantined {
				return transition.ErrIntegrity
			}
			decoded, err := decodeTimer(id, data)
			if err != nil || !timerProjectionMatches(decoded, actual) {
				return recordTimerFinding(ctx, tx, locator)
			}
			existing = &decoded
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read timer: %w", err)
		}
		candidate, err := decide(existing)
		if err != nil {
			return err
		}
		if existing != nil {
			if candidate != nil {
				return transition.ErrInvalidDecision
			}
			// The callback may mutate its view; return the persisted value instead.
			record, err = decodeTimer(id, data)
			return err
		}
		if candidate == nil || candidate.Completed || candidate.Owner != "" || candidate.Generation != 0 || !candidate.LeaseExpiresAt.IsZero() {
			return transition.ErrInvalidDecision
		}
		data, err = json.Marshal(candidate)
		if err != nil {
			return fmt.Errorf("encode timer: %w", err)
		}
		record, err = decodeTimer(id, data)
		if err != nil {
			return fmt.Errorf("%w: invalid timer decision", transition.ErrInvalidDecision)
		}
		projection, err := timerProjectionFor(record)
		if err != nil {
			return fmt.Errorf("%w: invalid timer projection", transition.ErrInvalidDecision)
		}
		args := append([]any{id}, timerProjectionArgs(projection)...)
		args = append(args, data)
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_timers
			(id, deadline_seconds, deadline_nanoseconds, completed, compatibility_blocked, lease_expires_seconds, lease_expires_nanoseconds, record)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, args...); err != nil {
			return fmt.Errorf("persist timer: %w", err)
		}
		var collision bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_history_lookup WHERE kind = 'timer' AND locator = last_insert_rowid())`).Scan(&collision); err != nil {
			return fmt.Errorf("check timer locator: %w", err)
		}
		if collision {
			return fmt.Errorf("%w: reused timer locator", ErrUnsupportedSchema)
		}
		created = true
		return nil
	})
	if err != nil {
		return transition.TimerRecord{}, false, err
	}
	return record, created, nil
}

// ClaimDueTimer claims the earliest uncompleted timer whose deadline and any
// previous claim have elapsed.
func (s *Store[EntityID, StateID]) ClaimDueTimer(ctx context.Context, owner string, clock func() time.Time, ttl time.Duration) (
	record transition.TimerRecord, lease transition.TimerLease, err error,
) {
	if owner == "" || clock == nil || ttl <= 0 {
		return record, lease, transition.ErrInvalidLease
	}
	err = s.timerTransaction(ctx, func(tx *sql.Tx) error {
		now := clock().UTC()
		if now.IsZero() || !now.Add(ttl).After(now) {
			return transition.ErrInvalidLease
		}
		var id string
		var locator, revision int64
		var data []byte
		var actual timerProjection
		err := tx.QueryRowContext(ctx, `SELECT t.row_id, t.id, t.record, t.revision, t.deadline_seconds,
			t.deadline_nanoseconds, t.completed, t.compatibility_blocked, t.lease_expires_seconds, t.lease_expires_nanoseconds
			FROM rff_timers t
			WHERE t.completed = 0 AND t.compatibility_blocked = 0
				AND (t.deadline_seconds < ? OR (t.deadline_seconds = ? AND t.deadline_nanoseconds <= ?))
				AND (t.lease_expires_seconds IS NULL OR t.lease_expires_seconds < ?
					OR (t.lease_expires_seconds = ? AND t.lease_expires_nanoseconds <= ?))
				AND NOT EXISTS (SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
					WHERE kind = 'timer' AND locator = t.row_id AND resolved_at IS NULL)
			ORDER BY t.deadline_seconds, t.deadline_nanoseconds, t.id LIMIT 1`,
			now.Unix(), now.Unix(), now.Nanosecond(), now.Unix(), now.Unix(), now.Nanosecond(),
		).Scan(&locator, &id, &data, &revision, &actual.deadlineSeconds, &actual.deadlineNanoseconds,
			&actual.completed, &actual.compatibilityBlocked, &actual.leaseExpiresSeconds, &actual.leaseExpiresNanoseconds)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrNoDueTimer
		}
		if err != nil {
			return fmt.Errorf("read due timer candidate: %w", err)
		}
		if locator <= 0 || revision <= 0 {
			return ErrUnsupportedSchema
		}
		record, err = decodeTimer(id, data)
		if err != nil || !timerProjectionMatches(record, actual) {
			return recordTimerFinding(ctx, tx, locator)
		}
		if record.Completed || record.Compatibility != nil || record.Deadline.After(now) ||
			record.Generation > 0 && record.LeaseExpiresAt.After(now) {
			return recordTimerFinding(ctx, tx, locator)
		}
		if record.Generation == math.MaxInt64 {
			return fmt.Errorf("%w: timer generation exhausted", ErrUnsupportedSchema)
		}
		record.Generation++
		record.Owner = owner
		record.LeaseExpiresAt = now.Add(ttl)
		if err := updateTimerRecord(ctx, tx, record); err != nil {
			return fmt.Errorf("claim timer: %w", err)
		}
		lease = transition.TimerLease{ID: record.ID, Owner: owner, Generation: record.Generation, ExpiresAt: record.LeaseExpiresAt}
		return nil
	})
	if err != nil {
		return transition.TimerRecord{}, transition.TimerLease{}, err
	}
	return record, lease, nil
}

// BlockTimerCompatibility fences an incompatible claim and persists its diagnostic.
func (s *Store[EntityID, StateID]) BlockTimerCompatibility(
	ctx context.Context, lease transition.TimerLease, clock func() time.Time, diagnostic transition.CompatibilityDiagnostic,
) error {
	if err := transition.ValidateDurableID(lease.ID); err != nil {
		return errors.Join(err, transition.ErrInvalidLease)
	}
	if lease.ID == "" || lease.Owner == "" || lease.Generation == 0 || clock == nil {
		return transition.ErrInvalidLease
	}
	return s.timerTransaction(ctx, func(tx *sql.Tx) error {
		now := clock().UTC()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		record, err := loadIntegrityTimer(ctx, tx, lease.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return err
		}
		if record.Completed || record.Compatibility != nil || record.Owner != lease.Owner ||
			record.Generation != lease.Generation || !now.Before(record.LeaseExpiresAt) {
			return transition.ErrStaleLease
		}
		if diagnostic.Component != transition.CompatibilityEventCodec || diagnostic.Name != record.Event.Codec.Name ||
			diagnostic.Version != record.Event.Codec.Version || diagnostic.Check != "" {
			return transition.ErrInvalidDecision
		}
		if record.Generation == math.MaxInt64 {
			return fmt.Errorf("%w: timer generation exhausted", ErrUnsupportedSchema)
		}
		record.Generation++
		record.LeaseExpiresAt = now
		record.SchemaVersion = transition.CurrentTimerSchemaVersion
		record.Compatibility = &diagnostic
		if err := updateTimerRecord(ctx, tx, record); err != nil {
			return fmt.Errorf("block incompatible timer: %w", err)
		}
		return nil
	})
}

// BlockedTimers lists retained timer diagnostics for revalidation.
func (s *Store[EntityID, StateID]) BlockedTimers(ctx context.Context) (records []transition.TimerRecord, err error) {
	err = s.timerTransaction(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT t.row_id, t.id, t.record, t.revision FROM rff_timers t
			WHERE NOT EXISTS (SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
			WHERE kind = 'timer' AND locator = t.row_id AND resolved_at IS NULL) ORDER BY t.id`)
		if err != nil {
			return fmt.Errorf("list blocked timers: %w", err)
		}
		for rows.Next() {
			var locator, revision int64
			var id string
			var data []byte
			if err := rows.Scan(&locator, &id, &data, &revision); err != nil {
				return errors.Join(err, rows.Close())
			}
			if locator <= 0 || revision <= 0 {
				return errors.Join(ErrUnsupportedSchema, rows.Close())
			}
			record, err := decodeTimer(id, data)
			if err != nil {
				if err := rows.Close(); err != nil {
					return err
				}
				return recordTimerFinding(ctx, tx, locator)
			}
			if record.Compatibility != nil {
				records = append(records, record)
			}
		}
		return errors.Join(rows.Err(), rows.Close())
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// ClearTimerCompatibility clears only the diagnostic and generation that were revalidated.
func (s *Store[EntityID, StateID]) ClearTimerCompatibility(
	ctx context.Context, id string, generation uint64, expected transition.CompatibilityDiagnostic,
) error {
	if err := transition.ValidateDurableID(id); err != nil {
		return err
	}
	if id == "" || generation == 0 {
		return transition.ErrInvalidRequest
	}
	return s.timerTransaction(ctx, func(tx *sql.Tx) error {
		record, err := loadIntegrityTimer(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrInvalidRequest
		}
		if err != nil {
			return err
		}
		if record.Compatibility == nil {
			return nil
		}
		if record.Generation != generation || *record.Compatibility != expected {
			return transition.ErrConflict
		}
		record.Compatibility = nil
		if err := updateTimerRecord(ctx, tx, record); err != nil {
			return fmt.Errorf("clear timer compatibility: %w", err)
		}
		return nil
	})
}

// CompleteTimer fences late reports and retains the original scheduling record.
func (s *Store[EntityID, StateID]) CompleteTimer(ctx context.Context, lease transition.TimerLease, clock func() time.Time) error {
	if err := transition.ValidateDurableID(lease.ID); err != nil {
		return errors.Join(err, transition.ErrInvalidLease)
	}
	if lease.ID == "" || lease.Owner == "" || lease.Generation == 0 || lease.ExpiresAt.IsZero() || clock == nil {
		return transition.ErrInvalidLease
	}
	return s.timerTransaction(ctx, func(tx *sql.Tx) error {
		record, err := loadIntegrityTimer(ctx, tx, lease.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return err
		}
		if record.Compatibility != nil || record.Owner != lease.Owner || record.Generation != lease.Generation {
			return transition.ErrStaleLease
		}
		if record.Completed {
			return nil
		}
		now := clock()
		if now.IsZero() || !now.Before(record.LeaseExpiresAt) {
			return transition.ErrStaleLease
		}
		record.Completed = true
		if err := updateTimerRecord(ctx, tx, record); err != nil {
			return fmt.Errorf("complete timer: %w", err)
		}
		return nil
	})
}

// RenewTimer extends a current, unexpired delivery claim.
func (s *Store[EntityID, StateID]) RenewTimer(ctx context.Context, lease transition.TimerLease, clock func() time.Time, ttl time.Duration) (transition.TimerLease, error) {
	if err := transition.ValidateDurableID(lease.ID); err != nil {
		return transition.TimerLease{}, errors.Join(err, transition.ErrInvalidLease)
	}
	if lease.ID == "" || lease.Owner == "" || lease.Generation == 0 || clock == nil || ttl <= 0 {
		return transition.TimerLease{}, transition.ErrInvalidLease
	}
	err := s.timerTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		expiry, err := leaseExpiry(now, ttl)
		if err != nil {
			return err
		}
		record, err := loadIntegrityTimer(ctx, tx, lease.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return err
		}
		if record.Completed || record.Compatibility != nil || record.Owner != lease.Owner ||
			record.Generation != lease.Generation || !now.Before(record.LeaseExpiresAt) {
			return transition.ErrStaleLease
		}
		record.LeaseExpiresAt = expiry
		if err := updateTimerRecord(ctx, tx, record); err != nil {
			return fmt.Errorf("renew timer: %w", err)
		}
		lease.ExpiresAt = expiry
		return nil
	})
	if err != nil {
		return transition.TimerLease{}, err
	}
	return lease, nil
}

func validateTimers(ctx context.Context, tx *sql.Tx, adapterVersion int) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT row_id, id, record, revision FROM rff_timers`)
	if err != nil {
		return fmt.Errorf("%w: read timers: %w", ErrUnsupportedSchema, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close timer query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		var locator, revision int64
		var data []byte
		if err := rows.Scan(&locator, &id, &data, &revision); err != nil {
			return fmt.Errorf("%w: read timer: %w", ErrUnsupportedSchema, err)
		}
		if locator <= 0 || revision <= 0 {
			return fmt.Errorf("%w: invalid timer locator or revision", ErrUnsupportedSchema)
		}
		record, err := decodeTimer(id, data)
		if err != nil {
			return err
		}
		if adapterVersion < 9 && record.SchemaVersion != 1 {
			return fmt.Errorf("%w: timer version is newer than adapter schema", ErrUnsupportedSchema)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read timers: %w", ErrUnsupportedSchema, err)
	}
	return nil
}
