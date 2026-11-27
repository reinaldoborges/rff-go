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
		err := tx.QueryRowContext(ctx, `SELECT row_id, record, revision FROM rff_timers WHERE id = ? FOR UPDATE`, id).Scan(&locator, &data, &revision)
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
			if err != nil {
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
		result, err := tx.ExecContext(ctx, `INSERT INTO rff_timers (id, record) VALUES (?, ?)`, id, data)
		if err != nil {
			return fmt.Errorf("persist timer: %w", err)
		}
		locator, err = result.LastInsertId()
		if err != nil || locator <= 0 {
			return fmt.Errorf("%w: invalid allocated timer locator", ErrUnsupportedSchema)
		}
		var collision bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup) WHERE kind = 'timer' AND locator = ?)`, locator).Scan(&collision); err != nil {
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
		rows, err := tx.QueryContext(ctx, `SELECT t.row_id, t.id, t.record, t.revision FROM rff_timers t
			WHERE NOT EXISTS (SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
			WHERE kind = 'timer' AND locator = t.row_id AND open_marker = 0) ORDER BY t.id FOR UPDATE`)
		if err != nil {
			return fmt.Errorf("read due timers: %w", err)
		}
		var found bool
		for rows.Next() {
			var id string
			var locator, revision int64
			var data []byte
			if err := rows.Scan(&locator, &id, &data, &revision); err != nil {
				return errors.Join(fmt.Errorf("read timer: %w", err), rows.Close())
			}
			if locator <= 0 || revision <= 0 {
				return errors.Join(ErrUnsupportedSchema, rows.Close())
			}
			current, err := decodeTimer(id, data)
			if err != nil {
				if err := rows.Close(); err != nil {
					return err
				}
				return recordTimerFinding(ctx, tx, locator)
			}
			if current.Completed || current.Compatibility != nil || current.Deadline.After(now) ||
				current.Generation > 0 && current.LeaseExpiresAt.After(now) {
				continue
			}
			if !found || current.Deadline.Before(record.Deadline) {
				record, found = current, true
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("read due timers: %w", err)
		}
		if !found {
			return transition.ErrNoDueTimer
		}
		if record.Generation == math.MaxInt64 {
			return fmt.Errorf("%w: timer generation exhausted", ErrUnsupportedSchema)
		}
		record.Generation++
		record.Owner = owner
		record.LeaseExpiresAt = now.Add(ttl)
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode claimed timer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_timers SET record = ? WHERE id = ?`, data, record.ID); err != nil {
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
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode incompatible timer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_timers SET record = ? WHERE id = ?`, data, lease.ID); err != nil {
			return fmt.Errorf("block incompatible timer: %w", err)
		}
		return nil
	})
}

// BlockedTimers lists retained timer diagnostics for revalidation.
func (s *Store[EntityID, StateID]) BlockedTimers(ctx context.Context) (records []transition.TimerRecord, err error) {
	err = s.timerTransaction(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT t.row_id, t.id, t.record, t.revision FROM rff_timers t
			WHERE NOT EXISTS (SELECT 1 FROM rff_integrity_findings FORCE INDEX (rff_integrity_open_lookup)
			WHERE kind = 'timer' AND locator = t.row_id AND open_marker = 0) ORDER BY t.id FOR UPDATE`)
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
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode revalidated timer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_timers SET record = ? WHERE id = ?`, data, id); err != nil {
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
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode completed timer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_timers SET record = ? WHERE id = ?`, data, lease.ID); err != nil {
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
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode renewed timer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_timers SET record = ? WHERE id = ?`, data, lease.ID); err != nil {
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

func validateTimers(ctx context.Context, tx *sql.Tx) (err error) {
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
		if _, err := decodeTimer(id, data); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read timers: %w", ErrUnsupportedSchema, err)
	}
	return nil
}
