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

func decodeTimer(id string, data []byte) (transition.TimerRecord, error) {
	var record transition.TimerRecord
	if err := json.Unmarshal(data, &record); err != nil ||
		(record.SchemaVersion != 1 && record.SchemaVersion != transition.CurrentTimerSchemaVersion) || record.ID != id ||
		record.ID == "" || record.WorkflowID == "" || record.Deadline.IsZero() ||
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
	if id == "" || decide == nil {
		return record, false, transition.ErrInvalidRequest
	}
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_timers WHERE id = ?`, id).Scan(&data)
		var existing *transition.TimerRecord
		if err == nil {
			decoded, err := decodeTimer(id, data)
			if err != nil {
				return err
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
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_timers (id, record) VALUES (?, ?)`, id, data); err != nil {
			return fmt.Errorf("persist timer: %w", err)
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
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock().UTC()
		if now.IsZero() || !now.Add(ttl).After(now) {
			return transition.ErrInvalidLease
		}
		rows, err := tx.QueryContext(ctx, `SELECT id, record FROM rff_timers ORDER BY id`)
		if err != nil {
			return fmt.Errorf("read due timers: %w", err)
		}
		var found bool
		for rows.Next() {
			var id string
			var data []byte
			if err := rows.Scan(&id, &data); err != nil {
				return errors.Join(fmt.Errorf("read timer: %w", err), rows.Close())
			}
			current, err := decodeTimer(id, data)
			if err != nil {
				return errors.Join(err, rows.Close())
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
	if lease.ID == "" || lease.Owner == "" || lease.Generation == 0 || clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock().UTC()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_timers WHERE id = ?`, lease.ID).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return fmt.Errorf("read timer claim: %w", err)
		}
		record, err := decodeTimer(lease.ID, data)
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
		data, err = json.Marshal(record)
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
	rows, err := s.db.QueryContext(ctx, `SELECT id, record FROM rff_timers ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list blocked timers: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close blocked timers: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, fmt.Errorf("read blocked timer: %w", err)
		}
		record, err := decodeTimer(id, data)
		if err != nil {
			return nil, err
		}
		if record.Compatibility != nil {
			records = append(records, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list blocked timers: %w", err)
	}
	return records, nil
}

// ClearTimerCompatibility clears only the diagnostic and generation that were revalidated.
func (s *Store[EntityID, StateID]) ClearTimerCompatibility(
	ctx context.Context, id string, generation uint64, expected transition.CompatibilityDiagnostic,
) error {
	if id == "" || generation == 0 {
		return transition.ErrInvalidRequest
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_timers WHERE id = ?`, id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrInvalidRequest
		}
		if err != nil {
			return fmt.Errorf("read blocked timer: %w", err)
		}
		record, err := decodeTimer(id, data)
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
		data, err = json.Marshal(record)
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
	if lease.ID == "" || lease.Owner == "" || lease.Generation == 0 || lease.ExpiresAt.IsZero() || clock == nil {
		return transition.ErrInvalidLease
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_timers WHERE id = ?`, lease.ID).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return fmt.Errorf("read timer claim: %w", err)
		}
		record, err := decodeTimer(lease.ID, data)
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
		data, err = json.Marshal(record)
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
	if lease.ID == "" || lease.Owner == "" || lease.Generation == 0 || clock == nil || ttl <= 0 {
		return transition.TimerLease{}, transition.ErrInvalidLease
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		expiry, err := leaseExpiry(now, ttl)
		if err != nil {
			return err
		}
		var data []byte
		err = tx.QueryRowContext(ctx, `SELECT record FROM rff_timers WHERE id = ?`, lease.ID).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrStaleLease
		}
		if err != nil {
			return fmt.Errorf("read timer claim: %w", err)
		}
		record, err := decodeTimer(lease.ID, data)
		if err != nil {
			return err
		}
		if record.Completed || record.Compatibility != nil || record.Owner != lease.Owner ||
			record.Generation != lease.Generation || !now.Before(record.LeaseExpiresAt) {
			return transition.ErrStaleLease
		}
		record.LeaseExpiresAt = expiry
		data, err = json.Marshal(record)
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

func validateTimers(ctx context.Context, tx *sql.Tx, adapterVersion int) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, record FROM rff_timers`)
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
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return fmt.Errorf("%w: read timer: %w", ErrUnsupportedSchema, err)
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
