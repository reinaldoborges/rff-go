package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func leaseExpiry(now time.Time, ttl time.Duration) (time.Time, error) {
	expiry := now.Add(ttl).UTC()
	if now.IsZero() || ttl <= 0 || expiry.Year() < 1 || expiry.Year() > 9999 {
		return time.Time{}, transition.ErrInvalidLease
	}
	return expiry, nil
}

// AcquireTransitionLease acquires or reclaims the active entity's coordinator lease.
func (s *Store[EntityID, StateID]) AcquireTransitionLease(
	ctx context.Context, entity EntityID, executionID, owner string, clock func() time.Time, ttl time.Duration,
) (result transition.TransitionLease[EntityID], err error) {
	if err := transition.ValidateDurableID(executionID); err != nil {
		return result, errors.Join(err, transition.ErrInvalidLease)
	}
	if clock == nil || ttl <= 0 || executionID == "" || owner == "" {
		return result, transition.ErrInvalidLease
	}
	encoded, err := s.encodeEntity(entity)
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin lease acquisition: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback lease acquisition: %w", rollbackErr))
		}
	}()
	now := clock()
	expiry, err := leaseExpiry(now, ttl)
	if err != nil {
		return result, err
	}
	var active string
	if err := tx.QueryRowContext(ctx, `SELECT execution_id FROM rff_active WHERE entity = ?`, encoded).Scan(&active); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, transition.ErrStaleLease
		}
		return result, fmt.Errorf("read active execution: %w", err)
	}
	if active != executionID {
		return result, transition.ErrStaleLease
	}
	var priorID, priorOwner, expiryText string
	var generation int64
	err = tx.QueryRowContext(ctx, `SELECT execution_id, owner, generation, expires_at FROM rff_transition_leases WHERE entity = ?`, encoded).
		Scan(&priorID, &priorOwner, &generation, &expiryText)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf("read transition lease: %w", err)
	}
	if err == nil {
		priorExpiry, parseErr := time.Parse(time.RFC3339Nano, expiryText)
		if parseErr != nil || generation <= 0 || priorOwner == "" {
			return result, fmt.Errorf("%w: invalid transition lease", ErrUnsupportedSchema)
		}
		if priorID == executionID && now.Before(priorExpiry) {
			if priorOwner != owner {
				return result, transition.ErrLeaseHeld
			}
			return transition.TransitionLease[EntityID]{Entity: entity, ExecutionID: executionID, Owner: owner, Generation: uint64(generation), ExpiresAt: priorExpiry}, nil
		}
		if generation == math.MaxInt64 {
			return result, fmt.Errorf("transition lease generation exhausted")
		}
	}
	generation++
	_, err = tx.ExecContext(ctx, `INSERT INTO rff_transition_leases (entity, execution_id, owner, generation, expires_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(entity) DO UPDATE SET execution_id = excluded.execution_id,
		owner = excluded.owner, generation = excluded.generation, expires_at = excluded.expires_at`,
		encoded, executionID, owner, generation, expiry.Format(time.RFC3339Nano))
	if err != nil {
		return result, fmt.Errorf("write transition lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit lease acquisition: %w", err)
	}
	return transition.TransitionLease[EntityID]{Entity: entity, ExecutionID: executionID, Owner: owner, Generation: uint64(generation), ExpiresAt: expiry}, nil
}

// RenewTransitionLease extends an unexpired lease only for its current fence.
func (s *Store[EntityID, StateID]) RenewTransitionLease(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, ttl time.Duration,
) (result transition.TransitionLease[EntityID], err error) {
	if err := transition.ValidateDurableID(lease.ExecutionID); err != nil {
		return result, errors.Join(err, transition.ErrInvalidLease)
	}
	if clock == nil || ttl <= 0 || lease.ExecutionID == "" || lease.Owner == "" || lease.Generation == 0 || lease.Generation > math.MaxInt64 {
		return result, transition.ErrInvalidLease
	}
	encoded, err := s.encodeEntity(lease.Entity)
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin lease renewal: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback lease renewal: %w", rollbackErr))
		}
	}()
	now := clock()
	expiry, err := leaseExpiry(now, ttl)
	if err != nil {
		return result, err
	}
	var oldExpiry string
	err = tx.QueryRowContext(ctx, `SELECT l.expires_at FROM rff_transition_leases AS l
		JOIN rff_active AS a ON a.entity = l.entity AND a.execution_id = l.execution_id
		WHERE l.entity = ? AND l.execution_id = ? AND l.owner = ? AND l.generation = ?`,
		encoded, lease.ExecutionID, lease.Owner, int64(lease.Generation)).Scan(&oldExpiry)
	if errors.Is(err, sql.ErrNoRows) {
		return result, transition.ErrStaleLease
	}
	if err != nil {
		return result, fmt.Errorf("read transition lease: %w", err)
	}
	previousExpiry, err := time.Parse(time.RFC3339Nano, oldExpiry)
	if err != nil {
		return result, fmt.Errorf("%w: invalid transition lease expiry", ErrUnsupportedSchema)
	}
	if !now.Before(previousExpiry) {
		return result, transition.ErrStaleLease
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rff_transition_leases SET expires_at = ? WHERE entity = ?`, expiry.Format(time.RFC3339Nano), encoded); err != nil {
		return result, fmt.Errorf("renew transition lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit lease renewal: %w", err)
	}
	lease.ExpiresAt = expiry
	return lease, nil
}
