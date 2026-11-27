package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationrecord"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func (s *Store[E, S]) continuationDriveTransaction(
	ctx context.Context, id string,
	fn func(*sql.Tx, transition.ContinuationMarker[E, S], *transition.ContinuationProgress) error,
) error {
	if err := transition.ValidateDurableID(id); err != nil {
		return err
	}
	if id == "" || fn == nil {
		return transition.ErrInvalidRequest
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		digest := continuationrecord.Digest(id)
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_continuations WHERE digest = ?`, digest[:]).Scan(&data); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return transition.ErrContinuationPending
			}
			return err
		}
		marker, err := s.decodeContinuation(digest[:], data)
		if err != nil {
			return err
		}
		if marker.ExecutionID != id {
			return transition.ErrConflict
		}
		if err := s.validateContinuationOrigin(ctx, tx, marker); err != nil {
			return err
		}
		progress, err := continuationrecord.Progress(data)
		if err != nil {
			return fmt.Errorf("%w: invalid continuation progress", ErrUnsupportedSchema)
		}
		if progress == nil {
			progress = &transition.ContinuationProgress{Version: 1, Phase: transition.ContinuationWaiting}
		}
		if err := fn(tx, marker, progress); err != nil {
			return err
		}
		data, err = continuationrecord.SetProgress(data, progress)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE rff_continuations SET record = ? WHERE digest = ?`, data, digest[:])
		return err
	})
}

func (s *Store[E, S]) AcquireContinuation(
	ctx context.Context, marker transition.ContinuationMarker[E, S], owner string, clock func() time.Time, duration time.Duration,
) (result transition.ContinuationLease, err error) {
	if owner == "" || clock == nil || duration <= 0 {
		return result, transition.ErrInvalidLease
	}
	err = s.continuationDriveTransaction(ctx, marker.ExecutionID, func(tx *sql.Tx, stored transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		if err := transition.MatchContinuationMarkers(stored, marker); err != nil {
			return err
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, marker.ExecutionID).Scan(&data); err != nil {
			return err
		}
		execution, err := s.decodeRecord(data)
		if err != nil {
			return err
		}
		if execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded ||
			execution.CommitReceipt.ExecutionID != marker.ExecutionID || execution.CommitReceipt.OperationID != marker.OperationID ||
			execution.CommitReceipt.Failure != nil || execution.Status != transition.Complete {
			return transition.ErrContinuationPending
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE execution_id = ?)`, marker.ExecutionID).Scan(&active); err != nil {
			return err
		}
		if active {
			return transition.ErrContinuationPending
		}
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		if progress.Phase != transition.ContinuationWaiting && progress.Phase != transition.ContinuationDriving && progress.Phase != transition.ContinuationDecided {
			return transition.ErrInvalidDecision
		}
		if progress.CycleStarted.IsZero() {
			progress.CycleStarted = now
			progress.Deadline = now.Add(marker.Policy.OverallTimeout)
		}
		if now.Before(progress.LeaseExpires) {
			if !bytes.Equal(progress.Owner, []byte(owner)) {
				return transition.ErrLeaseHeld
			}
		} else {
			progress.Generation++
			progress.Owner = []byte(owner)
			progress.LeaseExpires = now.Add(duration)
		}
		result = transition.ContinuationLease{ExecutionID: marker.ExecutionID, Owner: owner, Generation: progress.Generation, ExpiresAt: progress.LeaseExpires}
		return nil
	})
	if err != nil {
		return transition.ContinuationLease{}, err
	}
	return result, nil
}

func validContinuationLease(progress *transition.ContinuationProgress, lease transition.ContinuationLease, now time.Time) bool {
	return lease.ExecutionID != "" && lease.Owner != "" && lease.Generation != 0 &&
		progress.Generation == lease.Generation && bytes.Equal(progress.Owner, []byte(lease.Owner)) &&
		now.Before(progress.LeaseExpires) && lease.ExpiresAt.Equal(progress.LeaseExpires)
}

func (s *Store[E, S]) continuationReservationBusy(ctx context.Context, tx *sql.Tx, entity E) (bool, error) {
	encoded, err := s.encodeEntity(entity)
	if err != nil {
		return false, err
	}
	var busy bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE entity = ? UNION ALL SELECT 1 FROM rff_successors WHERE entity = ? AND phase IN ('prepared', 'denied'))`, encoded, encoded).Scan(&busy)
	return busy, err
}

func (s *Store[E, S]) ParkContinuation(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.continuationDriveTransaction(ctx, lease.ExecutionID, func(tx *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if (progress.Phase != transition.ContinuationDriving && progress.Phase != transition.ContinuationDecided) || progress.StartedGeneration != lease.Generation || !progress.PausedAt.IsZero() ||
			!now.Before(progress.Deadline) || !now.Before(progress.AttemptDeadline(marker.Policy)) {
			return transition.ErrInvalidDecision
		}
		digest := continuationrecord.Digest(marker.ExecutionID)
		var successor bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_successors WHERE digest = ?)`, digest[:]).Scan(&successor); err != nil {
			return err
		}
		if successor {
			return transition.ErrSuccessorPending
		}
		busy, err := s.continuationReservationBusy(ctx, tx, marker.Entity)
		if err != nil {
			return err
		}
		if !busy {
			return transition.ErrInvalidDecision
		}
		progress.PausedAt = now
		return nil
	})
}

func (s *Store[E, S]) ResumeContinuation(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.continuationDriveTransaction(ctx, lease.ExecutionID, func(tx *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if (progress.Phase != transition.ContinuationDriving && progress.Phase != transition.ContinuationDecided) || progress.PausedAt.IsZero() || now.Before(progress.PausedAt) {
			return transition.ErrInvalidDecision
		}
		busy, err := s.continuationReservationBusy(ctx, tx, marker.Entity)
		if err != nil {
			return err
		}
		if busy {
			return transition.ErrBusy
		}
		paused := now.Sub(progress.PausedAt)
		progress.Deadline = progress.Deadline.Add(paused)
		progress.AttemptPaused += paused
		progress.PausedAt = time.Time{}
		progress.StartedGeneration = lease.Generation
		return nil
	})
}

func (s *Store[E, S]) StartContinuation(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	var outcome error
	err := s.continuationDriveTransaction(ctx, lease.ExecutionID, func(_ *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if progress.Phase != transition.ContinuationWaiting || progress.CompatibilityBlocked {
			return transition.ErrInvalidDecision
		}
		if progress.Attempts >= marker.Policy.MaxAttempts || !now.Before(progress.Deadline) {
			progress.Phase = transition.ContinuationIntervention
			outcome = transition.ErrContinuationIntervention
			return nil
		}
		if now.Before(progress.NextAttemptAt) {
			return transition.ErrContinuationNotDue
		}
		progress.Attempts++
		progress.StartedAt = now
		progress.AttemptPaused = 0
		progress.StartedGeneration = lease.Generation
		progress.Phase = transition.ContinuationDriving
		return nil
	})
	return errors.Join(err, outcome)
}

func (s *Store[E, S]) SetContinuationCompatibility(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time, blocked bool) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	var outcome error
	err := s.continuationDriveTransaction(ctx, lease.ExecutionID, func(_ *sql.Tx, _ transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if progress.Phase != transition.ContinuationWaiting {
			return transition.ErrInvalidDecision
		}
		progress.CompatibilityBlocked = blocked
		if !now.Before(progress.Deadline) {
			progress.Phase = transition.ContinuationIntervention
			outcome = transition.ErrContinuationIntervention
		}
		return nil
	})
	return errors.Join(err, outcome)
}

func (s *Store[E, S]) FailContinuationAttempt(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time, failure fsm.Failure, disposition fsm.Disposition) error {
	return s.failContinuationAttempt(ctx, lease, clock, failure, disposition, false)
}

func (s *Store[E, S]) RecoverContinuationAttempt(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time) error {
	return s.failContinuationAttempt(ctx, lease, clock, fsm.Failure{Code: "attempt_outcome_unknown"}, fsm.DispositionRetry, true)
}

func (s *Store[E, S]) failContinuationAttempt(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time, failure fsm.Failure, disposition fsm.Disposition, recovering bool) error {
	if clock == nil || (disposition != fsm.DispositionRetry && disposition != fsm.DispositionIntervention) {
		return transition.ErrInvalidDecision
	}
	return s.continuationDriveTransaction(ctx, lease.ExecutionID, func(tx *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if (progress.Phase != transition.ContinuationDriving && progress.Phase != transition.ContinuationDecided) ||
			(recovering == (progress.StartedGeneration == lease.Generation)) || !progress.PausedAt.IsZero() {
			return transition.ErrInvalidDecision
		}
		digest := continuationrecord.Digest(marker.ExecutionID)
		var successor bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_successors WHERE digest = ? AND phase IN ('prepared', 'denied', 'accepted'))`, digest[:]).Scan(&successor); err != nil {
			return err
		}
		if successor {
			return transition.ErrSuccessorPending
		}
		progress.History = append(progress.History, transition.ContinuationAttempt{StartedAt: progress.StartedAt, EndedAt: now, Failure: failure})
		retry, delay, err := transition.DecideRetry(marker.Policy, progress.Attempts, progress.Attempts-1, disposition)
		if err != nil {
			return err
		}
		if !retry || !now.Before(progress.Deadline) {
			progress.Phase = transition.ContinuationIntervention
		} else {
			progress.Phase = transition.ContinuationWaiting
			progress.NextAttemptAt = now.Add(delay)
		}
		return nil
	})
}

func (s *Store[E, S]) RecordContinuationDecision(ctx context.Context, lease transition.ContinuationLease, clock func() time.Time, decision transition.ContinuationDecision) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	return s.continuationDriveTransaction(ctx, lease.ExecutionID, func(_ *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if progress.Phase != transition.ContinuationDriving || progress.Decision != nil || progress.StartedGeneration != lease.Generation || !progress.PausedAt.IsZero() || !now.Before(progress.Deadline) || !now.Before(progress.AttemptDeadline(marker.Policy)) {
			return transition.ErrInvalidDecision
		}
		progress.Decision = &decision
		if decision.Continue {
			progress.Phase = transition.ContinuationDecided
		} else {
			progress.Phase = transition.ContinuationStopped
		}
		return nil
	})
}

// PrepareContinuationSuccessor serializes preparation with the drive fence
// and waiver controls. The callback has the same restrictions as PrepareSuccessor.
func (s *Store[E, S]) PrepareContinuationSuccessor(
	ctx context.Context, lease transition.ContinuationLease, clock func() time.Time,
	decide func(*transition.SuccessorBinding[E, S]) (*transition.SuccessorBinding[E, S], error),
) (binding transition.SuccessorBinding[E, S], created bool, err error) {
	if clock == nil || decide == nil {
		return binding, false, transition.ErrInvalidRequest
	}
	err = s.continuationDriveTransaction(ctx, lease.ExecutionID, func(tx *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		now := clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if (progress.Phase != transition.ContinuationDriving && progress.Phase != transition.ContinuationDecided) ||
			progress.StartedGeneration != lease.Generation || !progress.PausedAt.IsZero() ||
			!now.Before(progress.Deadline) || !now.Before(progress.AttemptDeadline(marker.Policy)) ||
			progress.Decision == nil || !progress.Decision.Continue {
			return transition.ErrInvalidDecision
		}
		var prepareErr error
		binding, created, prepareErr = s.prepareSuccessor(ctx, tx, marker, decide)
		if prepareErr != nil {
			return prepareErr
		}
		now = clock()
		if !validContinuationLease(progress, lease, now) {
			return transition.ErrStaleLease
		}
		if !now.Before(progress.Deadline) || !now.Before(progress.AttemptDeadline(marker.Policy)) {
			return context.DeadlineExceeded
		}
		decision := progress.Decision
		if binding.Execution.Event.Codec != decision.Event.Codec || !bytes.Equal(binding.Execution.Event.Data, decision.Event.Data) ||
			binding.Execution.Input.Codec != decision.Input.Codec || !bytes.Equal(binding.Execution.Input.Data, decision.Input.Data) {
			return transition.ErrConflict
		}
		progress.Phase = transition.ContinuationAccepting
		progress.SuccessorID = []byte(binding.Execution.ID)
		progress.PermitExpires = progress.Deadline
		if expiry := progress.AttemptDeadline(marker.Policy); expiry.Before(progress.PermitExpires) {
			progress.PermitExpires = expiry
		}
		if lease.ExpiresAt.Before(progress.PermitExpires) {
			progress.PermitExpires = lease.ExpiresAt
		}
		return nil
	})
	if err != nil {
		return transition.SuccessorBinding[E, S]{}, false, err
	}
	return binding, created, nil
}

// RecordContinuationDenial fences a host denial to the attempt that issued it.
func (s *Store[E, S]) RecordContinuationDenial(ctx context.Context, binding transition.SuccessorBinding[E, S], principal string, permit transition.ContinuationDeliveryPermit, clock func() time.Time) error {
	if clock == nil || principal == "" {
		return transition.ErrInvalidRequest
	}
	return s.continuationDriveTransaction(ctx, binding.Origin.ExecutionID, func(tx *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		if err := transition.MatchContinuationMarkers(marker, binding.Origin); err != nil {
			return err
		}
		if progress.Phase != transition.ContinuationAccepting || permit.Generation != progress.StartedGeneration ||
			!permit.ExpiresAt.Equal(progress.PermitExpires) {
			return transition.ErrStaleLease
		}
		if err := s.recordSuccessorDenial(ctx, tx, binding, principal); err != nil {
			return err
		}
		progress.History = append(progress.History, transition.ContinuationAttempt{
			StartedAt: progress.StartedAt, EndedAt: clock(), Failure: fsm.Failure{Code: "successor_denied"},
		})
		progress.Phase = transition.ContinuationIntervention
		progress.Generation++
		progress.Owner = nil
		progress.LeaseExpires = time.Time{}
		return nil
	})
}

// ResolveContinuationAbsent follows an authoritative host lookup serialized
// with acceptance and only after all prior delivery permits expired.
func (s *Store[E, S]) ResolveContinuationAbsent(ctx context.Context, marker transition.ContinuationMarker[E, S], permit transition.ContinuationDeliveryPermit, clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidRequest
	}
	var outcome error
	err := s.continuationDriveTransaction(ctx, marker.ExecutionID, func(tx *sql.Tx, stored transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		if err := transition.MatchContinuationMarkers(stored, marker); err != nil {
			return err
		}
		now := clock()
		if permit.Generation != progress.StartedGeneration || !permit.ExpiresAt.Equal(progress.PermitExpires) {
			return transition.ErrStaleLease
		}
		if progress.Phase != transition.ContinuationAccepting || progress.PermitExpires.IsZero() || now.Before(progress.PermitExpires) {
			return transition.ErrSuccessorPending
		}
		digest := continuationrecord.Digest(marker.ExecutionID)
		var phase string
		if err := tx.QueryRowContext(ctx, `SELECT phase FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&phase); err != nil {
			return err
		}
		if phase != "prepared" {
			return transition.ErrSuccessorPending
		}
		progress.History = append(progress.History, transition.ContinuationAttempt{
			StartedAt: progress.StartedAt, EndedAt: now, Failure: fsm.Failure{Code: "successor_outcome_absent"},
		})
		retry, delay, err := transition.DecideRetry(marker.Policy, progress.Attempts, progress.Attempts-1, fsm.DispositionRetry)
		if err != nil {
			return err
		}
		progress.Generation++
		progress.Owner = nil
		progress.LeaseExpires = time.Time{}
		if !retry || !now.Before(progress.Deadline) {
			progress.Phase = transition.ContinuationIntervention
			outcome = transition.ErrContinuationIntervention
		} else {
			progress.Phase = transition.ContinuationWaiting
			progress.NextAttemptAt = now.Add(delay)
		}
		return nil
	})
	return errors.Join(err, outcome)
}

// ReconcileContinuation projects authoritative successor state without
// authorizing a new delivery or requiring remaining attempt budget.
func (s *Store[E, S]) ReconcileContinuation(ctx context.Context, marker transition.ContinuationMarker[E, S], clock func() time.Time) error {
	if clock == nil {
		return transition.ErrInvalidRequest
	}
	return s.continuationDriveTransaction(ctx, marker.ExecutionID, func(tx *sql.Tx, stored transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		if err := transition.MatchContinuationMarkers(stored, marker); err != nil {
			return err
		}
		digest := continuationrecord.Digest(marker.ExecutionID)
		var entity, data []byte
		var phase string
		if err := tx.QueryRowContext(ctx, `SELECT entity, phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&entity, &phase, &data); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if progress.Phase == transition.ContinuationWaiverPending {
					return s.finalizeContinuationWaiver(ctx, tx, marker, progress)
				}
				return transition.ErrSuccessorPending
			}
			return err
		}
		binding, err := s.decodeSuccessor(digest[:], entity, data)
		if err != nil {
			return err
		}
		if err := transition.MatchContinuationMarkers(binding.Origin, marker); err != nil {
			return err
		}
		var record successorRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return ErrUnsupportedSchema
		}
		switch phase {
		case "accepted":
			if len(record.Principal) == 0 || string(record.Decision) != "allowed" || record.AcceptedAt.IsZero() || len(record.Cancellation) != 0 ||
				progress.Phase == transition.ContinuationWaived || progress.Phase == transition.ContinuationStopped || progress.Phase == transition.ContinuationSuperseded {
				return transition.ErrConflict
			}
			progress.Phase = transition.ContinuationAccepted
			progress.SuccessorID = []byte(binding.Execution.ID)
			return nil
		case "superseded":
			if len(record.Cancellation) == 0 || progress.Phase == transition.ContinuationWaived || progress.Phase == transition.ContinuationStopped || progress.Phase == transition.ContinuationAccepted {
				return transition.ErrConflict
			}
			progress.Phase = transition.ContinuationSuperseded
			return nil
		case "cancelled":
			if progress.Phase == transition.ContinuationWaiverPending {
				return s.finalizeContinuationWaiver(ctx, tx, marker, progress)
			}
			return transition.ErrSuccessorCancelled
		case "denied":
			if len(record.Denials) == 0 {
				return ErrUnsupportedSchema
			}
			if progress.Phase == transition.ContinuationWaiverPending {
				return transition.ErrSuccessorPending
			}
			if progress.Phase != transition.ContinuationIntervention {
				progress.History = append(progress.History, transition.ContinuationAttempt{
					StartedAt: progress.StartedAt, EndedAt: clock(), Failure: fsm.Failure{Code: "successor_denied"},
				})
				progress.Phase = transition.ContinuationIntervention
				progress.Generation++
				progress.Owner = nil
				progress.LeaseExpires = time.Time{}
			}
			return nil
		case "prepared":
			return transition.ErrSuccessorPending
		default:
			return ErrUnsupportedSchema
		}
	})
}

func (s *Store[E, S]) ReadContinuation(ctx context.Context, executionID string) (result transition.ContinuationProgress, err error) {
	err = s.continuationDriveTransaction(ctx, executionID, func(_ *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		result = *progress
		result.Policy = marker.Policy
		return nil
	})
	return result, err
}

func (s *Store[E, S]) finalizeContinuationWaiver(ctx context.Context, tx *sql.Tx, marker transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
	digest := continuationrecord.Digest(marker.ExecutionID)
	var phase string
	err := tx.QueryRowContext(ctx, `SELECT phase FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&phase)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	switch phase {
	case "prepared", "denied", "accepted", "superseded":
		return nil
	case "", "cancelled":
	default:
		return ErrUnsupportedSchema
	}
	var data []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, marker.ExecutionID).Scan(&data); err != nil {
		return err
	}
	execution, err := s.decodeRecord(data)
	if err != nil {
		return err
	}
	if execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded || execution.Status != transition.Complete {
		return nil
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE execution_id = ?)`, marker.ExecutionID).Scan(&active); err != nil {
		return err
	}
	if !active {
		progress.Phase = transition.ContinuationWaived
	}
	return nil
}

// ApplyContinuationControl is an internal primitive; callers must authorize
// each request and replay before invoking it.
func (s *Store[E, S]) ApplyContinuationControl(ctx context.Context, marker transition.ContinuationMarker[E, S], clock func() time.Time, control transition.ContinuationControl) error {
	if clock == nil || control.ID == "" || control.Principal == "" || control.Reason == "" {
		return transition.ErrInvalidRequest
	}
	return s.continuationDriveTransaction(ctx, marker.ExecutionID, func(tx *sql.Tx, stored transition.ContinuationMarker[E, S], progress *transition.ContinuationProgress) error {
		if err := transition.MatchContinuationMarkers(stored, marker); err != nil {
			return err
		}
		for _, prior := range progress.Controls {
			if bytes.Equal(prior.ID, []byte(control.ID)) {
				if prior.Action != control.Action || !bytes.Equal(prior.Principal, []byte(control.Principal)) || !bytes.Equal(prior.Reason, []byte(control.Reason)) {
					return transition.ErrConflict
				}
				if control.Action == transition.WaiveContinuation && progress.Phase == transition.ContinuationWaiverPending {
					return s.finalizeContinuationWaiver(ctx, tx, marker, progress)
				}
				return nil
			}
		}
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidDecision
		}
		switch control.Action {
		case transition.RetryContinuation:
			if progress.Phase != transition.ContinuationIntervention {
				return transition.ErrInvalidDecision
			}
		case transition.WaiveContinuation:
			if progress.Phase == transition.ContinuationStopped || progress.Phase == transition.ContinuationWaiverPending || progress.Phase == transition.ContinuationWaived || progress.Phase == transition.ContinuationAccepted || progress.Phase == transition.ContinuationSuperseded {
				return transition.ErrInvalidDecision
			}
			digest := continuationrecord.Digest(marker.ExecutionID)
			var phase string
			err := tx.QueryRowContext(ctx, `SELECT phase FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&phase)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if phase == "accepted" || phase == "superseded" {
				return transition.ErrInvalidDecision
			}
		default:
			return transition.ErrInvalidDecision
		}
		progress.Controls = append(progress.Controls, transition.ContinuationControlAudit{
			ID: []byte(control.ID), Action: control.Action, Principal: []byte(control.Principal), Reason: []byte(control.Reason), At: now,
		})
		progress.Generation++
		progress.Owner = nil
		progress.LeaseExpires = time.Time{}
		if control.Action == transition.WaiveContinuation {
			progress.Phase = transition.ContinuationWaiverPending
			return s.finalizeContinuationWaiver(ctx, tx, marker, progress)
		}
		digest := continuationrecord.Digest(marker.ExecutionID)
		if _, err := tx.ExecContext(ctx, `UPDATE rff_successors SET phase = 'prepared' WHERE digest = ? AND phase = 'denied'`, digest[:]); err != nil {
			return err
		}
		progress.Attempts = 0
		progress.CycleStarted = now
		progress.Deadline = now.Add(marker.Policy.OverallTimeout)
		progress.NextAttemptAt = time.Time{}
		progress.StartedAt = time.Time{}
		progress.AttemptPaused = 0
		progress.StartedGeneration = 0
		progress.PausedAt = time.Time{}
		progress.Phase = transition.ContinuationWaiting
		return nil
	})
}
