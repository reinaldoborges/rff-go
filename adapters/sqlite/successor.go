package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/internal/continuationrecord"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type successorRecord struct {
	Version      int
	Origin       []byte
	Revision     []byte
	Execution    []byte
	Principal    []byte
	Decision     []byte
	Cancellation []byte
	AcceptedAt   time.Time
	Denials      [][]byte
}

func (s *Store[EntityID, StateID]) validateSuccessors(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT digest, entity, execution_id, phase, record FROM rff_successors`)
	if err != nil {
		return fmt.Errorf("%w: read successor records: %w", ErrUnsupportedSchema, err)
	}
	defer rows.Close()
	preparedEntities := make(map[string]bool)
	for rows.Next() {
		var digest, entity, id, data []byte
		var phase string
		if err := rows.Scan(&digest, &entity, &id, &phase, &data); err != nil {
			return fmt.Errorf("%w: read successor record: %w", ErrUnsupportedSchema, err)
		}
		binding, err := s.decodeSuccessor(digest, entity, data)
		if err != nil {
			return err
		}
		var record successorRecord
		if err := json.Unmarshal(data, &record); err != nil || !bytes.Equal(id, []byte(binding.Execution.ID)) {
			return fmt.Errorf("%w: successor execution index mismatch", ErrUnsupportedSchema)
		}
		switch phase {
		case "prepared", "denied":
			if len(record.Principal) != 0 || len(record.Decision) != 0 || len(record.Cancellation) != 0 || !record.AcceptedAt.IsZero() || preparedEntities[string(entity)] {
				return fmt.Errorf("%w: invalid prepared successor", ErrUnsupportedSchema)
			}
			if phase == "denied" && len(record.Denials) == 0 {
				return fmt.Errorf("%w: missing successor denial", ErrUnsupportedSchema)
			}
			preparedEntities[string(entity)] = true
		case "accepted":
			if len(record.Principal) == 0 || string(record.Decision) != "allowed" || len(record.Cancellation) != 0 || record.AcceptedAt.IsZero() {
				return fmt.Errorf("%w: invalid successor receipt", ErrUnsupportedSchema)
			}
		case "cancelled", "superseded":
			if len(record.Cancellation) == 0 || len(record.Principal) != 0 || len(record.Decision) != 0 || !record.AcceptedAt.IsZero() {
				return fmt.Errorf("%w: invalid successor cancellation", ErrUnsupportedSchema)
			}
		default:
			return fmt.Errorf("%w: invalid successor phase", ErrUnsupportedSchema)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read successors: %w", ErrUnsupportedSchema, err)
	}
	return nil
}

func (s *Store[EntityID, StateID]) decodeSuccessor(digest, entity, data []byte) (transition.SuccessorBinding[EntityID, StateID], error) {
	var binding transition.SuccessorBinding[EntityID, StateID]
	var record successorRecord
	if err := json.Unmarshal(data, &record); err != nil || record.Version != 1 || len(record.Revision) == 0 {
		return binding, fmt.Errorf("%w: invalid successor prepare", ErrUnsupportedSchema)
	}
	marker, err := s.decodeContinuation(digest, record.Origin)
	if err != nil {
		return binding, err
	}
	expectedEntity, err := s.encodeEntity(marker.Entity)
	if err != nil || !bytes.Equal(entity, expectedEntity) {
		return binding, fmt.Errorf("%w: successor entity mismatch", ErrUnsupportedSchema)
	}
	execution, err := s.decodeRecord(record.Execution)
	if err != nil {
		return binding, err
	}
	binding = transition.SuccessorBinding[EntityID, StateID]{Origin: marker, OriginRevision: bytes.Clone(record.Revision), Execution: execution}
	if err := validateSuccessorBinding(binding); err != nil {
		return transition.SuccessorBinding[EntityID, StateID]{}, fmt.Errorf("%w: invalid successor binding", ErrUnsupportedSchema)
	}
	return binding, nil
}

func validateSuccessorBinding[E comparable, S comparable](binding transition.SuccessorBinding[E, S]) error {
	return transition.ValidateSuccessorBinding(binding)
}

// InspectSuccessor reads retained protocol state for internal recovery. The
// host must authorize disclosure of this record to external callers.
func (s *Store[EntityID, StateID]) InspectSuccessor(
	ctx context.Context, marker transition.ContinuationMarker[EntityID, StateID],
) (transition.SuccessorInspection[EntityID, StateID], error) {
	var result transition.SuccessorInspection[EntityID, StateID]
	if err := transition.ValidateContinuationMarker(marker); err != nil {
		return result, err
	}
	digest := continuationrecord.Digest(marker.ExecutionID)
	var entity, data []byte
	var phase string
	if err := s.db.QueryRowContext(ctx, `SELECT entity, phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&entity, &phase, &data); err != nil {
		return result, err
	}
	binding, err := s.decodeSuccessor(digest[:], entity, data)
	if err != nil {
		return result, err
	}
	if err := transition.MatchContinuationMarkers(binding.Origin, marker); err != nil {
		return result, err
	}
	var record successorRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return result, fmt.Errorf("%w: invalid successor record", ErrUnsupportedSchema)
	}
	switch phase {
	case "prepared", "denied":
		if len(record.Principal) != 0 || len(record.Decision) != 0 || len(record.Cancellation) != 0 || !record.AcceptedAt.IsZero() {
			return result, ErrUnsupportedSchema
		}
		if phase == "denied" && len(record.Denials) == 0 {
			return result, ErrUnsupportedSchema
		}
	case "accepted":
		if len(record.Principal) == 0 || string(record.Decision) != "allowed" || len(record.Cancellation) != 0 || record.AcceptedAt.IsZero() {
			return result, ErrUnsupportedSchema
		}
	case "cancelled", "superseded":
		if len(record.Cancellation) == 0 || len(record.Principal) != 0 || len(record.Decision) != 0 || !record.AcceptedAt.IsZero() {
			return result, ErrUnsupportedSchema
		}
	default:
		return result, ErrUnsupportedSchema
	}
	denials := make([]string, len(record.Denials))
	for i := range record.Denials {
		denials[i] = string(record.Denials[i])
	}
	return transition.SuccessorInspection[EntityID, StateID]{
		Binding: binding, Phase: transition.SuccessorPhase(phase), Principal: string(record.Principal),
		Decision: string(record.Decision), Cancellation: bytes.Clone(record.Cancellation), Denials: denials,
	}, nil
}

// ReadSuccessorExecution is an internal primitive; the host must authorize
// any disclosure to external callers.
func (s *Store[EntityID, StateID]) ReadSuccessorExecution(
	ctx context.Context, marker transition.ContinuationMarker[EntityID, StateID],
) (transition.Execution[EntityID, StateID], error) {
	var zero transition.Execution[EntityID, StateID]
	inspection, err := s.InspectSuccessor(ctx, marker)
	if err != nil {
		return zero, err
	}
	if inspection.Phase != transition.SuccessorAccepted {
		return zero, transition.ErrSuccessorPending
	}
	var data []byte
	if err := s.db.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, inspection.Binding.Execution.ID).Scan(&data); err != nil {
		return zero, err
	}
	execution, err := s.decodeRecord(data)
	if err != nil {
		return zero, err
	}
	if execution.ID != inspection.Binding.Execution.ID || execution.Entity != marker.Entity || execution.Key != inspection.Binding.Execution.Key {
		return zero, transition.ErrConflict
	}
	return execution, nil
}

// PrepareSuccessor reserves the entity without accepting an execution. The
// callback compares existing bindings on replay and must not re-enter the store.
// A prepared reservation cannot expire; recovery or fenced cancellation must
// resolve it.
func (s *Store[EntityID, StateID]) PrepareSuccessor(
	ctx context.Context,
	marker transition.ContinuationMarker[EntityID, StateID],
	decide func(*transition.SuccessorBinding[EntityID, StateID]) (*transition.SuccessorBinding[EntityID, StateID], error),
) (transition.SuccessorBinding[EntityID, StateID], bool, error) {
	return s.prepareSuccessor(ctx, nil, marker, decide)
}

func (s *Store[EntityID, StateID]) prepareSuccessor(
	ctx context.Context, shared *sql.Tx,
	marker transition.ContinuationMarker[EntityID, StateID],
	decide func(*transition.SuccessorBinding[EntityID, StateID]) (*transition.SuccessorBinding[EntityID, StateID], error),
) (binding transition.SuccessorBinding[EntityID, StateID], created bool, err error) {
	if decide == nil {
		return binding, false, transition.ErrInvalidRequest
	}
	if err := transition.ValidateContinuationMarker(marker); err != nil {
		return binding, false, err
	}
	entity, err := s.encodeEntity(marker.Entity)
	if err != nil {
		return binding, false, err
	}
	work := func(tx *sql.Tx) error {
		digest := continuationrecord.Digest(marker.ExecutionID)
		var record, storedEntity []byte
		var phase string
		err := tx.QueryRowContext(ctx, `SELECT entity, phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&storedEntity, &phase, &record)
		if err == nil {
			existing, err := s.decodeSuccessor(digest[:], storedEntity, record)
			if err != nil {
				return err
			}
			if err := transition.MatchContinuationMarkers(existing.Origin, marker); err != nil {
				return err
			}
			if phase != "prepared" && phase != "accepted" && phase != "cancelled" && phase != "superseded" && phase != "denied" {
				return fmt.Errorf("%w: unsupported successor phase", ErrUnsupportedSchema)
			}
			next, err := decide(&existing)
			if err != nil {
				return err
			}
			if next != nil {
				return transition.ErrInvalidDecision
			}
			if phase == "cancelled" {
				return transition.ErrSuccessorCancelled
			}
			if phase == "superseded" {
				return transition.ErrSuccessorSuperseded
			}
			binding, err = s.decodeSuccessor(digest[:], storedEntity, record)
			return err
		}

		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var markerData []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_continuations WHERE digest = ?`, digest[:]).Scan(&markerData); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return transition.ErrContinuationPending
			}
			return err
		}
		storedMarker, err := s.decodeContinuation(digest[:], markerData)
		if err != nil {
			return err
		}
		if err := transition.MatchContinuationMarkers(storedMarker, marker); err != nil {
			return err
		}
		if err := s.validateContinuationOrigin(ctx, tx, marker); err != nil {
			return err
		}
		var originData []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, marker.ExecutionID).Scan(&originData); err != nil {
			return err
		}
		origin, err := s.decodeRecord(originData)
		if err != nil {
			return err
		}
		if origin.CommitReceipt == nil || origin.CommitReceipt.Outcome != transition.CommitSucceeded ||
			origin.CommitReceipt.ExecutionID != marker.ExecutionID || origin.CommitReceipt.OperationID != marker.OperationID ||
			origin.CommitReceipt.Failure != nil || origin.Status != transition.Complete {
			return transition.ErrContinuationPending
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE entity = ? UNION ALL SELECT 1 FROM rff_successors WHERE entity = ? AND phase IN ('prepared', 'denied'))`, entity, entity).Scan(&active); err != nil {
			return err
		}
		if active {
			return transition.ErrBusy
		}
		next, err := decide(nil)
		if err != nil {
			return err
		}
		if next == nil || validateSuccessorBinding(*next) != nil {
			return transition.ErrInvalidDecision
		}
		if err := transition.MatchContinuationMarkers(marker, next.Origin); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_executions WHERE id = ? OR (entity = ? AND request_key = ?) UNION ALL SELECT 1 FROM rff_successors WHERE execution_id = ?)`, next.Execution.ID, entity, next.Execution.Key, []byte(next.Execution.ID)).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return transition.ErrConflict
		}
		executionData, err := s.encodeRecord(next.Execution, entity)
		if err != nil {
			return err
		}
		record, err = json.Marshal(successorRecord{Version: 1, Origin: markerData, Revision: next.OriginRevision, Execution: executionData})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_successors (digest, entity, execution_id, phase, record) VALUES (?, ?, ?, 'prepared', ?)`, digest[:], entity, []byte(next.Execution.ID), record); err != nil {
			return err
		}
		binding, err = s.decodeSuccessor(digest[:], entity, record)
		if err == nil {
			created = true
		}
		return err
	}
	if shared != nil {
		err = work(shared)
	} else {
		err = s.hookTransaction(ctx, work)
	}
	if err != nil {
		return transition.SuccessorBinding[EntityID, StateID]{}, false, err
	}
	return binding, created, nil
}

// FinalizeSuccessor installs the execution and runtime reservation after the
// authoritative host has durably accepted the exact binding and fenced all
// state writers. The caller must supply only a verified host receipt.
func (s *Store[EntityID, StateID]) FinalizeSuccessor(
	ctx context.Context, receipt transition.SuccessorReceipt[EntityID, StateID],
) (transition.Execution[EntityID, StateID], error) {
	return s.finalizeSuccessor(ctx, nil, receipt)
}

func (s *Store[EntityID, StateID]) finalizeSuccessor(
	ctx context.Context, shared *sql.Tx, receipt transition.SuccessorReceipt[EntityID, StateID],
) (execution transition.Execution[EntityID, StateID], err error) {
	binding := receipt.Binding
	if err := validateSuccessorBinding(binding); err != nil {
		return execution, err
	}
	if receipt.Principal == "" || receipt.Decision != "allowed" || receipt.AcceptedAt.IsZero() || binding.Execution.Policy.OverallTimeout <= 0 {
		return execution, transition.ErrInvalidRequest
	}
	work := func(tx *sql.Tx) error {
		digest := continuationrecord.Digest(binding.Origin.ExecutionID)
		var entity, data []byte
		var phase string
		if err := tx.QueryRowContext(ctx, `SELECT entity, phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&entity, &phase, &data); err != nil {
			return err
		}
		stored, err := s.decodeSuccessor(digest[:], entity, data)
		if err != nil {
			return err
		}
		if err := transition.MatchContinuationMarkers(stored.Origin, binding.Origin); err != nil {
			return err
		}
		if !bytes.Equal(stored.OriginRevision, binding.OriginRevision) {
			return transition.ErrConflict
		}
		encoded, err := s.encodeRecord(binding.Execution, entity)
		if err != nil {
			return err
		}
		var record successorRecord
		if err := json.Unmarshal(data, &record); err != nil || !bytes.Equal(record.Execution, encoded) {
			return transition.ErrConflict
		}
		if phase == "accepted" {
			if !bytes.Equal(record.Principal, []byte(receipt.Principal)) || !bytes.Equal(record.Decision, []byte(receipt.Decision)) || !record.AcceptedAt.Equal(receipt.AcceptedAt) {
				return transition.ErrConflict
			}
			if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, stored.Execution.ID).Scan(&encoded); err != nil {
				return err
			}
			execution, err = s.decodeRecord(encoded)
			return err
		}
		if (phase != "prepared" && phase != "denied") || len(record.Principal) != 0 || len(record.Decision) != 0 {
			return fmt.Errorf("%w: invalid successor phase", ErrUnsupportedSchema)
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_executions WHERE id = ? OR (entity = ? AND request_key = ?))`, stored.Execution.ID, entity, stored.Execution.Key).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return transition.ErrConflict
		}
		stored.Execution.AcceptedAt = receipt.AcceptedAt
		stored.Execution.Deadline = receipt.AcceptedAt.Add(stored.Execution.Policy.OverallTimeout)
		acceptedData, err := s.encodeRecord(stored.Execution, entity)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_executions (id, entity, request_key, record) VALUES (?, ?, ?, ?)`, stored.Execution.ID, entity, stored.Execution.Key, acceptedData); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_active (entity, execution_id) VALUES (?, ?)`, entity, stored.Execution.ID); err != nil {
			return err
		}
		record.Principal, record.Decision = []byte(receipt.Principal), []byte(receipt.Decision)
		record.AcceptedAt = receipt.AcceptedAt
		data, err = json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_successors SET phase = 'accepted', record = ? WHERE digest = ?`, data, digest[:]); err != nil {
			return err
		}
		execution = stored.Execution
		return nil
	}
	if shared != nil {
		err = work(shared)
	} else {
		err = s.hookTransaction(ctx, work)
	}
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, err
	}
	return execution, nil
}

// RecordSuccessorDenial retains a definitive host authorization denial.
func (s *Store[EntityID, StateID]) RecordSuccessorDenial(
	ctx context.Context, binding transition.SuccessorBinding[EntityID, StateID], principal string,
) error {
	return s.recordSuccessorDenial(ctx, nil, binding, principal)
}

func (s *Store[EntityID, StateID]) recordSuccessorDenial(
	ctx context.Context, shared *sql.Tx, binding transition.SuccessorBinding[EntityID, StateID], principal string,
) error {
	if err := validateSuccessorBinding(binding); err != nil {
		return err
	}
	if principal == "" {
		return transition.ErrInvalidRequest
	}
	work := func(tx *sql.Tx) error {
		digest := continuationrecord.Digest(binding.Origin.ExecutionID)
		var entity, data []byte
		var phase string
		if err := tx.QueryRowContext(ctx, `SELECT entity, phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&entity, &phase, &data); err != nil {
			return err
		}
		stored, err := s.decodeSuccessor(digest[:], entity, data)
		if err != nil {
			return err
		}
		if err := transition.MatchContinuationMarkers(stored.Origin, binding.Origin); err != nil {
			return err
		}
		if !bytes.Equal(stored.OriginRevision, binding.OriginRevision) {
			return transition.ErrConflict
		}
		encoded, err := s.encodeRecord(binding.Execution, entity)
		if err != nil {
			return err
		}
		var record successorRecord
		if err := json.Unmarshal(data, &record); err != nil || !bytes.Equal(record.Execution, encoded) {
			return transition.ErrConflict
		}
		if phase != "prepared" && phase != "denied" {
			return transition.ErrConflict
		}
		for _, prior := range record.Denials {
			if bytes.Equal(prior, []byte(principal)) {
				return nil
			}
		}
		record.Denials = append(record.Denials, []byte(principal))
		data, err = json.Marshal(record)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE rff_successors SET phase = 'denied', record = ? WHERE digest = ?`, data, digest[:])
		return err
	}
	if shared != nil {
		return work(shared)
	}
	return s.hookTransaction(ctx, work)
}

// CancelSuccessor releases a provisional reservation only after the caller
// has verified the host's durable cancellation tombstone. An unknown host
// outcome is not a cancellation.
func (s *Store[EntityID, StateID]) CancelSuccessor(
	ctx context.Context, cancellation transition.SuccessorCancellation[EntityID, StateID],
) error {
	return s.cancelSuccessor(ctx, nil, cancellation)
}

func (s *Store[EntityID, StateID]) cancelSuccessor(
	ctx context.Context, shared *sql.Tx, cancellation transition.SuccessorCancellation[EntityID, StateID],
) error {
	binding := cancellation.Binding
	if err := validateSuccessorBinding(binding); err != nil {
		return err
	}
	if len(cancellation.Evidence) == 0 {
		return transition.ErrInvalidRequest
	}
	work := func(tx *sql.Tx) error {
		digest := continuationrecord.Digest(binding.Origin.ExecutionID)
		var entity, data []byte
		var phase string
		if err := tx.QueryRowContext(ctx, `SELECT entity, phase, record FROM rff_successors WHERE digest = ?`, digest[:]).Scan(&entity, &phase, &data); err != nil {
			return err
		}
		stored, err := s.decodeSuccessor(digest[:], entity, data)
		if err != nil {
			return err
		}
		if err := transition.MatchContinuationMarkers(stored.Origin, binding.Origin); err != nil {
			return err
		}
		if !bytes.Equal(stored.OriginRevision, binding.OriginRevision) {
			return transition.ErrConflict
		}
		encoded, err := s.encodeRecord(binding.Execution, entity)
		if err != nil {
			return err
		}
		var record successorRecord
		if err := json.Unmarshal(data, &record); err != nil || !bytes.Equal(record.Execution, encoded) {
			return transition.ErrConflict
		}
		wantPhase := "cancelled"
		if cancellation.Superseded {
			wantPhase = "superseded"
		}
		if phase == wantPhase {
			if !bytes.Equal(record.Cancellation, cancellation.Evidence) {
				return transition.ErrConflict
			}
			return nil
		}
		if phase != "prepared" && phase != "denied" {
			return transition.ErrConflict
		}
		record.Cancellation = bytes.Clone(cancellation.Evidence)
		data, err = json.Marshal(record)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE rff_successors SET phase = ?, record = ? WHERE digest = ?`, wantPhase, data, digest[:])
		return err
	}
	if shared != nil {
		return work(shared)
	}
	return s.hookTransaction(ctx, work)
}
