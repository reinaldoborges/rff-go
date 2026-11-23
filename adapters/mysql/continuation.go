package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/reinaldoborges/rff-go/internal/continuationrecord"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func (s *Store[EntityID, StateID]) decodeContinuation(digest, data []byte) (transition.ContinuationMarker[EntityID, StateID], error) {
	marker, err := continuationrecord.Decode(data, s.entityCodec.Decode, s.stateCodec.Decode, s.encodeEntity, s.encodeState)
	if err != nil {
		return marker, fmt.Errorf("%w: invalid continuation marker: %w", ErrUnsupportedSchema, err)
	}
	expected := continuationrecord.Digest(marker.ExecutionID)
	if !bytes.Equal(digest, expected[:]) {
		return transition.ContinuationMarker[EntityID, StateID]{}, fmt.Errorf("%w: continuation identity index mismatch", ErrUnsupportedSchema)
	}
	return marker, nil
}

// RecordContinuationMarker retains a marker independently of reservations.
// Recording does not certify that the origin has been reconciled or released.
func (s *Store[EntityID, StateID]) RecordContinuationMarker(ctx context.Context, marker transition.ContinuationMarker[EntityID, StateID]) error {
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		return s.recordContinuationMarker(ctx, tx, marker)
	})
}

func (s *Store[EntityID, StateID]) recordContinuationMarker(ctx context.Context, tx *sql.Tx, marker transition.ContinuationMarker[EntityID, StateID]) error {
	if err := s.validateContinuationOrigin(ctx, tx, marker); err != nil {
		return err
	}
	entity, err := s.encodeEntity(marker.Entity)
	if err != nil {
		return err
	}
	target, err := s.encodeState(marker.Target)
	if err != nil {
		return err
	}
	data, err := continuationrecord.Encode(marker, entity, target)
	if err != nil {
		return err
	}
	digest := continuationrecord.Digest(marker.ExecutionID)
	var existing []byte
	err = tx.QueryRowContext(ctx, `SELECT record FROM rff_continuations WHERE digest = ?`, digest[:]).Scan(&existing)
	if err == nil {
		prior, err := s.decodeContinuation(digest[:], existing)
		if err != nil {
			return err
		}
		return transition.MatchContinuationMarkers(prior, marker)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO rff_continuations (digest, record) VALUES (?, ?)`, digest[:], data)
	return err
}

func (s *Store[EntityID, StateID]) validateContinuationOrigin(ctx context.Context, tx *sql.Tx, marker transition.ContinuationMarker[EntityID, StateID]) error {
	var data []byte
	err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, marker.ExecutionID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	execution, err := s.decodeRecord(data)
	if err != nil {
		return err
	}
	if execution.Continuation == nil || execution.CommitReceipt != nil && execution.CommitReceipt.Outcome != transition.CommitSucceeded {
		return transition.ErrConflict
	}
	operation := execution.CommitOperationID
	if operation == "" {
		operation = marker.OperationID
	}
	expected := transition.ContinuationMarker[EntityID, StateID]{
		SchemaVersion: 1, ExecutionID: execution.ID, OperationID: operation, Entity: execution.Entity, Target: execution.Target,
		MachineID: execution.Continuation.MachineID, MachineVersion: execution.Continuation.MachineVersion, Policy: execution.Continuation.Policy,
	}
	return transition.MatchContinuationMarkers(expected, marker)
}

func (s *Store[EntityID, StateID]) requireContinuationMarker(ctx context.Context, tx *sql.Tx, stored diskRecord) error {
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	execution, err := s.decodeRecord(data)
	if err != nil {
		return err
	}
	digest := continuationrecord.Digest(execution.ID)
	var markerData []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_continuations WHERE digest = ?`, digest[:]).Scan(&markerData); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrContinuationPending
		}
		return err
	}
	marker, err := s.decodeContinuation(digest[:], markerData)
	if err != nil {
		return err
	}
	expected := transition.ContinuationMarker[EntityID, StateID]{
		SchemaVersion: 1, ExecutionID: execution.ID, OperationID: execution.CommitOperationID,
		Entity: execution.Entity, Target: execution.Target, MachineID: execution.Continuation.MachineID,
		MachineVersion: execution.Continuation.MachineVersion, Policy: execution.Continuation.Policy,
	}
	return transition.MatchContinuationMarkers(expected, marker)
}

// ContinuationMarkers inventories durable markers; it does not identify
// markers eligible for evaluation.
func (s *Store[EntityID, StateID]) ContinuationMarkers(ctx context.Context) ([]transition.ContinuationMarker[EntityID, StateID], error) {
	rows, err := s.db.QueryContext(ctx, `SELECT digest, record FROM rff_continuations ORDER BY digest`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var markers []transition.ContinuationMarker[EntityID, StateID]
	for rows.Next() {
		var digest, data []byte
		if err := rows.Scan(&digest, &data); err != nil {
			return nil, err
		}
		marker, err := s.decodeContinuation(digest, data)
		if err != nil {
			return nil, err
		}
		markers = append(markers, marker)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return markers, nil
}

// ContinuationReady reports whether the matching successful origin receipt and
// reservation release are durable. It does not start or claim an evaluation.
func (s *Store[EntityID, StateID]) ContinuationReady(ctx context.Context, executionID string) (bool, error) {
	if executionID == "" {
		return false, transition.ErrInvalidRequest
	}
	var ready bool
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		digest := continuationrecord.Digest(executionID)
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_continuations WHERE digest = ?`, digest[:]).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrContinuationPending
		}
		if err != nil {
			return err
		}
		marker, err := s.decodeContinuation(digest[:], data)
		if err != nil {
			return err
		}
		if marker.ExecutionID != executionID {
			return transition.ErrConflict
		}
		if err := s.validateContinuationOrigin(ctx, tx, marker); err != nil {
			return err
		}
		err = tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, executionID).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		execution, err := s.decodeRecord(data)
		if err != nil {
			return err
		}
		if execution.CommitReceipt == nil {
			return nil
		}
		if execution.CommitReceipt.ExecutionID != executionID || execution.CommitReceipt.OperationID != marker.OperationID ||
			execution.CommitReceipt.Outcome != transition.CommitSucceeded || execution.CommitReceipt.Failure != nil {
			return transition.ErrConflict
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE execution_id = ?)`, executionID).Scan(&active); err != nil {
			return err
		}
		if active {
			return nil
		}
		if execution.Status != transition.Complete {
			return fmt.Errorf("%w: continuation origin reservation is inconsistent", ErrUnsupportedSchema)
		}
		ready = true
		return nil
	})
	return ready, err
}
