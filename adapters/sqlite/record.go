package sqlite

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type diskRecord struct {
	SchemaVersion        uint
	ID                   string
	Entity               []byte
	Key                  string
	Operation            transition.Operation
	MachineID            string
	MachineVersion       string
	Previous             []byte
	HasPrevious          bool
	Target               []byte
	Event                transition.EncodedValue
	Input                transition.EncodedValue
	NotificationOverride transition.NotificationOverride
	Notify               bool
	Policy               fsm.Policy
	AcceptedAt           time.Time
	Deadline             time.Time
	CycleDeadline        time.Time
	Controls             []transition.HookControlReceipt
	Status               transition.Status
	CommitOperationID    string
	RecoveryStarted      bool
	Failure              *fsm.Failure
	FailureDisposition   fsm.Disposition
}

func (s *Store[EntityID, StateID]) encodeRecord(record transition.Execution[EntityID, StateID], entity []byte) ([]byte, error) {
	target, err := s.encodeState(record.Target)
	if err != nil {
		return nil, fmt.Errorf("encode target: %w", err)
	}
	var previous []byte
	if record.HasPrevious {
		previous, err = s.encodeState(record.Previous)
		if err != nil {
			return nil, fmt.Errorf("encode source: %w", err)
		}
	}
	return json.Marshal(diskRecord{
		SchemaVersion: record.SchemaVersion, ID: record.ID, Entity: entity, Key: record.Key,
		Operation: record.Operation, MachineID: record.MachineID, MachineVersion: record.MachineVersion,
		Previous: previous, HasPrevious: record.HasPrevious, Target: target,
		Event: record.Event, Input: record.Input, NotificationOverride: record.NotificationOverride,
		Notify: record.Notify, Policy: record.Policy, AcceptedAt: record.AcceptedAt,
		Deadline: record.Deadline, CycleDeadline: record.CycleDeadline, Controls: record.Controls,
		Status: record.Status, CommitOperationID: record.CommitOperationID, RecoveryStarted: record.RecoveryStarted,
		Failure: record.Failure, FailureDisposition: record.FailureDisposition,
	})
}

func (s *Store[EntityID, StateID]) decodeRecord(data []byte) (transition.Execution[EntityID, StateID], error) {
	var record diskRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: decode record: %w", ErrUnsupportedSchema, err)
	}
	if record.SchemaVersion != transition.CurrentSchemaVersion {
		return transition.Execution[EntityID, StateID]{}, ErrUnsupportedSchema
	}
	entity, err := s.entityCodec.Decode(record.Entity)
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: decode entity: %w", transition.ErrCompatibility, err)
	}
	target, err := s.stateCodec.Decode(record.Target)
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: decode target: %w", transition.ErrCompatibility, err)
	}
	var previous StateID
	if record.HasPrevious {
		previous, err = s.stateCodec.Decode(record.Previous)
		if err != nil {
			return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: decode source: %w", transition.ErrCompatibility, err)
		}
	}
	return transition.Execution[EntityID, StateID]{
		SchemaVersion: record.SchemaVersion, ID: record.ID, Entity: entity, Key: record.Key,
		Operation: record.Operation, MachineID: record.MachineID, MachineVersion: record.MachineVersion,
		Previous: previous, HasPrevious: record.HasPrevious, Target: target,
		Event: record.Event, Input: record.Input, NotificationOverride: record.NotificationOverride,
		Notify: record.Notify, Policy: record.Policy, AcceptedAt: record.AcceptedAt,
		Deadline: record.Deadline, CycleDeadline: record.CycleDeadline, Controls: record.Controls,
		Status: record.Status, CommitOperationID: record.CommitOperationID, RecoveryStarted: record.RecoveryStarted,
		Failure: record.Failure, FailureDisposition: record.FailureDisposition,
	}, nil
}

func (r diskRecord) effectiveDeadline() time.Time {
	if !r.CycleDeadline.IsZero() {
		return r.CycleDeadline
	}
	return r.Deadline
}

func (s *Store[EntityID, StateID]) encodeState(value StateID) ([]byte, error) {
	data, err := s.stateCodec.Encode(value)
	if err != nil {
		return nil, err
	}
	data = slices.Clone(data)
	roundTrip, err := s.stateCodec.Decode(slices.Clone(data))
	if err != nil {
		return nil, err
	}
	if roundTrip != value {
		return nil, fmt.Errorf("state codec does not round-trip")
	}
	return data, nil
}
