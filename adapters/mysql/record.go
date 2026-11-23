package mysql

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/executionidentity"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type diskRecord struct {
	SchemaVersion             uint
	ID                        string
	Entity                    []byte
	Key                       string
	Operation                 transition.Operation
	MachineID                 string
	MachineVersion            string
	Previous                  []byte
	HasPrevious               bool
	Target                    []byte
	Event                     transition.EncodedValue
	Input                     transition.EncodedValue
	NotificationOverride      transition.NotificationOverride
	Notify                    bool
	Policy                    fsm.Policy
	Continuation              *transition.ContinuationSnapshot
	AcceptedAt                time.Time
	Deadline                  time.Time
	CycleDeadline             time.Time
	Controls                  []transition.HookControlReceipt
	Status                    transition.Status
	CommitOperationID         string
	CommitReceipt             *transition.CommitReceipt
	NotificationIntentID      string
	NotificationFailure       *fsm.Failure
	NotificationWaived        bool
	NotificationControls      []transition.NotificationControlReceipt
	Compatibility             *transition.CompatibilityDiagnostic
	OriginalRequest           *transition.OriginalRequest
	Migrations                []transition.MigrationReceipt
	RecoveryStarted           bool
	RejectionRecoveryDeadline time.Time
	Failure                   *fsm.Failure
	FailureDisposition        fsm.Disposition
}

type diskRecordAlias diskRecord

func (r diskRecord) MarshalJSON() ([]byte, error) {
	if r.SchemaVersion == 1 {
		return json.Marshal(diskRecordAlias(r))
	}
	fields := executionidentity.Fields{
		ID: []byte(r.ID), Key: []byte(r.Key), MachineID: []byte(r.MachineID), MachineVersion: []byte(r.MachineVersion),
		CommitOperationID: []byte(r.CommitOperationID),
		EventCodecName:    []byte(r.Event.Codec.Name), EventCodecVersion: []byte(r.Event.Codec.Version),
		InputCodecName: []byte(r.Input.Codec.Name), InputCodecVersion: []byte(r.Input.Codec.Version),
	}
	if r.Continuation != nil {
		fields.ContinuationMachineID = []byte(r.Continuation.MachineID)
		fields.ContinuationMachineVersion = []byte(r.Continuation.MachineVersion)
	}
	if r.OriginalRequest != nil {
		fields.OriginalMachineID, fields.OriginalMachineVersion = []byte(r.OriginalRequest.MachineID), []byte(r.OriginalRequest.MachineVersion)
		fields.OriginalEventCodecName, fields.OriginalEventCodecVersion = []byte(r.OriginalRequest.Event.Codec.Name), []byte(r.OriginalRequest.Event.Codec.Version)
		fields.OriginalInputCodecName, fields.OriginalInputCodecVersion = []byte(r.OriginalRequest.Input.Codec.Name), []byte(r.OriginalRequest.Input.Codec.Version)
	}
	if r.CommitReceipt != nil {
		fields.ReceiptExecutionID, fields.ReceiptOperationID = []byte(r.CommitReceipt.ExecutionID), []byte(r.CommitReceipt.OperationID)
	}
	return json.Marshal(struct {
		diskRecordAlias
		Identity executionidentity.Fields
	}{diskRecordAlias: diskRecordAlias(r), Identity: fields})
}

func (r *diskRecord) UnmarshalJSON(data []byte) error {
	var envelope struct {
		diskRecordAlias
		Identity *executionidentity.Fields
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	*r = diskRecord(envelope.diskRecordAlias)
	if r.SchemaVersion == 1 {
		if r.Continuation != nil {
			return fmt.Errorf("continuation in legacy execution record")
		}
		return nil
	}
	if !transition.SupportedExecutionSchema(r.SchemaVersion) || envelope.Identity == nil {
		return fmt.Errorf("missing execution identity")
	}
	f := envelope.Identity
	if !executionidentity.Matches(r.ID, f.ID) || !executionidentity.Matches(r.Key, f.Key) ||
		!executionidentity.Matches(r.MachineID, f.MachineID) || !executionidentity.Matches(r.MachineVersion, f.MachineVersion) ||
		!executionidentity.Matches(r.CommitOperationID, f.CommitOperationID) ||
		!executionidentity.Matches(r.Event.Codec.Name, f.EventCodecName) || !executionidentity.Matches(r.Event.Codec.Version, f.EventCodecVersion) ||
		!executionidentity.Matches(r.Input.Codec.Name, f.InputCodecName) || !executionidentity.Matches(r.Input.Codec.Version, f.InputCodecVersion) ||
		r.Continuation != nil && (!executionidentity.Matches(r.Continuation.MachineID, f.ContinuationMachineID) ||
			!executionidentity.Matches(r.Continuation.MachineVersion, f.ContinuationMachineVersion)) ||
		r.OriginalRequest != nil && (!executionidentity.Matches(r.OriginalRequest.MachineID, f.OriginalMachineID) ||
			!executionidentity.Matches(r.OriginalRequest.MachineVersion, f.OriginalMachineVersion) ||
			!executionidentity.Matches(r.OriginalRequest.Event.Codec.Name, f.OriginalEventCodecName) ||
			!executionidentity.Matches(r.OriginalRequest.Event.Codec.Version, f.OriginalEventCodecVersion) ||
			!executionidentity.Matches(r.OriginalRequest.Input.Codec.Name, f.OriginalInputCodecName) ||
			!executionidentity.Matches(r.OriginalRequest.Input.Codec.Version, f.OriginalInputCodecVersion)) ||
		r.CommitReceipt != nil && (!executionidentity.Matches(r.CommitReceipt.ExecutionID, f.ReceiptExecutionID) ||
			!executionidentity.Matches(r.CommitReceipt.OperationID, f.ReceiptOperationID)) {
		return fmt.Errorf("inconsistent execution identity")
	}
	r.ID, r.Key, r.MachineID, r.MachineVersion = string(f.ID), string(f.Key), string(f.MachineID), string(f.MachineVersion)
	r.CommitOperationID = string(f.CommitOperationID)
	r.Event.Codec.Name, r.Event.Codec.Version = string(f.EventCodecName), string(f.EventCodecVersion)
	r.Input.Codec.Name, r.Input.Codec.Version = string(f.InputCodecName), string(f.InputCodecVersion)
	if r.Continuation != nil {
		r.Continuation.MachineID, r.Continuation.MachineVersion = string(f.ContinuationMachineID), string(f.ContinuationMachineVersion)
	}
	if r.OriginalRequest != nil {
		r.OriginalRequest.MachineID, r.OriginalRequest.MachineVersion = string(f.OriginalMachineID), string(f.OriginalMachineVersion)
		r.OriginalRequest.Event.Codec.Name, r.OriginalRequest.Event.Codec.Version = string(f.OriginalEventCodecName), string(f.OriginalEventCodecVersion)
		r.OriginalRequest.Input.Codec.Name, r.OriginalRequest.Input.Codec.Version = string(f.OriginalInputCodecName), string(f.OriginalInputCodecVersion)
	}
	if r.CommitReceipt != nil {
		r.CommitReceipt.ExecutionID, r.CommitReceipt.OperationID = string(f.ReceiptExecutionID), string(f.ReceiptOperationID)
	}
	if err := transition.ValidateContinuationSnapshot(r.Continuation); err != nil {
		return fmt.Errorf("invalid continuation snapshot")
	}
	if r.Continuation != nil {
		id, version := r.MachineID, r.MachineVersion
		if r.OriginalRequest != nil {
			id, version = r.OriginalRequest.MachineID, r.OriginalRequest.MachineVersion
		}
		if r.Continuation.MachineID != id || r.Continuation.MachineVersion != version {
			return fmt.Errorf("continuation behavior differs from acceptance")
		}
	}
	return nil
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
		Notify: record.Notify, Policy: record.Policy, Continuation: record.Continuation, AcceptedAt: record.AcceptedAt,
		Deadline: record.Deadline, CycleDeadline: record.CycleDeadline, Controls: record.Controls,
		Status: record.Status, CommitOperationID: record.CommitOperationID, CommitReceipt: record.CommitReceipt, RecoveryStarted: record.RecoveryStarted,
		NotificationIntentID: record.NotificationIntentID,
		NotificationFailure:  record.NotificationFailure, NotificationWaived: record.NotificationWaived, NotificationControls: record.NotificationControls,
		Compatibility: record.Compatibility, OriginalRequest: record.OriginalRequest, Migrations: record.Migrations,
		RejectionRecoveryDeadline: record.RejectionRecoveryDeadline,
		Failure:                   record.Failure, FailureDisposition: record.FailureDisposition,
	})
}

func (s *Store[EntityID, StateID]) decodeRecord(data []byte) (transition.Execution[EntityID, StateID], error) {
	var record diskRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: decode record: %w", ErrUnsupportedSchema, err)
	}
	if !transition.SupportedExecutionSchema(record.SchemaVersion) || record.SchemaVersion == 1 && record.Continuation != nil {
		return transition.Execution[EntityID, StateID]{}, ErrUnsupportedSchema
	}
	if err := transition.ValidateContinuationSnapshot(record.Continuation); err != nil {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: invalid continuation snapshot", ErrUnsupportedSchema)
	}
	entity, err := s.entityCodec.Decode(record.Entity)
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, transition.NewCompatibilityError(transition.CompatibilityDiagnostic{
			Component: transition.CompatibilityEntity, Name: record.MachineID, Version: record.MachineVersion,
		}, err)
	}
	target, err := s.stateCodec.Decode(record.Target)
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, transition.NewCompatibilityError(transition.CompatibilityDiagnostic{
			Component: transition.CompatibilityState, Name: record.MachineID, Version: record.MachineVersion,
		}, err)
	}
	var previous StateID
	if record.HasPrevious {
		previous, err = s.stateCodec.Decode(record.Previous)
		if err != nil {
			return transition.Execution[EntityID, StateID]{}, transition.NewCompatibilityError(transition.CompatibilityDiagnostic{
				Component: transition.CompatibilityState, Name: record.MachineID, Version: record.MachineVersion,
			}, err)
		}
	}
	return transition.Execution[EntityID, StateID]{
		SchemaVersion: record.SchemaVersion, ID: record.ID, Entity: entity, Key: record.Key,
		Operation: record.Operation, MachineID: record.MachineID, MachineVersion: record.MachineVersion,
		Previous: previous, HasPrevious: record.HasPrevious, Target: target,
		Event: record.Event, Input: record.Input, NotificationOverride: record.NotificationOverride,
		Notify: record.Notify, Policy: record.Policy, Continuation: record.Continuation, AcceptedAt: record.AcceptedAt,
		Deadline: record.Deadline, CycleDeadline: record.CycleDeadline, Controls: record.Controls,
		Status: record.Status, CommitOperationID: record.CommitOperationID, CommitReceipt: record.CommitReceipt, RecoveryStarted: record.RecoveryStarted,
		NotificationIntentID: record.NotificationIntentID,
		NotificationFailure:  record.NotificationFailure, NotificationWaived: record.NotificationWaived, NotificationControls: record.NotificationControls,
		Compatibility: record.Compatibility, OriginalRequest: record.OriginalRequest, Migrations: record.Migrations,
		RejectionRecoveryDeadline: record.RejectionRecoveryDeadline,
		Failure:                   record.Failure, FailureDisposition: record.FailureDisposition,
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
