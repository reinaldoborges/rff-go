// Package continuationrecord implements the common durable marker encoding.
package continuationrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type record struct {
	SchemaVersion  uint
	ExecutionID    []byte
	OperationID    []byte
	Entity         []byte
	Target         []byte
	MachineID      []byte
	MachineVersion []byte
	Policy         fsm.Policy
}

func Digest(executionID string) [32]byte { return sha256.Sum256([]byte(executionID)) }

func Encode[E comparable, S comparable](marker transition.ContinuationMarker[E, S], entity, target []byte) ([]byte, error) {
	if err := transition.ValidateContinuationMarker(marker); err != nil {
		return nil, err
	}
	return json.Marshal(record{
		SchemaVersion: marker.SchemaVersion, ExecutionID: []byte(marker.ExecutionID), OperationID: []byte(marker.OperationID),
		Entity: entity, Target: target, MachineID: []byte(marker.MachineID), MachineVersion: []byte(marker.MachineVersion), Policy: marker.Policy,
	})
}

func Decode[E comparable, S comparable](
	data []byte, entity func([]byte) (E, error), state func([]byte) (S, error),
	encodeEntity func(E) ([]byte, error), encodeState func(S) ([]byte, error),
) (transition.ContinuationMarker[E, S], error) {
	var out transition.ContinuationMarker[E, S]
	var stored record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return out, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return out, errors.New("trailing continuation record data")
	}
	e, err := entity(bytes.Clone(stored.Entity))
	if err != nil {
		return out, err
	}
	target, err := state(bytes.Clone(stored.Target))
	if err != nil {
		return out, err
	}
	entityBytes, err := encodeEntity(e)
	if err != nil || !bytes.Equal(entityBytes, stored.Entity) {
		return out, errors.New("noncanonical continuation entity")
	}
	targetBytes, err := encodeState(target)
	if err != nil || !bytes.Equal(targetBytes, stored.Target) {
		return out, errors.New("noncanonical continuation target")
	}
	out = transition.ContinuationMarker[E, S]{
		SchemaVersion: stored.SchemaVersion, ExecutionID: string(stored.ExecutionID), OperationID: string(stored.OperationID),
		Entity: e, Target: target, MachineID: string(stored.MachineID), MachineVersion: string(stored.MachineVersion), Policy: stored.Policy,
	}
	if err := transition.ValidateContinuationMarker(out); err != nil {
		return transition.ContinuationMarker[E, S]{}, err
	}
	return out, nil
}
