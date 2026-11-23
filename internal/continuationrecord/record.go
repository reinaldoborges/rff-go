// Package continuationrecord implements the common durable marker encoding.
package continuationrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"time"

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
	Progress       *transition.ContinuationProgress `json:",omitempty"`
}

func Digest(executionID string) [32]byte { return sha256.Sum256([]byte(executionID)) }

// Progress and SetProgress retain the marker and its driver state in the
// same transactionally updated record.
func Progress(data []byte) (*transition.ContinuationProgress, error) {
	var stored record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing continuation record data")
	}
	if stored.Progress != nil {
		p := stored.Progress
		switch p.Phase {
		case transition.ContinuationWaiting, transition.ContinuationDriving, transition.ContinuationDecided,
			transition.ContinuationStopped, transition.ContinuationIntervention, transition.ContinuationAccepting,
			transition.ContinuationWaiverPending, transition.ContinuationWaived, transition.ContinuationAccepted,
			transition.ContinuationSuperseded:
		default:
			return nil, errors.New("unsupported continuation phase")
		}
		if p.Version != 1 || p.Attempts > stored.Policy.MaxAttempts || p.AttemptPaused < 0 ||
			p.CycleStarted.IsZero() != p.Deadline.IsZero() ||
			(!p.CycleStarted.IsZero() && !p.Deadline.After(p.CycleStarted)) {
			return nil, errors.New("invalid continuation progress")
		}
		if p.Phase == transition.ContinuationDriving || p.Phase == transition.ContinuationDecided || p.Phase == transition.ContinuationAccepting {
			if p.Attempts == 0 || p.StartedAt.IsZero() || p.StartedGeneration == 0 || p.CycleStarted.IsZero() {
				return nil, errors.New("invalid started continuation")
			}
		}
		if p.Phase == transition.ContinuationDecided && (p.Decision == nil || !p.Decision.Continue) ||
			p.Phase == transition.ContinuationStopped && (p.Decision == nil || p.Decision.Continue) ||
			p.Phase == transition.ContinuationAccepting && (p.Decision == nil || !p.Decision.Continue || p.PermitExpires.IsZero() || len(p.SuccessorID) == 0) {
			return nil, errors.New("invalid continuation decision")
		}
		if !p.PausedAt.IsZero() && (p.StartedAt.IsZero() || p.PausedAt.Before(p.StartedAt)) {
			return nil, errors.New("invalid continuation pause")
		}
	}
	return stored.Progress, nil
}

func SetProgress(data []byte, progress *transition.ContinuationProgress) ([]byte, error) {
	if _, err := Progress(data); err != nil {
		return nil, err
	}
	var stored record
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, err
	}
	stored.Progress = progress
	return json.Marshal(stored)
}

// StartCycle records the instant the origin reservation is released, so
// queueing and downtime before the first worker claim consume the budget.
func StartCycle(data []byte, now time.Time) ([]byte, error) {
	progress, err := Progress(data)
	if err != nil {
		return nil, err
	}
	if now.IsZero() {
		return nil, transition.ErrInvalidLease
	}
	if progress == nil {
		progress = &transition.ContinuationProgress{Version: 1, Phase: transition.ContinuationWaiting}
	}
	if progress.Phase != transition.ContinuationWaiting || !progress.CycleStarted.IsZero() {
		return data, nil
	}
	var stored record
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, err
	}
	progress.CycleStarted = now
	progress.Deadline = now.Add(stored.Policy.OverallTimeout)
	return SetProgress(data, progress)
}

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
