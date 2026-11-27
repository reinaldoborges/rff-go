package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// ErrSuccessorCancelled reports a durably fenced cancellation.
var ErrSuccessorCancelled = errors.New("successor request was fenced and cancelled")

// ErrSuccessorSuperseded reports a provably unaccepted request whose origin advanced.
var ErrSuccessorSuperseded = errors.New("successor origin was superseded")

// ErrSuccessorReplayDenied does not disclose host authorization details for
// replay or inspection.
var ErrSuccessorReplayDenied = errors.New("successor replay was not authorized")

// SuccessorRequest binds a successor request to its committed origin and to
// the host's non-reused revision for that commit.
type SuccessorRequest[EntityID comparable, StateID comparable, Event, Input any] struct {
	Origin         ContinuationMarker[EntityID, StateID]
	OriginRevision []byte
	Request        Request[EntityID, StateID, Event, Input]
}

// SuccessorBinding is the immutable evidence retained with a successor.
type SuccessorBinding[EntityID comparable, StateID comparable] struct {
	Origin         ContinuationMarker[EntityID, StateID]
	OriginRevision []byte
	Execution      Execution[EntityID, StateID]
}

// ValidateSuccessorBinding rejects incomplete or already-progressed snapshots.
func ValidateSuccessorBinding[EntityID comparable, StateID comparable](binding SuccessorBinding[EntityID, StateID]) error {
	e, m := binding.Execution, binding.Origin
	if err := ValidateContinuationMarker(m); err != nil {
		return err
	}
	if err := ValidateDurableID(e.ID); err != nil {
		return err
	}
	if len(binding.OriginRevision) == 0 || e.SchemaVersion != CurrentSchemaVersion ||
		e.Key == "" || e.Entity != m.Entity || e.Operation != Transition ||
		!e.HasPrevious || e.Previous != m.Target || e.Status != Waiting ||
		e.MachineID == "" || e.MachineVersion == "" ||
		e.Event.Codec.Name == "" || e.Event.Codec.Version == "" ||
		e.Input.Codec.Name == "" || e.Input.Codec.Version == "" ||
		(e.NotificationOverride != NotifyInherit && e.NotificationOverride != NotifyEnable && e.NotificationOverride != NotifyDisable) ||
		e.AcceptedAt.IsZero() || !e.Deadline.Equal(e.AcceptedAt.Add(e.Policy.OverallTimeout)) ||
		e.CommitOperationID != "" || e.CommitReceipt != nil || e.NotificationIntentID != "" ||
		e.NotificationFailure != nil || e.NotificationWaived || len(e.NotificationControls) != 0 ||
		e.Compatibility != nil || e.OriginalRequest != nil || len(e.Migrations) != 0 ||
		e.RecoveryStarted || e.Failure != nil || e.FailureDisposition != "" ||
		!e.CycleDeadline.IsZero() || !e.RejectionRecoveryDeadline.IsZero() || len(e.Controls) != 0 {
		return ErrInvalidDecision
	}
	if err := validatePolicy(e.Policy); err != nil {
		return ErrInvalidDecision
	}
	if err := ValidateContinuationSnapshot(e.Continuation); err != nil ||
		e.Continuation != nil && (e.Continuation.MachineID != e.MachineID || e.Continuation.MachineVersion != e.MachineVersion) {
		return ErrInvalidDecision
	}
	return nil
}

// SuccessorReceipt is durable host evidence that origin validation,
// authorization and the host fence were committed together.
type SuccessorReceipt[EntityID comparable, StateID comparable] struct {
	Binding    SuccessorBinding[EntityID, StateID]
	Principal  string
	Decision   string
	AcceptedAt time.Time
}

// SuccessorCancellation is authoritative host evidence of a durable
// tombstone preventing this exact prepared request from being accepted.
type SuccessorCancellation[EntityID comparable, StateID comparable] struct {
	Binding    SuccessorBinding[EntityID, StateID]
	Evidence   []byte
	Superseded bool
}

type SuccessorPhase string

const (
	SuccessorPrepared   SuccessorPhase = "prepared"
	SuccessorAccepted   SuccessorPhase = "accepted"
	SuccessorCancelled  SuccessorPhase = "cancelled"
	SuccessorDenied     SuccessorPhase = "denied"
	SuccessorSuperseded SuccessorPhase = "superseded"
)

// SuccessorInspection contains sensitive host-defined values. The host must
// authorize any external disclosure; internal recovery is independent.
type SuccessorInspection[EntityID comparable, StateID comparable] struct {
	Binding      SuccessorBinding[EntityID, StateID]
	Phase        SuccessorPhase
	Principal    string
	Decision     string
	Cancellation []byte
	Denials      []string
}

// SuccessorPreparationStore retains a provisional binding and reservation.
// The callback runs in the store transaction and must not re-enter the store.
// It returns nil on a matching replay and a binding only for a new prepare.
type SuccessorPreparationStore[EntityID comparable, StateID comparable] interface {
	PrepareSuccessor(context.Context, ContinuationMarker[EntityID, StateID],
		func(*SuccessorBinding[EntityID, StateID]) (*SuccessorBinding[EntityID, StateID], error),
	) (SuccessorBinding[EntityID, StateID], bool, error)
}

type successorReplayStore[EntityID comparable, StateID comparable] interface {
	InspectSuccessor(context.Context, ContinuationMarker[EntityID, StateID]) (SuccessorInspection[EntityID, StateID], error)
	ReadSuccessorExecution(context.Context, ContinuationMarker[EntityID, StateID]) (Execution[EntityID, StateID], error)
}

// InspectSuccessor authorizes external disclosure before reading protocol data.
func (a *Acceptor[EntityID, StateID, Event, Input]) InspectSuccessor(
	ctx context.Context,
	marker ContinuationMarker[EntityID, StateID],
	authorize func(context.Context, ContinuationMarker[EntityID, StateID]) error,
) (SuccessorInspection[EntityID, StateID], error) {
	var zero SuccessorInspection[EntityID, StateID]
	if authorize == nil {
		return zero, ErrInvalidRequest
	}
	if err := authorize(ctx, marker); err != nil {
		return zero, ErrSuccessorReplayDenied
	}
	store, ok := a.store.(interface {
		InspectSuccessor(context.Context, ContinuationMarker[EntityID, StateID]) (SuccessorInspection[EntityID, StateID], error)
	})
	if !ok {
		return zero, ErrContinuationUnavailable
	}
	return store.InspectSuccessor(ctx, marker)
}

// ReplaySuccessor authorizes disclosure separately from internal recovery.
func (a *Acceptor[EntityID, StateID, Event, Input]) ReplaySuccessor(
	ctx context.Context,
	successor SuccessorRequest[EntityID, StateID, Event, Input],
	authorize func(context.Context, SuccessorRequest[EntityID, StateID, Event, Input]) error,
) (Execution[EntityID, StateID], error) {
	var zero Execution[EntityID, StateID]
	if authorize == nil {
		return zero, ErrInvalidRequest
	}
	if err := authorize(ctx, successor); err != nil {
		return zero, ErrSuccessorReplayDenied
	}
	store, ok := a.store.(successorReplayStore[EntityID, StateID])
	if !ok {
		return zero, ErrContinuationUnavailable
	}
	inspection, err := store.InspectSuccessor(ctx, successor.Origin)
	if err != nil {
		return zero, err
	}
	if err := a.MatchSuccessor(ctx, inspection.Binding, successor); err != nil {
		return zero, err
	}
	if inspection.Phase != SuccessorAccepted {
		return zero, ErrSuccessorPending
	}
	return store.ReadSuccessorExecution(ctx, successor.Origin)
}

// PrepareSuccessor selects and durably reserves the successor without
// accepting it. It must later be reconciled with the authoritative host.
func (a *Acceptor[EntityID, StateID, Event, Input]) PrepareSuccessor(
	ctx context.Context, successor SuccessorRequest[EntityID, StateID, Event, Input],
) (SuccessorBinding[EntityID, StateID], bool, error) {
	var zero SuccessorBinding[EntityID, StateID]
	decision, err := a.SuccessorDecision(ctx, successor)
	if err != nil {
		return zero, false, err
	}
	store, ok := a.store.(SuccessorPreparationStore[EntityID, StateID])
	if !ok {
		return zero, false, fmt.Errorf("%w: successor preparation is unsupported", ErrContinuationUnavailable)
	}
	return store.PrepareSuccessor(ctx, successor.Origin, decision)
}

// SuccessorDecision constructs the selection and replay callback used by a
// store's transactional acceptance boundary. The request must not be mutated
// while the callback is in use.
func (a *Acceptor[EntityID, StateID, Event, Input]) SuccessorDecision(
	ctx context.Context, successor SuccessorRequest[EntityID, StateID, Event, Input],
) (func(*SuccessorBinding[EntityID, StateID]) (*SuccessorBinding[EntityID, StateID], error), error) {
	if err := ValidateContinuationMarker(successor.Origin); err != nil {
		return nil, err
	}
	request := successor.Request
	if err := validateRequest(request); err != nil {
		return nil, err
	}
	if len(successor.OriginRevision) == 0 || request.Operation != Transition ||
		request.Entity != successor.Origin.Entity || request.Previous != successor.Origin.Target {
		return nil, ErrInvalidRequest
	}
	successor.Origin.Policy.RetryDelays = slices.Clone(successor.Origin.Policy.RetryDelays)
	successor.OriginRevision = bytes.Clone(successor.OriginRevision)
	return func(existing *SuccessorBinding[EntityID, StateID]) (*SuccessorBinding[EntityID, StateID], error) {
		if existing != nil {
			return nil, a.MatchSuccessor(ctx, *existing, successor)
		}
		execution, err := a.prepare(ctx, request)
		if err != nil {
			return nil, err
		}
		origin := successor.Origin
		origin.Policy.RetryDelays = slices.Clone(origin.Policy.RetryDelays)
		return &SuccessorBinding[EntityID, StateID]{
			Origin: origin, OriginRevision: bytes.Clone(successor.OriginRevision), Execution: *execution,
		}, nil
	}, nil
}

// MatchSuccessor compares a replay with the pinned origin and semantic
// request. It does not disclose or establish acceptance.
func (a *Acceptor[EntityID, StateID, Event, Input]) MatchSuccessor(
	ctx context.Context,
	binding SuccessorBinding[EntityID, StateID],
	incoming SuccessorRequest[EntityID, StateID, Event, Input],
) error {
	if err := ValidateContinuationMarker(binding.Origin); err != nil {
		return err
	}
	if err := ValidateContinuationMarker(incoming.Origin); err != nil {
		return err
	}
	if len(binding.OriginRevision) == 0 || len(incoming.OriginRevision) == 0 {
		return ErrInvalidRequest
	}
	if err := MatchContinuationMarkers(binding.Origin, incoming.Origin); err != nil {
		return err
	}
	request := incoming.Request
	if !bytes.Equal(binding.OriginRevision, incoming.OriginRevision) ||
		binding.Execution.Entity != binding.Origin.Entity || request.Entity != binding.Origin.Entity ||
		binding.Execution.Key != request.Key || request.Operation != Transition ||
		!request.HasPrevious || request.Previous != binding.Origin.Target {
		return ErrConflict
	}
	if err := validateRequest(request); err != nil {
		return err
	}
	return a.match(ctx, binding.Execution, request)
}
