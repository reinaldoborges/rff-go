package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

type ContinuationDriveStore[E comparable, S comparable] interface {
	SuccessorRecoveryStore[E, S]
	AcquireContinuation(context.Context, ContinuationMarker[E, S], string, func() time.Time, time.Duration) (ContinuationLease, error)
	ReadContinuation(context.Context, string) (ContinuationProgress, error)
	StartContinuation(context.Context, ContinuationLease, func() time.Time) error
	SetContinuationCompatibility(context.Context, ContinuationLease, func() time.Time, bool) error
	RecordContinuationDecision(context.Context, ContinuationLease, func() time.Time, ContinuationDecision) error
	FailContinuationAttempt(context.Context, ContinuationLease, func() time.Time, fsm.Failure, fsm.Disposition) error
	RecoverContinuationAttempt(context.Context, ContinuationLease, func() time.Time) error
	ResumeContinuation(context.Context, ContinuationLease, func() time.Time) error
	ParkContinuation(context.Context, ContinuationLease, func() time.Time) error
	PrepareContinuationSuccessor(context.Context, ContinuationLease, func() time.Time, func(*SuccessorBinding[E, S]) (*SuccessorBinding[E, S], error)) (SuccessorBinding[E, S], bool, error)
	ReconcileContinuation(context.Context, ContinuationMarker[E, S], func() time.Time) error
	RecordContinuationDenial(context.Context, SuccessorBinding[E, S], string, ContinuationDeliveryPermit, func() time.Time) error
	ResolveContinuationAbsent(context.Context, ContinuationMarker[E, S], ContinuationDeliveryPermit, func() time.Time) error
}

// ContinuationDeliveryPermit limits a single started drive. A separate-store
// host must atomically reject expired permits and honor durable cancellation
// tombstones when accepting. Lookup must serialize with in-flight host
// acceptance transactions; an absent result is usable for retry only once all
// prior permits have expired.
type ContinuationDeliveryPermit struct {
	Generation uint64
	ExpiresAt  time.Time
}

type ContinuationDeliveryHost[E comparable, S comparable] interface {
	LookupSuccessor(context.Context, SuccessorBinding[E, S]) (SuccessorHostResult[E, S], error)
	AcceptContinuationSuccessor(context.Context, SuccessorBinding[E, S], ContinuationDeliveryPermit) (SuccessorHostResult[E, S], error)
	// Cancellation must durably tombstone the identity against every late
	// permit, or return the already accepted/superseded result.
	CancelContinuationSuccessor(context.Context, SuccessorBinding[E, S]) (SuccessorHostResult[E, S], error)
}

type ContinuationDriverConfig[E comparable, S comparable, Event, Input any] struct {
	Clock         func() time.Time
	LeaseDuration time.Duration
	EventCodec    CodecVersion
	InputCodec    CodecVersion
	ResolveEvent  CodecResolver[Event]
	ResolveInput  CodecResolver[Input]
	Acceptor      *Acceptor[E, S, Event, Input]
	Host          ContinuationDeliveryHost[E, S]
	// OriginRevision must return the durable revision of this exact commit,
	// not the entity's current revision.
	OriginRevision func(context.Context, ContinuationMarker[E, S]) ([]byte, error)
}

// ContinuationDriver evaluates the exact pinned behavior under a durable
// fenced claim. Host scheduling must continue polling durable markers.
type ContinuationDriver[E comparable, S comparable, Event, Input any] struct {
	store    ContinuationDriveStore[E, S]
	machines *fsm.Registry[E, S, Event, Input]
	config   ContinuationDriverConfig[E, S, Event, Input]
}

func NewContinuationDriver[E comparable, S comparable, Event, Input any](
	store ContinuationDriveStore[E, S], machines *fsm.Registry[E, S, Event, Input],
	config ContinuationDriverConfig[E, S, Event, Input],
) (*ContinuationDriver[E, S, Event, Input], error) {
	if nilcheck.IsNil(store) || machines == nil || config.Clock == nil || config.LeaseDuration <= 0 ||
		config.EventCodec.Name == "" || config.EventCodec.Version == "" || config.InputCodec.Name == "" || config.InputCodec.Version == "" ||
		config.ResolveEvent == nil || config.ResolveInput == nil {
		return nil, ErrInvalidRequest
	}
	if (config.Acceptor != nil || !nilcheck.IsNil(config.Host) || config.OriginRevision != nil) &&
		(config.Acceptor == nil || nilcheck.IsNil(config.Host) || config.OriginRevision == nil) {
		return nil, ErrInvalidRequest
	}
	return &ContinuationDriver[E, S, Event, Input]{store: store, machines: machines, config: config}, nil
}

func (d *ContinuationDriver[E, S, Event, Input]) Drive(ctx context.Context, marker ContinuationMarker[E, S], owner string) error {
	progress, err := d.store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil {
		return err
	}
	if progress.Phase == ContinuationAccepting {
		if nilcheck.IsNil(d.config.Host) {
			return ErrSuccessorPending
		}
		permit := ContinuationDeliveryPermit{Generation: progress.StartedGeneration, ExpiresAt: progress.PermitExpires}
		err := d.reconcile(ctx, marker, permit, false)
		if !errors.Is(err, ErrSuccessorPending) || progress.PermitExpires.IsZero() || d.config.Clock().Before(progress.PermitExpires) {
			return err
		}
		inspection, err := d.store.InspectSuccessor(ctx, marker)
		if err != nil {
			return err
		}
		result, err := d.config.Host.LookupSuccessor(ctx, inspection.Binding)
		if err != nil {
			return err
		}
		if result.Outcome == SuccessorHostAbsent {
			return d.store.ResolveContinuationAbsent(ctx, marker, ContinuationDeliveryPermit{
				Generation: progress.StartedGeneration, ExpiresAt: progress.PermitExpires,
			}, d.config.Clock)
		}
		return d.reconcile(ctx, marker, permit, false)
	}
	if progress.Phase == ContinuationWaiverPending {
		return d.reconcileWaiver(ctx, marker)
	}
	if progress.Phase == ContinuationIntervention {
		if len(progress.SuccessorID) != 0 && !nilcheck.IsNil(d.config.Host) {
			err := d.reconcile(ctx, marker, ContinuationDeliveryPermit{
				Generation: progress.StartedGeneration, ExpiresAt: progress.PermitExpires,
			}, false)
			if err == nil || errors.Is(err, ErrSuccessorSuperseded) {
				return err
			}
			return errors.Join(ErrContinuationIntervention, err)
		}
		return ErrContinuationIntervention
	}
	if progress.Phase == ContinuationAccepted || progress.Phase == ContinuationStopped || progress.Phase == ContinuationSuperseded || progress.Phase == ContinuationWaived {
		return nil
	}
	lease, err := d.store.AcquireContinuation(ctx, marker, owner, d.config.Clock, d.config.LeaseDuration)
	if err != nil {
		return err
	}
	progress, err = d.store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil {
		return err
	}
	if progress.Phase == ContinuationDriving || progress.Phase == ContinuationDecided {
		if !progress.PausedAt.IsZero() {
			if err := d.store.ResumeContinuation(ctx, lease, d.config.Clock); err != nil {
				return err
			}
			progress, err = d.store.ReadContinuation(ctx, marker.ExecutionID)
			if err != nil {
				return err
			}
		} else if progress.StartedGeneration != lease.Generation {
			return d.store.RecoverContinuationAttempt(ctx, lease, d.config.Clock)
		} else {
			now := d.config.Clock()
			if !now.Before(progress.Deadline) || !now.Before(progress.AttemptDeadline(marker.Policy)) {
				machine, lookupErr := d.machines.Lookup(marker.MachineID, marker.MachineVersion)
				if lookupErr == nil {
					return d.fail(ctx, lease, machine, context.DeadlineExceeded)
				}
				return d.store.FailContinuationAttempt(ctx, lease, d.config.Clock,
					fsm.Failure{Code: "continuation_timeout"}, fsm.DispositionIntervention)
			}
			return ErrLeaseHeld
		}
	}
	machine, lookupErr := d.machines.Lookup(marker.MachineID, marker.MachineVersion)
	compatible := lookupErr == nil && machine.HasContinuationBehavior()
	if compatible {
		policy, enabled := machine.ContinuationPolicy(marker.Target)
		pinned := marker
		pinned.Policy = policy
		compatible = enabled && MatchContinuationMarkers(marker, pinned) == nil
	}
	if progress.Phase == ContinuationWaiting {
		if err := d.store.SetContinuationCompatibility(ctx, lease, d.config.Clock, !compatible); err != nil {
			return err
		}
	}
	if !compatible {
		return fmt.Errorf("%w: pinned continuation behavior unavailable", ErrCompatibility)
	}
	if progress.Phase == ContinuationWaiting {
		if err := d.store.StartContinuation(ctx, lease, d.config.Clock); err != nil {
			return err
		}
		progress, err = d.store.ReadContinuation(ctx, marker.ExecutionID)
		if err != nil {
			return err
		}
	}
	if progress.Decision != nil {
		return d.deliver(ctx, marker, lease, machine, *progress.Decision)
	}
	now := d.config.Clock()
	remaining := min(progress.AttemptDeadline(marker.Policy).Sub(now), progress.Deadline.Sub(now), lease.ExpiresAt.Sub(now))
	if remaining <= 0 {
		return d.fail(ctx, lease, machine, context.DeadlineExceeded)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	decision, evalErr := machine.EvaluateContinuation(attemptCtx, marker.Entity, marker.Target)
	if evalErr != nil {
		return d.fail(ctx, lease, machine, evalErr)
	}
	var persisted ContinuationDecision
	if decision.Continue {
		eventCodec, err := resolveCodec(d.config.ResolveEvent, d.config.EventCodec)
		if err != nil {
			return d.fail(ctx, lease, machine, err)
		}
		inputCodec, err := resolveCodec(d.config.ResolveInput, d.config.InputCodec)
		if err != nil {
			return d.fail(ctx, lease, machine, err)
		}
		event, err := eventCodec.Encode(attemptCtx, decision.Event)
		if err != nil {
			return d.fail(ctx, lease, machine, err)
		}
		input, err := inputCodec.Encode(attemptCtx, decision.Input)
		if err != nil {
			return d.fail(ctx, lease, machine, err)
		}
		persisted = ContinuationDecision{
			Continue: true, Event: EncodedValue{Codec: d.config.EventCodec, Data: event},
			Input: EncodedValue{Codec: d.config.InputCodec, Data: input},
		}
	}
	if err := d.store.RecordContinuationDecision(ctx, lease, d.config.Clock, persisted); err != nil {
		now := d.config.Clock()
		if !now.Before(progress.Deadline) || !now.Before(progress.AttemptDeadline(marker.Policy)) {
			return d.fail(ctx, lease, machine, context.DeadlineExceeded)
		}
		return fmt.Errorf("record continuation decision: %w", err)
	}
	if persisted.Continue {
		return d.deliver(ctx, marker, lease, machine, persisted)
	}
	return nil
}

type permittedSuccessorHost[E comparable, S comparable] struct {
	host   ContinuationDeliveryHost[E, S]
	permit ContinuationDeliveryPermit
}

type fencedSuccessorStore[E comparable, S comparable] struct {
	ContinuationDriveStore[E, S]
	permit ContinuationDeliveryPermit
	clock  func() time.Time
}

func (s fencedSuccessorStore[E, S]) RecordSuccessorDenial(ctx context.Context, binding SuccessorBinding[E, S], principal string) error {
	return s.RecordContinuationDenial(ctx, binding, principal, s.permit, s.clock)
}

func (h permittedSuccessorHost[E, S]) LookupSuccessor(ctx context.Context, binding SuccessorBinding[E, S]) (SuccessorHostResult[E, S], error) {
	return h.host.LookupSuccessor(ctx, binding)
}

func (h permittedSuccessorHost[E, S]) AcceptSuccessor(ctx context.Context, binding SuccessorBinding[E, S]) (SuccessorHostResult[E, S], error) {
	return h.host.AcceptContinuationSuccessor(ctx, binding, h.permit)
}

func (d *ContinuationDriver[E, S, Event, Input]) reconcile(ctx context.Context, marker ContinuationMarker[E, S], permit ContinuationDeliveryPermit, allowNew bool) error {
	if nilcheck.IsNil(d.config.Host) {
		return ErrSuccessorPending
	}
	recovery, err := NewSuccessorRecovery[E, S](
		fencedSuccessorStore[E, S]{ContinuationDriveStore: d.store, permit: permit, clock: d.config.Clock},
		permittedSuccessorHost[E, S]{host: d.config.Host, permit: permit},
	)
	if err != nil {
		return err
	}
	err = recovery.Reconcile(ctx, marker, allowNew)
	if err == nil || errors.Is(err, ErrSuccessorDenied) || errors.Is(err, ErrSuccessorSuperseded) || errors.Is(err, ErrSuccessorCancelled) {
		if reconcileErr := d.store.ReconcileContinuation(ctx, marker, d.config.Clock); reconcileErr != nil {
			return errors.Join(err, reconcileErr)
		}
	}
	return err
}

func (d *ContinuationDriver[E, S, Event, Input]) reconcileWaiver(ctx context.Context, marker ContinuationMarker[E, S]) error {
	err := d.store.ReconcileContinuation(ctx, marker, d.config.Clock)
	if err == nil {
		progress, readErr := d.store.ReadContinuation(ctx, marker.ExecutionID)
		if readErr != nil {
			return readErr
		}
		if progress.Phase != ContinuationWaiverPending {
			return nil
		}
		return ErrContinuationPending
	}
	if !errors.Is(err, ErrSuccessorPending) || nilcheck.IsNil(d.config.Host) {
		return err
	}
	inspection, err := d.store.InspectSuccessor(ctx, marker)
	if err != nil {
		return err
	}
	result, err := d.config.Host.CancelContinuationSuccessor(ctx, inspection.Binding)
	if err != nil {
		return err
	}
	switch result.Outcome {
	case SuccessorHostAccepted:
		if result.Receipt == nil || result.Cancellation != nil || !reflect.DeepEqual(result.Receipt.Binding, inspection.Binding) {
			return ErrConflict
		}
		if _, err := d.store.FinalizeSuccessor(ctx, *result.Receipt); err != nil {
			return err
		}
	case SuccessorHostCanceled, SuccessorHostSuperseded:
		if result.Cancellation == nil || result.Receipt != nil || !reflect.DeepEqual(result.Cancellation.Binding, inspection.Binding) ||
			result.Cancellation.Superseded != (result.Outcome == SuccessorHostSuperseded) {
			return ErrConflict
		}
		if err := d.store.CancelSuccessor(ctx, *result.Cancellation); err != nil {
			return err
		}
	default:
		return ErrSuccessorPending
	}
	return d.store.ReconcileContinuation(ctx, marker, d.config.Clock)
}

func continuationSuccessorKey[E comparable, S comparable](marker ContinuationMarker[E, S]) string {
	h := sha256.New()
	for _, value := range []string{marker.ExecutionID, marker.OperationID} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	return "rff-continuation:" + hex.EncodeToString(h.Sum(nil))
}

func (d *ContinuationDriver[E, S, Event, Input]) deliver(ctx context.Context, marker ContinuationMarker[E, S], lease ContinuationLease, machine *fsm.Machine[E, S, Event, Input], decision ContinuationDecision) error {
	if d.config.Acceptor == nil {
		return ErrSuccessorPending
	}
	progress, err := d.store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil {
		return err
	}
	expires := minTime(progress.Deadline, progress.AttemptDeadline(marker.Policy), lease.ExpiresAt)
	remaining := expires.Sub(d.config.Clock())
	if remaining <= 0 {
		return d.fail(ctx, lease, machine, context.DeadlineExceeded)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	eventCodec, err := resolveCodec(d.config.ResolveEvent, decision.Event.Codec)
	if err != nil {
		return d.fail(ctx, lease, machine, err)
	}
	inputCodec, err := resolveCodec(d.config.ResolveInput, decision.Input.Codec)
	if err != nil {
		return d.fail(ctx, lease, machine, err)
	}
	event, err := eventCodec.Decode(attemptCtx, decision.Event.Data)
	if err != nil {
		return d.fail(ctx, lease, machine, err)
	}
	input, err := inputCodec.Decode(attemptCtx, decision.Input.Data)
	if err != nil {
		return d.fail(ctx, lease, machine, err)
	}
	revision, err := d.config.OriginRevision(attemptCtx, marker)
	if err != nil {
		return d.fail(ctx, lease, machine, err)
	}
	request := SuccessorRequest[E, S, Event, Input]{
		Origin: marker, OriginRevision: revision,
		Request: Request[E, S, Event, Input]{
			Entity: marker.Entity, Key: continuationSuccessorKey(marker), Operation: Transition,
			MachineID: marker.MachineID, MachineVersion: marker.MachineVersion, Previous: marker.Target, HasPrevious: true,
			Event: event, HasEvent: true, EventCodec: decision.Event.Codec, Input: input, InputCodec: decision.Input.Codec,
			Notification: NotifyInherit,
		},
	}
	decide, err := d.config.Acceptor.SuccessorDecision(attemptCtx, request)
	if err != nil {
		return d.fail(ctx, lease, machine, err)
	}
	_, _, err = d.store.PrepareContinuationSuccessor(ctx, lease, d.config.Clock, decide)
	if errors.Is(err, ErrBusy) {
		if parkErr := d.store.ParkContinuation(ctx, lease, d.config.Clock); parkErr != nil {
			return errors.Join(err, parkErr)
		}
		return ErrBusy
	}
	if err != nil {
		if errors.Is(err, ErrStaleLease) {
			return fmt.Errorf("prepare continuation successor: %w", err)
		}
		return d.fail(ctx, lease, machine, err)
	}
	progress, err = d.store.ReadContinuation(ctx, marker.ExecutionID)
	if err != nil {
		return err
	}
	expires = minTime(progress.Deadline, progress.AttemptDeadline(marker.Policy), lease.ExpiresAt)
	return d.reconcile(attemptCtx, marker, ContinuationDeliveryPermit{Generation: lease.Generation, ExpiresAt: expires}, true)
}

func minTime(first time.Time, rest ...time.Time) time.Time {
	for _, candidate := range rest {
		if candidate.Before(first) {
			first = candidate
		}
	}
	return first
}

func (d *ContinuationDriver[E, S, Event, Input]) fail(
	ctx context.Context, lease ContinuationLease, machine *fsm.Machine[E, S, Event, Input], cause error,
) error {
	failure, disposition, err := machine.ClassifyFailure(cause)
	if err != nil {
		failure, disposition = fsm.Failure{Code: "continuation_unclassified"}, fsm.DispositionIntervention
	}
	if err := d.store.FailContinuationAttempt(ctx, lease, d.config.Clock, failure, disposition); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
