package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var (
	ErrInvalidRequest  = errors.New("invalid transition request")
	ErrConflict        = errors.New("idempotency key conflicts with an earlier request")
	ErrBusy            = errors.New("entity already has an active transition")
	ErrCompatibility   = errors.New("request compatibility cannot be verified")
	ErrInvalidDecision = errors.New("invalid acceptance transaction decision")
)

// Acceptor evaluates new selections and delegates atomic acceptance to a host store.
type Acceptor[EntityID comparable, StateID comparable, Event, Input any] struct {
	store      AcceptanceStore[EntityID, StateID]
	machines   *fsm.Registry[EntityID, StateID, Event, Input]
	inputCodec CodecResolver[Input]
	eventCodec CodecResolver[Event]
	newID      func() (string, error)
	now        func() time.Time
}

// NewAcceptor creates an acceptor with explicit host-owned dependencies.
func NewAcceptor[EntityID comparable, StateID comparable, Event, Input any](
	store AcceptanceStore[EntityID, StateID],
	machines *fsm.Registry[EntityID, StateID, Event, Input],
	inputCodec CodecResolver[Input],
	eventCodec CodecResolver[Event],
	newID func() (string, error),
	now func() time.Time,
) (*Acceptor[EntityID, StateID, Event, Input], error) {
	if nilcheck.IsNil(store) || machines == nil || inputCodec == nil || eventCodec == nil || newID == nil || now == nil {
		return nil, fmt.Errorf("%w: acceptor dependencies are required", ErrInvalidRequest)
	}
	return &Acceptor[EntityID, StateID, Event, Input]{
		store: store, machines: machines, inputCodec: inputCodec, eventCodec: eventCodec, newID: newID, now: now,
	}, nil
}

// Accept creates an execution or returns an existing semantically equal one.
func (a *Acceptor[EntityID, StateID, Event, Input]) Accept(
	ctx context.Context, request Request[EntityID, StateID, Event, Input],
) (Execution[EntityID, StateID], bool, error) {
	if err := validateRequest(request); err != nil {
		return Execution[EntityID, StateID]{}, false, err
	}
	return a.store.Accept(ctx, request.Entity, request.Key, func(view AcceptanceView[EntityID, StateID]) (*Execution[EntityID, StateID], error) {
		if view.Existing != nil {
			if err := a.match(ctx, *view.Existing, request); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if view.Active {
			return nil, ErrBusy
		}
		return a.prepare(ctx, request)
	})
}

func validateRequest[EntityID comparable, StateID comparable, Event, Input any](r Request[EntityID, StateID, Event, Input]) error {
	if r.Key == "" || r.MachineID == "" || r.MachineVersion == "" || r.InputCodec.Name == "" || r.InputCodec.Version == "" {
		return fmt.Errorf("%w: key, machine, version, and input codec are required", ErrInvalidRequest)
	}
	switch r.Operation {
	case Initialize:
		if r.HasPrevious || r.HasEvent {
			return fmt.Errorf("%w: initialization cannot have a source or event", ErrInvalidRequest)
		}
	case Transition:
		if !r.HasPrevious || !r.HasEvent || r.EventCodec.Name == "" || r.EventCodec.Version == "" {
			return fmt.Errorf("%w: transition requires a source, event, and event codec", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: unknown operation", ErrInvalidRequest)
	}
	if r.Notification != NotifyInherit && r.Notification != NotifyEnable && r.Notification != NotifyDisable {
		return fmt.Errorf("%w: invalid notification override", ErrInvalidRequest)
	}
	return nil
}

func (a *Acceptor[EntityID, StateID, Event, Input]) match(ctx context.Context, existing Execution[EntityID, StateID], request Request[EntityID, StateID, Event, Input]) error {
	original := OriginalRequest{
		Operation: existing.Operation, MachineID: existing.MachineID, MachineVersion: existing.MachineVersion,
		Event: existing.Event, Input: existing.Input, NotificationOverride: existing.NotificationOverride,
	}
	if existing.OriginalRequest != nil {
		original = *existing.OriginalRequest
	}
	if original.Operation != request.Operation || original.MachineID != request.MachineID ||
		original.MachineVersion != request.MachineVersion || original.NotificationOverride != request.Notification {
		return ErrConflict
	}
	input, err := resolveCodec(a.inputCodec, original.Input.Codec)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	equal, err := input.Equal(ctx, slices.Clone(original.Input.Data), request.Input)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: input equality: %w", ErrCompatibility, err)
	}
	if !equal {
		return ErrConflict
	}
	if request.Operation == Transition {
		event, err := resolveCodec(a.eventCodec, original.Event.Codec)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		equal, err = event.Equal(ctx, slices.Clone(original.Event.Data), request.Event)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: event equality: %w", ErrCompatibility, err)
		}
		if !equal {
			return ErrConflict
		}
	}
	return nil
}

func (a *Acceptor[EntityID, StateID, Event, Input]) prepare(ctx context.Context, request Request[EntityID, StateID, Event, Input]) (*Execution[EntityID, StateID], error) {
	machine, err := a.machines.Lookup(request.MachineID, request.MachineVersion)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCompatibility, err)
	}
	inputCodec, err := resolveCodec(a.inputCodec, request.InputCodec)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	inputData, err := inputCodec.Encode(ctx, request.Input)
	if err != nil {
		return nil, fmt.Errorf("encode input: %w", err)
	}
	var selected fsm.Selection[StateID]
	var event EncodedValue
	if request.Operation == Initialize {
		selected, err = machine.SelectInitial(request.Input)
	} else {
		eventCodec, codecErr := resolveCodec(a.eventCodec, request.EventCodec)
		if codecErr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, codecErr
		}

		data, codecErr := eventCodec.Encode(ctx, request.Event)
		if codecErr != nil {
			return nil, fmt.Errorf("encode event: %w", codecErr)
		}
		event = EncodedValue{Codec: request.EventCodec, Data: slices.Clone(data)}
		selected, err = machine.Select(request.Previous, request.Event, request.Input)
	}
	if err != nil {
		return nil, err
	}
	if err := validatePolicy(selected.Policy); err != nil {
		return nil, err
	}
	id, err := a.newID()
	if err != nil {
		return nil, fmt.Errorf("allocate execution ID: %w", err)
	}
	if id == "" {
		return nil, fmt.Errorf("%w: execution ID is empty", ErrInvalidRequest)
	}
	now := a.now()
	notify := selected.Notify
	if request.Notification == NotifyEnable {
		notify = true
	} else if request.Notification == NotifyDisable {
		notify = false
	}
	var previous StateID
	if request.HasPrevious {
		previous = request.Previous
	}
	return &Execution[EntityID, StateID]{
		SchemaVersion: CurrentSchemaVersion, ID: id, Entity: request.Entity, Key: request.Key, Operation: request.Operation,
		MachineID: request.MachineID, MachineVersion: request.MachineVersion,
		Previous: previous, HasPrevious: request.HasPrevious, Target: selected.Target,
		Event: event, Input: EncodedValue{Codec: request.InputCodec, Data: slices.Clone(inputData)},
		NotificationOverride: request.Notification, Notify: notify, Policy: selected.Policy,
		AcceptedAt: now, Deadline: now.Add(selected.Policy.OverallTimeout), Status: Waiting,
	}, nil
}
