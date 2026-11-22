package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/reinaldoborges/rff-go/fsm"
)

// HookKind identifies a state hook dispatched within an execution.
type HookKind string

const (
	HookEnter      HookKind = "enter"
	HookTransition HookKind = "transition"
	HookRetry      HookKind = "retry"
	HookRecover    HookKind = "recover"
)

var ErrInvalidHook = errors.New("invalid state hook invocation")

// HookCall describes one already-started activity attempt.
type HookCall struct {
	Kind    HookKind
	Attempt fsm.AttemptMetadata
	Failure *fsm.Failure
}

// HookResult contains only safe application-classified failure data.
type HookResult struct {
	Success     bool
	Failure     fsm.Failure
	Disposition fsm.Disposition
}

// HookRunner rehydrates pinned execution data and invokes target-state hooks.
// Scheduling, fencing, timeouts, and durable result recording remain the
// coordinator and worker's responsibility.
type HookRunner[EntityID comparable, StateID comparable, Event, Input any] struct {
	machines   *fsm.Registry[EntityID, StateID, Event, Input]
	inputCodec CodecResolver[Input]
	eventCodec CodecResolver[Event]
}

// NewHookRunner constructs a runner using exact-version application behavior.
func NewHookRunner[EntityID comparable, StateID comparable, Event, Input any](
	machines *fsm.Registry[EntityID, StateID, Event, Input],
	inputCodec CodecResolver[Input], eventCodec CodecResolver[Event],
) (*HookRunner[EntityID, StateID, Event, Input], error) {
	if machines == nil || inputCodec == nil || eventCodec == nil {
		return nil, fmt.Errorf("%w: runner dependencies are required", ErrInvalidHook)
	}
	return &HookRunner[EntityID, StateID, Event, Input]{machines: machines, inputCodec: inputCodec, eventCodec: eventCodec}, nil
}

type preparedHook[EntityID comparable, StateID comparable, Event, Input any] struct {
	machine   *fsm.Machine[EntityID, StateID, Event, Input]
	handler   fsm.Handler[EntityID, StateID, Event, Input]
	execution Execution[EntityID, StateID]
	previous  fsm.Optional[StateID]
	event     fsm.Optional[Event]
	input     Input
	kind      HookKind
	failure   fsm.Failure
}

func (r *HookRunner[EntityID, StateID, Event, Input]) prepare(
	ctx context.Context, execution Execution[EntityID, StateID], call HookCall,
) (preparedHook[EntityID, StateID, Event, Input], error) {
	var prepared preparedHook[EntityID, StateID, Event, Input]
	if execution.ID == "" || execution.Key == "" {
		return prepared, ErrInvalidHook
	}
	switch call.Kind {
	case HookEnter:
		if execution.Operation != Initialize || execution.HasPrevious || call.Failure != nil {
			return prepared, ErrInvalidHook
		}
	case HookTransition:
		if execution.Operation != Transition || !execution.HasPrevious || call.Failure != nil {
			return prepared, ErrInvalidHook
		}
	case HookRetry, HookRecover:
		if call.Failure == nil {
			return prepared, ErrInvalidHook
		}
		prepared.failure = *call.Failure
	default:
		return prepared, ErrInvalidHook
	}
	if execution.Operation != Initialize && execution.Operation != Transition ||
		(execution.Operation == Initialize) == execution.HasPrevious {
		return prepared, ErrInvalidHook
	}
	machine, err := r.machines.Lookup(execution.MachineID, execution.MachineVersion)
	if err != nil {
		return prepared, fmt.Errorf("%w: machine: %w", ErrCompatibility, err)
	}
	if !machine.HasFailureClassifier() {
		return prepared, fmt.Errorf("%w: failure classifier unavailable", ErrCompatibility)
	}
	prepared.machine = machine
	prepared.handler, err = machine.HandlerFor(execution.Target)
	if err != nil {
		return prepared, fmt.Errorf("%w: target handler: %w", ErrCompatibility, err)
	}
	inputCodec, err := resolveCodec(r.inputCodec, execution.Input.Codec)
	if err != nil {
		return prepared, err
	}
	prepared.input, err = inputCodec.Decode(ctx, slices.Clone(execution.Input.Data))
	if err != nil {
		if ctx.Err() != nil {
			return prepared, ctx.Err()
		}
		return prepared, fmt.Errorf("%w: decode input: %w", ErrCompatibility, err)
	}
	if execution.Operation == Transition {
		eventCodec, err := resolveCodec(r.eventCodec, execution.Event.Codec)
		if err != nil {
			return prepared, err
		}
		event, err := eventCodec.Decode(ctx, slices.Clone(execution.Event.Data))
		if err != nil {
			if ctx.Err() != nil {
				return prepared, ctx.Err()
			}
			return prepared, fmt.Errorf("%w: decode event: %w", ErrCompatibility, err)
		}
		prepared.previous = fsm.Some(execution.Previous)
		prepared.event = fsm.Some(event)
	}
	prepared.execution = execution
	prepared.kind = call.Kind
	return prepared, nil
}

func (p preparedHook[EntityID, StateID, Event, Input]) classify(err error) (HookResult, error) {
	failure, disposition, classifyErr := p.machine.ClassifyFailure(err)
	if classifyErr != nil {
		return HookResult{}, fmt.Errorf("%w: classify hook failure: %w", ErrCompatibility, classifyErr)
	}
	return HookResult{Failure: failure, Disposition: disposition}, nil
}

func validateHookAttempt(executionID string, attempt fsm.AttemptMetadata) error {
	if executionID == "" || attempt.ExecutionID != executionID || attempt.Number == 0 || attempt.LeaseGeneration == 0 ||
		attempt.ActivityID == "" || attempt.ActivityIdempotencyKey == "" {
		return ErrInvalidHook
	}
	return nil
}

func (p preparedHook[EntityID, StateID, Event, Input]) run(ctx context.Context, attempt fsm.AttemptMetadata) (HookResult, error) {
	if err := validateHookAttempt(p.execution.ID, attempt); err != nil {
		return HookResult{}, err
	}
	hookContext, err := fsm.NewExecutionContext(
		p.execution.Entity, p.previous, p.execution.Target, p.event, p.input, p.execution.Key, attempt,
	)
	if err != nil {
		return HookResult{}, fmt.Errorf("%w: %w", ErrInvalidHook, err)
	}
	if ctx.Err() == nil {
		switch p.kind {
		case HookEnter:
			err = p.handler.Enter(ctx, hookContext)
		case HookTransition:
			err = p.handler.Transition(ctx, hookContext)
		case HookRetry:
			err = p.handler.Retry(ctx, hookContext, p.failure)
		case HookRecover:
			err = p.handler.Recover(ctx, hookContext, p.failure)
		}
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return HookResult{}, context.Canceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = context.DeadlineExceeded
	}
	if err == nil {
		return HookResult{Success: true}, nil
	}
	return p.classify(err)
}

// Run invokes one hook without rerunning selection or persisting its outcome.
func (r *HookRunner[EntityID, StateID, Event, Input]) Run(
	ctx context.Context, execution Execution[EntityID, StateID], call HookCall,
) (HookResult, error) {
	if err := validateHookAttempt(execution.ID, call.Attempt); err != nil {
		return HookResult{}, err
	}
	prepared, err := r.prepare(ctx, execution, call)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && prepared.machine != nil {
			return prepared.classify(context.DeadlineExceeded)
		}
		return HookResult{}, err
	}
	return prepared.run(ctx, call.Attempt)
}

// ClassifyHookFailure applies the machine version pinned by an execution.
func (r *HookRunner[EntityID, StateID, Event, Input]) ClassifyHookFailure(
	execution Execution[EntityID, StateID], failure error,
) (HookResult, error) {
	if failure == nil {
		return HookResult{}, ErrInvalidHook
	}
	machine, err := r.machines.Lookup(execution.MachineID, execution.MachineVersion)
	if err != nil {
		return HookResult{}, fmt.Errorf("%w: machine: %w", ErrCompatibility, err)
	}
	safe, disposition, err := machine.ClassifyFailure(failure)
	if err != nil {
		return HookResult{}, fmt.Errorf("%w: classify hook failure: %w", ErrCompatibility, err)
	}
	return HookResult{Failure: safe, Disposition: disposition}, nil
}
