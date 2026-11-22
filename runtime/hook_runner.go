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

// Run invokes one hook without rerunning selection or persisting its outcome.
func (r *HookRunner[EntityID, StateID, Event, Input]) Run(
	ctx context.Context, execution Execution[EntityID, StateID], call HookCall,
) (HookResult, error) {
	if execution.ID == "" || execution.Key == "" || call.Attempt.ExecutionID != execution.ID ||
		call.Attempt.Number == 0 || call.Attempt.LeaseGeneration == 0 ||
		call.Attempt.ActivityID == "" || call.Attempt.ActivityIdempotencyKey == "" {
		return HookResult{}, ErrInvalidHook
	}
	switch call.Kind {
	case HookEnter:
		if execution.Operation != Initialize || execution.HasPrevious || call.Failure != nil {
			return HookResult{}, ErrInvalidHook
		}
	case HookTransition:
		if execution.Operation != Transition || !execution.HasPrevious || call.Failure != nil {
			return HookResult{}, ErrInvalidHook
		}
	case HookRetry, HookRecover:
		if call.Failure == nil {
			return HookResult{}, ErrInvalidHook
		}
	default:
		return HookResult{}, ErrInvalidHook
	}
	if execution.Operation != Initialize && execution.Operation != Transition ||
		(execution.Operation == Initialize) == execution.HasPrevious {
		return HookResult{}, ErrInvalidHook
	}
	machine, err := r.machines.Lookup(execution.MachineID, execution.MachineVersion)
	if err != nil {
		return HookResult{}, fmt.Errorf("%w: machine: %w", ErrCompatibility, err)
	}
	if !machine.HasFailureClassifier() {
		return HookResult{}, fmt.Errorf("%w: failure classifier unavailable", ErrCompatibility)
	}
	classify := func(err error) (HookResult, error) {
		failure, disposition, classifyErr := machine.ClassifyFailure(err)
		if classifyErr != nil {
			return HookResult{}, fmt.Errorf("%w: classify hook failure: %w", ErrCompatibility, classifyErr)
		}
		return HookResult{Failure: failure, Disposition: disposition}, nil
	}
	contextResult := func() (HookResult, error) {
		if errors.Is(ctx.Err(), context.Canceled) {
			return HookResult{}, context.Canceled
		}
		return classify(ctx.Err())
	}
	handler, err := machine.HandlerFor(execution.Target)
	if err != nil {
		return HookResult{}, fmt.Errorf("%w: target handler: %w", ErrCompatibility, err)
	}
	inputCodec, err := resolveCodec(r.inputCodec, execution.Input.Codec)
	if err != nil {
		return HookResult{}, err
	}
	input, err := inputCodec.Decode(ctx, slices.Clone(execution.Input.Data))
	if err != nil {
		if ctx.Err() != nil {
			return contextResult()
		}
		return HookResult{}, fmt.Errorf("%w: decode input: %w", ErrCompatibility, err)
	}
	previous := fsm.None[StateID]()
	event := fsm.None[Event]()
	if execution.Operation == Transition {
		eventCodec, err := resolveCodec(r.eventCodec, execution.Event.Codec)
		if err != nil {
			return HookResult{}, err
		}
		value, err := eventCodec.Decode(ctx, slices.Clone(execution.Event.Data))
		if err != nil {
			if ctx.Err() != nil {
				return contextResult()
			}
			return HookResult{}, fmt.Errorf("%w: decode event: %w", ErrCompatibility, err)
		}
		previous = fsm.Some(execution.Previous)
		event = fsm.Some(value)
	}
	hookContext, err := fsm.NewExecutionContext(
		execution.Entity, previous, execution.Target, event, input, execution.Key, call.Attempt,
	)
	if err != nil {
		return HookResult{}, fmt.Errorf("%w: %w", ErrInvalidHook, err)
	}
	if ctx.Err() == nil {
		switch call.Kind {
		case HookEnter:
			err = handler.Enter(ctx, hookContext)
		case HookTransition:
			err = handler.Transition(ctx, hookContext)
		case HookRetry:
			err = handler.Retry(ctx, hookContext, *call.Failure)
		case HookRecover:
			err = handler.Recover(ctx, hookContext, *call.Failure)
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
	return classify(err)
}
