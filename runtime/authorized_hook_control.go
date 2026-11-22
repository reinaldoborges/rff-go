package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// CheckedHookControlStore validates pinned behavior within the fenced
// transaction before a new control mutates the execution. A recorded replay
// returns its original receipt without revalidation. Compatibility errors
// persist their safe diagnostic without changing the execution phase.
type CheckedHookControlStore[EntityID comparable, StateID comparable] interface {
	ApplyCheckedHookControl(context.Context, TransitionLease[EntityID], func() time.Time, HookControl,
		func(Execution[EntityID, StateID], HookCall) error) (HookControlReceipt, error)
}

// AuthorizedHookControls combines host authorization with pinned-behavior checks.
type AuthorizedHookControls[EntityID comparable, StateID comparable, Event, Input any] struct {
	store     CheckedHookControlStore[EntityID, StateID]
	runner    *HookRunner[EntityID, StateID, Event, Input]
	authorize func(context.Context, ControlAuthorization) error
}

func NewAuthorizedHookControls[EntityID comparable, StateID comparable, Event, Input any](
	store CheckedHookControlStore[EntityID, StateID], runner *HookRunner[EntityID, StateID, Event, Input],
	authorize func(context.Context, ControlAuthorization) error,
) (*AuthorizedHookControls[EntityID, StateID, Event, Input], error) {
	if nilcheck.IsNil(store) || runner == nil || authorize == nil {
		return nil, errors.New("checked control store, hook runner, and host authorization are required")
	}
	return &AuthorizedHookControls[EntityID, StateID, Event, Input]{store: store, runner: runner, authorize: authorize}, nil
}

func (c *AuthorizedHookControls[EntityID, StateID, Event, Input]) Hook(
	ctx context.Context, lease TransitionLease[EntityID], clock func() time.Time, control HookControl,
) (HookControlReceipt, error) {
	var action ControlAction
	switch control.Action {
	case RetryIntervention:
		action = ControlRetry
	case AbortIntervention:
		action = ControlAbort
	default:
		return HookControlReceipt{}, ErrInvalidHook
	}
	if control.ID == "" || lease.ExecutionID == "" || clock == nil {
		return HookControlReceipt{}, ErrInvalidRequest
	}
	if err := c.authorize(ctx, ControlAuthorization{ExecutionID: lease.ExecutionID, RequestID: control.ID, Action: action}); err != nil {
		return HookControlReceipt{}, err
	}
	return c.store.ApplyCheckedHookControl(ctx, lease, clock, control, func(execution Execution[EntityID, StateID], call HookCall) error {
		_, err := c.runner.prepare(ctx, execution, call)
		return err
	})
}
