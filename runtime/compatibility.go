package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// CompatibilityComponent identifies behavior required by a pinned execution.
type CompatibilityComponent string

const (
	CompatibilityMachine    CompatibilityComponent = "machine"
	CompatibilityClassifier CompatibilityComponent = "classifier"
	CompatibilityHandler    CompatibilityComponent = "handler"
	CompatibilityInputCodec CompatibilityComponent = "input-codec"
	CompatibilityEventCodec CompatibilityComponent = "event-codec"
)

// CompatibilityDiagnostic contains safe identity data, never input or raw errors.
type CompatibilityDiagnostic struct {
	Component CompatibilityComponent
	Name      string
	Version   string
	Check     CompatibilityCheck
}

// CompatibilityCheck identifies a coordinator action requiring revalidation.
type CompatibilityCheck string

const (
	CompatibilityUnknownAttempt CompatibilityCheck = "unknown-attempt"
	CompatibilityDeadline       CompatibilityCheck = "deadline"
)

// CompatibilityError retains the underlying error for programmatic inspection,
// while its message contains only the safe component identity.
type CompatibilityError struct {
	Diagnostic CompatibilityDiagnostic
	cause      error
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("%s: %s %q version %q", ErrCompatibility, e.Diagnostic.Component, e.Diagnostic.Name, e.Diagnostic.Version)
}

func (e *CompatibilityError) Unwrap() []error {
	if e.cause == nil {
		return []error{ErrCompatibility}
	}
	return []error{ErrCompatibility, e.cause}
}

func compatibilityError(component CompatibilityComponent, name, version string, cause error) error {
	return &CompatibilityError{Diagnostic: CompatibilityDiagnostic{Component: component, Name: name, Version: version}, cause: cause}
}

// CompatibilityStore provides fenced revalidation of persisted diagnostics.
type CompatibilityStore[EntityID comparable, StateID comparable] interface {
	ActiveExecutionReader[EntityID, StateID]
	TransitionLeaseStore[EntityID]
	ReadHookProgress(context.Context, string) (HookProgress, error)
	RevalidateCompatibility(context.Context, TransitionLease[EntityID], func() time.Time, CompatibilityDiagnostic, *CompatibilityDiagnostic) error
}

// CompatibilityCoordinator revalidates blocked hook executions without changing
// their protocol phase, attempt history, or deadlines.
type CompatibilityCoordinator[EntityID comparable, StateID comparable, Event, Input any] struct {
	store  CompatibilityStore[EntityID, StateID]
	runner *HookRunner[EntityID, StateID, Event, Input]
	config CoordinatorConfig
}

func NewCompatibilityCoordinator[EntityID comparable, StateID comparable, Event, Input any](
	store CompatibilityStore[EntityID, StateID], runner *HookRunner[EntityID, StateID, Event, Input], config CoordinatorConfig,
) (*CompatibilityCoordinator[EntityID, StateID, Event, Input], error) {
	if nilcheck.IsNil(store) || runner == nil || config.ID == "" || config.LeaseTTL <= 0 || config.Clock == nil {
		return nil, errors.New("compatibility store, runner, owner, lease duration, and clock are required")
	}
	return &CompatibilityCoordinator[EntityID, StateID, Event, Input]{store: store, runner: runner, config: config}, nil
}

// Reconcile scans blocked executions; hosts must schedule this alongside other scans.
func (c *CompatibilityCoordinator[EntityID, StateID, Event, Input]) Reconcile(ctx context.Context) error {
	ids, err := c.store.ActiveExecutionIDs(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, id := range ids {
		err := c.reconcileOne(ctx, id)
		if errors.Is(err, ErrExecutionNotActive) || errors.Is(err, ErrLeaseHeld) {
			continue
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("revalidate execution %q: %w", id, err))
		}
	}
	return result
}

func (c *CompatibilityCoordinator[EntityID, StateID, Event, Input]) reconcileOne(ctx context.Context, id string) error {
	execution, err := c.store.ReadActiveExecution(ctx, id)
	if err != nil || execution.Compatibility == nil {
		return err
	}
	lease, err := c.store.AcquireTransitionLease(ctx, execution.Entity, id, c.config.ID, c.config.Clock, c.config.LeaseTTL)
	if err != nil {
		return err
	}
	lease, err = c.store.RenewTransitionLease(ctx, lease, c.config.Clock, c.config.LeaseTTL)
	if err != nil {
		return err
	}
	execution, err = c.store.ReadActiveExecution(ctx, id)
	if err != nil || execution.Compatibility == nil {
		return err
	}
	progress, err := c.store.ReadHookProgress(ctx, id)
	hasProgress := err == nil
	if err != nil && !errors.Is(err, ErrNoHookActivity) {
		return err
	}
	call := HookCall{Kind: progress.Dispatch.Kind, Failure: progress.Dispatch.Failure}
	if !hasProgress {
		call.Kind = HookEnter
		if execution.Operation == Transition {
			call.Kind = HookTransition
		}
	}
	_, err = c.runner.prepare(ctx, execution, call)
	if err == nil {
		switch execution.Compatibility.Check {
		case CompatibilityUnknownAttempt:
			_, err = c.runner.ClassifyHookFailure(execution, ErrAttemptUnknown)
		case CompatibilityDeadline:
			_, err = c.runner.ClassifyHookFailure(execution, ErrTransitionDeadline)
		}
	}
	var next *CompatibilityDiagnostic
	if err != nil {
		var compatibility *CompatibilityError
		if !errors.As(err, &compatibility) {
			return err
		}
		next = &compatibility.Diagnostic
		next.Check = execution.Compatibility.Check
	}
	return c.store.RevalidateCompatibility(ctx, lease, c.config.Clock, *execution.Compatibility, next)
}
