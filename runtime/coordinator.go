package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// CoordinatorStore supplies durable discovery, fencing, and hook operations.
type CoordinatorStore[EntityID comparable, StateID comparable] interface {
	ActiveExecutionReader[EntityID, StateID]
	TransitionLeaseStore[EntityID]
	HookDispatcher[EntityID]
	WorkSuccessStore[EntityID]
	InterventionStore[EntityID]
	AbortStore[EntityID]
}

// HookFailureClassifier resolves the application's pinned failure classifier.
type HookFailureClassifier[EntityID comparable, StateID comparable] interface {
	ClassifyHookFailure(Execution[EntityID, StateID], error) (HookResult, error)
}

// CoordinatorConfig sets the deployment-specific lease and clock.
type CoordinatorConfig struct {
	ID       string
	LeaseTTL time.Duration
	Clock    func() time.Time
}

// Coordinator reconciles persisted executions; callers schedule regular scans.
type Coordinator[EntityID comparable, StateID comparable] struct {
	store      CoordinatorStore[EntityID, StateID]
	classifier HookFailureClassifier[EntityID, StateID]
	config     CoordinatorConfig
}

// NewCoordinator creates a reconciler with an explicitly configured lease.
func NewCoordinator[EntityID comparable, StateID comparable](
	store CoordinatorStore[EntityID, StateID], classifier HookFailureClassifier[EntityID, StateID], config CoordinatorConfig,
) (*Coordinator[EntityID, StateID], error) {
	if nilcheck.IsNil(store) || nilcheck.IsNil(classifier) || config.ID == "" || config.LeaseTTL <= 0 || config.Clock == nil {
		return nil, errors.New("coordinator store, classifier, owner, lease duration, and clock are required")
	}
	return &Coordinator[EntityID, StateID]{store: store, classifier: classifier, config: config}, nil
}

// Reconcile scans all active executions independently. Callers must invoke it
// regularly, even without wakeups, and often enough to renew held leases.
func (c *Coordinator[EntityID, StateID]) Reconcile(ctx context.Context) error {
	ids, err := c.store.ActiveExecutionIDs(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, id := range ids {
		if err := c.reconcileOne(ctx, id); err != nil {
			if errors.Is(err, ErrExecutionNotActive) || errors.Is(err, ErrLeaseHeld) {
				continue
			}
			result = errors.Join(result, fmt.Errorf("reconcile execution %q: %w", id, err))
		}
	}
	return result
}

func (c *Coordinator[EntityID, StateID]) reconcileOne(ctx context.Context, id string) error {
	execution, err := c.store.ReadActiveExecution(ctx, id)
	if err != nil {
		return err
	}
	if !reconcilable(execution) {
		return nil
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
	if err != nil {
		return err
	}
	if !reconcilable(execution) {
		return nil
	}
	progress, err := c.store.ReadHookProgress(ctx, id)
	if errors.Is(err, ErrNoHookActivity) {
		if execution.Status == Aborting {
			return ErrInvalidHook
		}
		if !c.config.Clock().Before(execution.EffectiveDeadline()) {
			return c.enterDeadlineIntervention(ctx, lease, execution)
		}
		kind := HookEnter
		if execution.Operation == Transition {
			kind = HookTransition
		}
		_, err = c.store.DispatchHook(ctx, lease, c.config.Clock, HookDispatch{Kind: kind, DueAt: execution.AcceptedAt})
		return err
	}
	if err != nil {
		return err
	}
	if !progress.Completed {
		if progress.Started && !c.config.Clock().Before(progress.LeaseExpiresAt) {
			result, err := c.classifier.ClassifyHookFailure(execution, ErrAttemptUnknown)
			if err != nil {
				return err
			}
			if len(progress.Attempts) == 0 {
				return ErrInvalidHook
			}
			attempt := progress.Attempts[len(progress.Attempts)-1]
			err = c.store.ResolveUnknownHook(ctx, lease, c.config.Clock, progress.ActivityID, attempt.Metadata.LeaseGeneration, result)
			if errors.Is(err, ErrActivityLeaseHeld) || errors.Is(err, ErrStaleActivityLease) {
				return nil
			}
			return err
		}
		if !progress.Started && !c.config.Clock().Before(execution.EffectiveDeadline()) {
			return c.enterDeadlineIntervention(ctx, lease, execution)
		}
		return nil
	}
	if !progress.Result.Success {
		if len(progress.Attempts) == 0 || progress.BudgetUsed == 0 || progress.BudgetUsed > uint(len(progress.Attempts)) {
			return ErrInvalidHook
		}
		var failures uint
		for _, attempt := range progress.Attempts[len(progress.Attempts)-int(progress.BudgetUsed):] {
			if attempt.Result != nil && !attempt.Result.Success && attempt.Result.Disposition == fsm.DispositionRetry {
				failures++
			}
		}
		if failures == 0 {
			return c.store.EnterHookIntervention(ctx, lease, c.config.Clock)
		}
		retry, delay, err := DecideRetry(execution.Policy, progress.BudgetUsed, failures-1, progress.Result.Disposition)
		if err != nil {
			return err
		}
		if !retry {
			return c.store.EnterHookIntervention(ctx, lease, c.config.Clock)
		}
		last := progress.Attempts[len(progress.Attempts)-1]
		due := last.CompletedAt.Add(delay)
		if !c.config.Clock().Before(execution.EffectiveDeadline()) {
			return c.enterDeadlineIntervention(ctx, lease, execution)
		}
		if !due.Before(execution.EffectiveDeadline()) {
			return nil
		}
		switch progress.Dispatch.Kind {
		case HookEnter, HookTransition:
			var cycle uint
			for _, attempt := range progress.Attempts {
				if attempt.Kind == HookRetry && attempt.Metadata.RetryCycle > cycle {
					cycle = attempt.Metadata.RetryCycle
				}
			}
			failure := progress.Result.Failure
			_, err = c.store.DispatchHook(ctx, lease, c.config.Clock, HookDispatch{
				Kind: HookRetry, RetryCycle: cycle + 1, DueAt: due, Failure: &failure,
			})
			return err
		case HookRetry, HookRecover:
			return c.store.RedeliverHook(ctx, lease, c.config.Clock, progress.ActivityID, last.Metadata.LeaseGeneration, due)
		default:
			return ErrInvalidHook
		}
	}
	if progress.Dispatch.Kind == HookRetry {
		if progress.BudgetUsed >= execution.Policy.MaxAttempts {
			return c.store.EnterHookIntervention(ctx, lease, c.config.Clock)
		}
		if !c.config.Clock().Before(execution.EffectiveDeadline()) {
			return c.enterDeadlineIntervention(ctx, lease, execution)
		}
		last := progress.Attempts[len(progress.Attempts)-1]
		for i := len(progress.Attempts) - 1; i >= 0; i-- {
			attempt := progress.Attempts[i]
			if attempt.Kind != HookEnter && attempt.Kind != HookTransition {
				continue
			}
			if attempt.Unknown {
				return c.store.RedeliverHook(ctx, lease, c.config.Clock, attempt.Metadata.ActivityID, attempt.Metadata.LeaseGeneration, last.CompletedAt)
			}
			_, err := c.store.DispatchHook(ctx, lease, c.config.Clock, HookDispatch{
				Kind: attempt.Kind, RetryCycle: progress.Dispatch.RetryCycle, DueAt: last.CompletedAt,
			})
			return err
		}

		return ErrInvalidHook
	}
	if progress.Dispatch.Kind == HookRecover && execution.Status == Aborting {
		return c.store.FinishAbort(ctx, lease, c.config.Clock)
	}
	if progress.Dispatch.Kind != HookEnter && progress.Dispatch.Kind != HookTransition {
		return ErrInvalidHook
	}
	_, err = c.store.MarkWorkSuccess(ctx, lease, c.config.Clock)
	return err
}

func reconcilable[EntityID comparable, StateID comparable](execution Execution[EntityID, StateID]) bool {
	if execution.Status == Aborting {
		return execution.RecoveryStarted
	}
	return execution.CommitOperationID == "" && (execution.Status == Waiting || execution.Status == Running)
}

func (c *Coordinator[EntityID, StateID]) enterDeadlineIntervention(
	ctx context.Context, lease TransitionLease[EntityID], execution Execution[EntityID, StateID],
) error {
	result, err := c.classifier.ClassifyHookFailure(execution, ErrTransitionDeadline)
	if err != nil {
		return err
	}
	return c.store.EnterDeadlineIntervention(ctx, lease, c.config.Clock, result)
}
