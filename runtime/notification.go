package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// NotificationIntent is the stable transition outcome submitted to an outbox.
type NotificationIntent[EntityID comparable, StateID comparable] struct {
	ID                string
	ExecutionID       string
	CommitOperationID string
	Entity            EntityID
	Operation         Operation
	Previous          StateID
	HasPrevious       bool
	Target            StateID
}

// NotificationHandoff is a host-owned durable outbox boundary. Handoff must
// enforce the current transition fence and idempotently retain one intent by ID.
// A nil error means handoff is durable. NotificationHandoffFailure means the
// host definitively knows no handoff occurred and requires repair; other errors
// are unknown outcomes and must be reconciled with the same intent.
// Delivery occurs independently.
type NotificationHandoff[EntityID comparable, StateID comparable] interface {
	Handoff(context.Context, TransitionLease[EntityID], NotificationIntent[EntityID, StateID]) error
}

// NotificationHandoffFailure is a safe, definitive failure before handoff.
type NotificationHandoffFailure struct {
	Failure fsm.Failure
}

func (*NotificationHandoffFailure) Error() string { return "notification handoff requires repair" }

// NotificationStore completes a durably handed-off transition under its fence.
type NotificationStore[EntityID comparable, StateID comparable] interface {
	ActiveExecutionReader[EntityID, StateID]
	TransitionLeaseStore[EntityID]
	CompleteNotificationHandoff(context.Context, TransitionLease[EntityID], func() time.Time, string) error
	EnterNotificationRepair(context.Context, TransitionLease[EntityID], func() time.Time, string, fsm.Failure) error
}

// NotificationCoordinator reconciles durable handoffs independently of delivery.
type NotificationCoordinator[EntityID comparable, StateID comparable] struct {
	store   NotificationStore[EntityID, StateID]
	handoff NotificationHandoff[EntityID, StateID]
	config  CoordinatorConfig
}

// NewNotificationCoordinator creates a fenced notification handoff reconciler.
func NewNotificationCoordinator[EntityID comparable, StateID comparable](
	store NotificationStore[EntityID, StateID], handoff NotificationHandoff[EntityID, StateID], config CoordinatorConfig,
) (*NotificationCoordinator[EntityID, StateID], error) {
	if nilcheck.IsNil(store) || nilcheck.IsNil(handoff) || config.ID == "" || config.LeaseTTL <= 0 || config.Clock == nil {
		return nil, errors.New("notification store, handoff, owner, lease duration, and clock are required")
	}
	return &NotificationCoordinator[EntityID, StateID]{store: store, handoff: handoff, config: config}, nil
}

// Reconcile scans all active notification handoffs, independent of wakeups.
func (c *NotificationCoordinator[EntityID, StateID]) Reconcile(ctx context.Context) error {
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
			result = errors.Join(result, fmt.Errorf("reconcile notification for %q: %w", id, err))
		}
	}
	return result
}

func (c *NotificationCoordinator[EntityID, StateID]) reconcileOne(ctx context.Context, id string) error {
	execution, err := readForReconciliation(ctx, c.store, id)
	if err != nil {
		return err
	}
	if execution.Compatibility != nil {
		return nil
	}
	if execution.Status != Notifying {
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
	execution, err = readForReconciliation(ctx, c.store, id)
	if err != nil {
		return err
	}
	if execution.Compatibility != nil {
		return nil
	}
	if execution.Status != Notifying {
		return nil
	}
	if !execution.Notify || execution.NotificationIntentID == "" || execution.CommitReceipt == nil ||
		execution.CommitReceipt.Outcome != CommitSucceeded || execution.CommitOperationID == "" {
		return ErrInvalidHook
	}
	intent := NotificationIntent[EntityID, StateID]{
		ID: execution.NotificationIntentID, ExecutionID: execution.ID, CommitOperationID: execution.CommitOperationID,
		Entity: execution.Entity, Operation: execution.Operation, Previous: execution.Previous,
		HasPrevious: execution.HasPrevious, Target: execution.Target,
	}
	if err := c.handoff.Handoff(ctx, lease, intent); err != nil {
		var failure *NotificationHandoffFailure
		if errors.As(err, &failure) && failure != nil {
			return c.store.EnterNotificationRepair(ctx, lease, c.config.Clock, intent.ID, failure.Failure)
		}
		return err
	}
	return c.store.CompleteNotificationHandoff(ctx, lease, c.config.Clock, intent.ID)
}
