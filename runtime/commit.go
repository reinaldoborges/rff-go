package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// CommitOutcome is the host's durable, final commit decision.
type CommitOutcome string

const (
	CommitSucceeded CommitOutcome = "succeeded"
	CommitDenied    CommitOutcome = "rejected"
)

// CommitReceipt contains only durable, safe host outcome data.
type CommitReceipt struct {
	ExecutionID string
	OperationID string
	Outcome     CommitOutcome
	Failure     *fsm.Failure
}

// CommitHost is implemented by the authoritative domain-state owner. It must
// return an existing receipt before re-evaluating the operation and atomically
// persist a new receipt with either its domain mutation or definitive
// rejection. It must enforce the current transition fence at its authoritative
// commit boundary. Errors indicate unknown or transient outcomes, never a
// definitive rejection. Hosts must not expose raw errors in receipts.
type CommitHost[EntityID comparable, StateID comparable] interface {
	Commit(context.Context, TransitionLease[EntityID], Execution[EntityID, StateID]) (CommitReceipt, error)
}

// CommitStore persists final host receipts under the current transition fence.
type CommitStore[EntityID comparable, StateID comparable] interface {
	ActiveExecutionReader[EntityID, StateID]
	TransitionLeaseStore[EntityID]
	RecordCommitReceipt(context.Context, TransitionLease[EntityID], func() time.Time, CommitReceipt) error
	StartRejectedRecovery(context.Context, TransitionLease[EntityID], func() time.Time) error
}

// CommitCoordinator reconciles durable work success with the host's receipt.
type CommitCoordinator[EntityID comparable, StateID comparable] struct {
	store  CommitStore[EntityID, StateID]
	host   CommitHost[EntityID, StateID]
	config CoordinatorConfig
}

// NewCommitCoordinator creates a commit reconciler with explicit lease settings.
func NewCommitCoordinator[EntityID comparable, StateID comparable](
	store CommitStore[EntityID, StateID], host CommitHost[EntityID, StateID], config CoordinatorConfig,
) (*CommitCoordinator[EntityID, StateID], error) {
	if nilcheck.IsNil(store) || nilcheck.IsNil(host) || config.ID == "" || config.LeaseTTL <= 0 || config.Clock == nil {
		return nil, errors.New("commit store, host, owner, lease duration, and clock are required")
	}
	return &CommitCoordinator[EntityID, StateID]{store: store, host: host, config: config}, nil
}

// Reconcile scans all active commit-pending executions, independent of wakeups.
func (c *CommitCoordinator[EntityID, StateID]) Reconcile(ctx context.Context) error {
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
			result = errors.Join(result, fmt.Errorf("reconcile commit for %q: %w", id, err))
		}
	}
	return result
}

func (c *CommitCoordinator[EntityID, StateID]) reconcileOne(ctx context.Context, id string) error {
	execution, err := readForReconciliation(ctx, c.store, id)
	if err != nil {
		return err
	}
	if execution.Compatibility != nil {
		return nil
	}
	if execution.Status == CommitRejected && (execution.CommitOperationID == "" || execution.CommitReceipt == nil ||
		execution.CommitReceipt.Outcome != CommitDenied) {
		return ErrInvalidHook
	}
	if !commitReconciliable(execution) {
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
	if execution.Status == CommitRejected && (execution.CommitOperationID == "" || execution.CommitReceipt == nil ||
		execution.CommitReceipt.Outcome != CommitDenied) {
		return ErrInvalidHook
	}
	if !commitReconciliable(execution) {
		return nil
	}
	if execution.Status == CommitRejected {
		return c.store.StartRejectedRecovery(ctx, lease, c.config.Clock)
	}
	receipt, err := c.host.Commit(ctx, lease, execution)
	if err != nil {
		return err
	}
	if err := c.store.RecordCommitReceipt(ctx, lease, c.config.Clock, receipt); err != nil {
		return err
	}
	if receipt.Outcome == CommitDenied {
		return c.store.StartRejectedRecovery(ctx, lease, c.config.Clock)
	}
	return nil
}

func commitReconciliable[EntityID comparable, StateID comparable](execution Execution[EntityID, StateID]) bool {
	return execution.CommitOperationID != "" &&
		(execution.Status == Running && execution.CommitReceipt == nil ||
			execution.Status == CommitRejected && execution.CommitReceipt != nil && execution.CommitReceipt.Outcome == CommitDenied)
}
