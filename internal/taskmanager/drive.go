package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type fixtureClock struct{ now func() time.Time }

func (c fixtureClock) Now() time.Time { return c.now() }
func (fixtureClock) NewTicker(d time.Duration) transition.WorkerTicker {
	return (transition.SystemWorkerClock{}).NewTicker(d)
}
func (fixtureClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

type membershipCommitHost struct{ fixture *Fixture }

func (h membershipCommitHost) Commit(ctx context.Context, lease transition.TransitionLease[string],
	_ transition.Execution[string, string],
) (transition.CommitReceipt, error) {
	return h.fixture.store.CommitAtomic(ctx, lease, h.fixture.clock, func(ctx context.Context, tx *sql.Tx,
		execution transition.Execution[string, string],
	) (transition.CommitReceipt, error) {
		receipt := transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID}
		input, err := (jsonCodec[fixtureInput]{}).Decode(ctx, execution.Input.Data)
		if err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("decode membership commit input: %w", err)
		}
		if execution.MachineID == "taskmanager-task" {
			return commitTask(ctx, tx, execution, input, receipt)
		}
		if execution.Entity != input.MembershipID {
			return transition.CommitReceipt{}, transition.ErrInvalidDecision
		}
		if execution.Operation == transition.Transition {
			if input.Event == removeMembership {
				return h.fixture.commitRemoval(ctx, tx, execution, input, receipt)
			}
			var status, deadline string
			var closed bool
			err := tx.QueryRowContext(ctx, `SELECT m.status, m.deadline, g.closed FROM tm_memberships AS m
				JOIN tm_groups AS g ON g.id = m.group_id WHERE m.id = ?`, input.MembershipID).Scan(&status, &deadline, &closed)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return transition.CommitReceipt{}, fmt.Errorf("read membership for decision: %w", err)
			}
			if errors.Is(err, sql.ErrNoRows) {
				receipt.Outcome = transition.CommitDenied
				receipt.Failure = &fsm.Failure{Code: "membership-invariant"}
				return receipt, nil
			}
			expiry, err := time.Parse(time.RFC3339Nano, deadline)
			if err != nil {
				return transition.CommitReceipt{}, fmt.Errorf("decode membership deadline: %w", err)
			}
			now := h.fixture.clock()
			if now.IsZero() {
				return transition.CommitReceipt{}, transition.ErrInvalidLease
			}
			due := !now.Before(expiry)
			if closed || status != execution.Previous || status != string(Invited) ||
				!expiry.Equal(input.Deadline) || (input.Event == expireInvitation) != due {
				receipt.Outcome = transition.CommitDenied
				receipt.Failure = &fsm.Failure{Code: "membership-invariant"}
				return receipt, nil
			}
			if _, err := tx.ExecContext(ctx, `UPDATE tm_memberships SET status = ? WHERE id = ?`, execution.Target, input.MembershipID); err != nil {
				return transition.CommitReceipt{}, fmt.Errorf("commit membership decision: %w", err)
			}
			receipt.Outcome = transition.CommitSucceeded
			return receipt, nil
		}
		if execution.Operation != transition.Initialize || execution.Target != string(Invited) {
			return transition.CommitReceipt{}, transition.ErrInvalidDecision
		}
		var valid bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_groups WHERE id = ? AND closed = 0)
			AND NOT EXISTS(SELECT 1 FROM tm_memberships WHERE id = ? OR
				(group_id = ? AND user_id = ? AND status IN ('Invited', 'Active')))`,
			input.GroupID, input.MembershipID, input.GroupID, input.UserID).Scan(&valid); err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("check invitation invariants: %w", err)
		}
		if !valid {
			receipt.Outcome = transition.CommitDenied
			receipt.Failure = &fsm.Failure{Code: "membership-invariant"}
			return receipt, nil
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_memberships (id, group_id, user_id, status, deadline, reason)
			VALUES (?, ?, ?, ?, ?, '')`, input.MembershipID, input.GroupID, input.UserID, string(Invited),
			input.Deadline.UTC().Format(time.RFC3339Nano)); err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("commit invitation: %w", err)
		}
		receipt.Outcome = transition.CommitSucceeded
		return receipt, nil
	})
}

// Drive reconciles pending inputs and one round of transition work.
func (f *Fixture) Drive(ctx context.Context) error {
	ingress, err := f.invitationIngress()
	if err != nil {
		return err
	}
	if err := ingress.Reconcile(ctx); err != nil {
		return err
	}
	tasks, err := f.taskIngress()
	if err != nil {
		return err
	}
	if err := tasks.Reconcile(ctx); err != nil {
		return err
	}
	decisions, err := f.decisionIngress()
	if err != nil {
		return err
	}
	pendingErr := decisions.Reconcile(ctx)
	config := transition.CoordinatorConfig{ID: "taskmanager", Clock: f.clock, LeaseTTL: time.Minute}
	coordinator, err := transition.NewCoordinator[string, string](f.store, f.runner, config)
	if err != nil {
		return err
	}
	worker, err := transition.NewHookWorker[string, string, string, fixtureInput](f.store, f.runner, transition.WorkerConfig{
		ID: "taskmanager-hook", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: 10 * time.Second, Clock: fixtureClock{now: f.clock},
	})
	if err != nil {
		return err
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		return err
	}
	for {
		err := worker.RunOne(ctx)
		if errors.Is(err, transition.ErrNoHookActivity) {
			break
		}
		if err != nil {
			return err
		}
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		return err
	}
	commits, err := transition.NewCommitCoordinator[string, string](f.store, membershipCommitHost{fixture: f}, config)
	if err != nil {
		return err
	}
	if err := commits.Reconcile(ctx); err != nil {
		return errors.Join(pendingErr, err)
	}
	return errors.Join(pendingErr, f.driveExpiryTimers(ctx), f.driveNotifications(ctx))
}
