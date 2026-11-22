package taskmanager

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func commitTask(ctx context.Context, tx *sql.Tx, execution transition.Execution[string, string], input fixtureInput,
	receipt transition.CommitReceipt,
) (transition.CommitReceipt, error) {
	if input.Task == nil {
		return transition.CommitReceipt{}, transition.ErrInvalidDecision
	}
	valid, err := validRuntimeEntity(ctx, tx, taskEntityKind, input.Task.ID, execution.Entity)
	if err != nil {
		return transition.CommitReceipt{}, fmt.Errorf("validate task identity: %w", err)
	}
	if !valid {
		return transition.CommitReceipt{}, transition.ErrInvalidDecision
	}
	var open bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_groups WHERE id = ? AND closed = 0)`, input.GroupID).Scan(&open); err != nil {
		return transition.CommitReceipt{}, fmt.Errorf("check task group: %w", err)
	}
	reject := func() (transition.CommitReceipt, error) {
		receipt.Outcome = transition.CommitDenied
		receipt.Failure = &fsm.Failure{Code: "task-invariant"}
		return receipt, nil
	}
	if !open {
		return reject()
	}
	if execution.Operation == transition.Initialize {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_tasks WHERE id = ?)`, input.Task.ID).Scan(&exists); err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("check task identity at commit: %w", err)
		}
		if exists {
			return reject()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_tasks (id, group_id, title, status, assignee) VALUES (?, ?, ?, ?, '')`,
			input.Task.ID, input.GroupID, input.Task.Title, execution.Target); err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("commit task creation: %w", err)
		}
	} else if execution.Operation == transition.Transition {
		result, err := tx.ExecContext(ctx, `UPDATE tm_tasks SET status = ? WHERE id = ? AND group_id = ? AND status = ?`,
			execution.Target, input.Task.ID, input.GroupID, execution.Previous)
		if err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("commit task transition: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return transition.CommitReceipt{}, fmt.Errorf("count task updates: %w", err)
		}
		if changed != 1 {
			return reject()
		}
	} else {
		return transition.CommitReceipt{}, transition.ErrInvalidDecision
	}
	receipt.Outcome = transition.CommitSucceeded
	return receipt, nil
}
