package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type TaskAssignment struct {
	MembershipID string
	At           time.Time
}

func authorizedTask(ctx context.Context, tx *sql.Tx, actor, id string) (groupID, assignee string, err error) {
	err = tx.QueryRowContext(ctx, `SELECT t.group_id, t.assignee FROM tm_tasks t
		JOIN tm_groups g ON g.id = t.group_id AND g.closed = 0
		JOIN tm_memberships m ON m.group_id = t.group_id AND m.user_id = ? AND m.status = 'Active'
		WHERE t.id = ?`, actor, id).Scan(&groupID, &assignee)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrDenied
	}
	if err != nil {
		return "", "", fmt.Errorf("authorize task access: %w", err)
	}
	return groupID, assignee, nil
}

// UpdateTask changes a task's title for an Active group member.
func (f *Fixture) UpdateTask(ctx context.Context, actor, id, title string) error {
	if actor == "" || id == "" || title == "" {
		return ErrInvalidRequest
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		if _, _, err := authorizedTask(ctx, tx, actor, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tm_tasks SET title = ? WHERE id = ?`, title, id); err != nil {
			return fmt.Errorf("update task title: %w", err)
		}
		return nil
	})
}

// AssignTask assigns a current Membership, or unassigns when membershipID is empty.
func (f *Fixture) AssignTask(ctx context.Context, actor, id, membershipID string) error {
	if actor == "" || id == "" {
		return ErrInvalidRequest
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		groupID, previous, err := authorizedTask(ctx, tx, actor, id)
		if err != nil {
			return err
		}
		if membershipID != "" {
			var active bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_memberships
				WHERE id = ? AND group_id = ? AND status = 'Active')`, membershipID, groupID).Scan(&active); err != nil {
				return fmt.Errorf("check task assignee: %w", err)
			}
			if !active {
				return ErrConflict
			}
		}
		if previous == membershipID {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tm_tasks SET assignee = ? WHERE id = ?`, membershipID, id); err != nil {
			return fmt.Errorf("assign task: %w", err)
		}
		if membershipID != "" {
			now := f.clock().UTC()
			if now.IsZero() {
				return ErrInvalidRequest
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO tm_task_assignments (task_id, membership_id, assigned_at) VALUES (?, ?, ?)`,
				id, membershipID, now.Format(time.RFC3339Nano)); err != nil {
				return fmt.Errorf("record task assignment: %w", err)
			}
		}
		return nil
	})
}

// TaskAssignmentHistory retains the Membership references of past assignments.
func (f *Fixture) TaskAssignmentHistory(ctx context.Context, actor, id string) (history []TaskAssignment, err error) {
	if actor == "" || id == "" {
		return nil, ErrInvalidRequest
	}
	err = f.transaction(ctx, func(tx *sql.Tx) error {
		if _, _, err := authorizedTask(ctx, tx, actor, id); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT membership_id, assigned_at FROM tm_task_assignments WHERE task_id = ? ORDER BY id`, id)
		if err != nil {
			return fmt.Errorf("read task assignment history: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var assignment TaskAssignment
			var at string
			if err := rows.Scan(&assignment.MembershipID, &at); err != nil {
				return fmt.Errorf("scan task assignment: %w", err)
			}
			assignment.At, err = time.Parse(time.RFC3339Nano, at)
			if err != nil {
				return fmt.Errorf("decode task assignment time: %w", err)
			}
			history = append(history, assignment)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return history, nil
}
