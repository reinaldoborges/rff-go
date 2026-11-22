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

type RemovalAudit struct {
	MembershipID string
	ExecutionID  string
	Actor        string
	At           time.Time
}

// RemoveMember durably accepts an owner-authorized Active Membership removal.
func (f *Fixture) RemoveMember(ctx context.Context, actor, membershipID, key string) (string, error) {
	if actor == "" || membershipID == "" || key == "" {
		return "", ErrInvalidRequest
	}
	var input fixtureInput
	err := f.db.QueryRowContext(ctx, `SELECT m.group_id, m.user_id FROM tm_memberships m
		JOIN tm_groups g ON g.id = m.group_id AND g.closed = 0 AND g.owner_id = ?
		WHERE m.id = ?`, actor, membershipID).Scan(&input.GroupID, &input.UserID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrDenied
	}
	if err != nil {
		return "", fmt.Errorf("authorize membership removal: %w", err)
	}
	input.Actor, input.MembershipID, input.Event = actor, membershipID, removeMembership
	id, err := inputID(actor, input.GroupID, key)
	if err != nil {
		return "", err
	}
	ingress, err := f.decisionIngress()
	if err != nil {
		return "", err
	}
	outcome, err := ingress.Submit(ctx, transition.InputRequest[fixtureInput]{
		ID: id, WorkflowID: membershipID, Kind: transition.ExternalEvent, Codec: membershipInputCodec, Value: input,
	})
	if errors.Is(err, transition.ErrConflict) {
		return "", ErrConflict
	}
	if err != nil {
		return "", err
	}
	if outcome.Failure != nil {
		switch outcome.Failure.Code {
		case "denied":
			return "", ErrDenied
		case "conflict":
			return "", ErrConflict
		default:
			return "", ErrInvalidRequest
		}
	}
	return outcome.Receipt, nil
}

func (f *Fixture) commitRemoval(ctx context.Context, tx *sql.Tx, execution transition.Execution[string, string],
	input fixtureInput, receipt transition.CommitReceipt,
) (transition.CommitReceipt, error) {
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_memberships m
		JOIN tm_groups g ON g.id = m.group_id AND g.closed = 0
		WHERE m.id = ? AND m.group_id = ? AND m.user_id = ? AND m.status = 'Active' AND m.user_id != g.owner_id)
		AND NOT EXISTS(SELECT 1 FROM tm_tasks WHERE group_id = ? AND assignee = ?)`,
		input.MembershipID, input.GroupID, input.UserID, input.GroupID, input.MembershipID).Scan(&valid); err != nil {
		return transition.CommitReceipt{}, fmt.Errorf("check removal invariants: %w", err)
	}
	if !valid || execution.Previous != string(Active) || execution.Target != string(Removed) {
		receipt.Outcome = transition.CommitDenied
		receipt.Failure = &fsm.Failure{Code: "removal-invariant"}
		return receipt, nil
	}
	now := f.clock().UTC()
	if now.IsZero() {
		return transition.CommitReceipt{}, transition.ErrInvalidLease
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tm_memberships SET status = ? WHERE id = ?`, string(Removed), input.MembershipID); err != nil {
		return transition.CommitReceipt{}, fmt.Errorf("remove membership: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tm_membership_removals (membership_id, execution_id, actor, occurred_at) VALUES (?, ?, ?, ?)`,
		input.MembershipID, execution.ID, input.Actor, now.Format(time.RFC3339Nano)); err != nil {
		return transition.CommitReceipt{}, fmt.Errorf("audit membership removal: %w", err)
	}
	receipt.Outcome = transition.CommitSucceeded
	return receipt, nil
}

// InspectRemoval exposes retained business audit to a local test operator.
func (f *Fixture) InspectRemoval(ctx context.Context, operator, membershipID string) (RemovalAudit, error) {
	if membershipID == "" {
		return RemovalAudit{}, ErrInvalidRequest
	}
	if err := f.authorizeOperator(ctx, operator, "inspect-removal", membershipID); err != nil {
		return RemovalAudit{}, err
	}
	var audit RemovalAudit
	var at string
	err := f.db.QueryRowContext(ctx, `SELECT membership_id, execution_id, actor, occurred_at
		FROM tm_membership_removals WHERE membership_id = ?`, membershipID).
		Scan(&audit.MembershipID, &audit.ExecutionID, &audit.Actor, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return RemovalAudit{}, ErrNotFound
	}
	if err != nil {
		return RemovalAudit{}, fmt.Errorf("read removal audit: %w", err)
	}
	audit.At, err = time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return RemovalAudit{}, fmt.Errorf("decode removal audit time: %w", err)
	}
	return audit, nil
}
