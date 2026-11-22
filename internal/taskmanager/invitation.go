package taskmanager

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func inputID(actor, group, key string) (string, error) {
	data, err := json.Marshal([]string{actor, group, key})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "taskmanager:" + hex.EncodeToString(sum[:]), nil
}

func (f *Fixture) invitationIngress() (*transition.InputIngress[fixtureInput], error) {
	return transition.NewInputIngress[fixtureInput](f.store, "taskmanager-invitations", resolveMembershipInput, f.acceptInvitation)
}

// Invite durably accepts an owner-authorized invitation initialization.
func (f *Fixture) Invite(ctx context.Context, actor, groupID, userID, membershipID, key string, deadline time.Time) (string, error) {
	if actor == "" || groupID == "" || userID == "" || membershipID == "" || key == "" || deadline.IsZero() {
		return "", ErrInvalidRequest
	}
	var known bool
	if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_users WHERE id = ?)`, actor).Scan(&known); err != nil {
		return "", fmt.Errorf("authenticate invitation actor: %w", err)
	}
	if !known {
		return "", ErrDenied
	}
	id, err := inputID(actor, groupID, key)
	if err != nil {
		return "", err
	}
	ingress, err := f.invitationIngress()
	if err != nil {
		return "", err
	}
	outcome, err := ingress.Submit(ctx, transition.InputRequest[fixtureInput]{
		ID: id, WorkflowID: membershipID, Kind: transition.Command, Codec: membershipInputCodec,
		Value: fixtureInput{Actor: actor, GroupID: groupID, UserID: userID, MembershipID: membershipID, Deadline: deadline.UTC()},
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

// Membership lets the open group's owner or the subject inspect the record.
func (f *Fixture) Membership(ctx context.Context, actor, id string) (Membership, error) {
	if actor == "" || id == "" {
		return Membership{}, ErrInvalidRequest
	}
	membership, owner, closed, err := f.readMembership(ctx, id)
	if err != nil {
		return Membership{}, err
	}
	if closed || actor != owner && actor != membership.UserID {
		return Membership{}, ErrDenied
	}
	return membership, nil
}

func (f *Fixture) readMembership(ctx context.Context, id string) (Membership, string, bool, error) {
	var membership Membership
	var deadline sql.NullString
	var owner string
	var closed bool
	err := f.db.QueryRowContext(ctx, `SELECT m.id, m.group_id, m.user_id, m.status, m.deadline, m.reason, g.owner_id, g.closed
		FROM tm_memberships AS m JOIN tm_groups AS g ON g.id = m.group_id WHERE m.id = ?`, id).
		Scan(&membership.ID, &membership.GroupID, &membership.UserID, &membership.Status, &deadline, &membership.Reason, &owner, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return Membership{}, "", false, ErrNotFound
	}
	if err != nil {
		return Membership{}, "", false, fmt.Errorf("read membership: %w", err)
	}
	if deadline.Valid {
		membership.Deadline, err = time.Parse(time.RFC3339Nano, deadline.String)
		if err != nil {
			return Membership{}, "", false, fmt.Errorf("decode membership deadline: %w", err)
		}
	}
	return membership, owner, closed, nil
}
