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

type InvitationDecision string

const (
	AcceptInvitation  InvitationDecision = "accept"
	DeclineInvitation InvitationDecision = "decline"
	RevokeInvitation  InvitationDecision = "revoke"
	removeMembership  InvitationDecision = "remove"
	expireInvitation  InvitationDecision = "expire"
)

type decisionAcceptanceStore struct {
	fixture *Fixture
	input   fixtureInput
}

func (s decisionAcceptanceStore) Accept(ctx context.Context, entity, key string,
	decide transition.AcceptanceDecisionFunc[string, string],
) (transition.Execution[string, string], bool, error) {
	return s.fixture.store.AcceptAtomic(ctx, entity, key,
		func(ctx context.Context, tx *sql.Tx, view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if view.Existing != nil {
				return decide(view)
			}
			var groupID, userID, status, owner string
			var deadline sql.NullString
			var closed bool
			err := tx.QueryRowContext(ctx, `SELECT m.group_id, m.user_id, m.status, m.deadline, g.owner_id, g.closed
				FROM tm_memberships AS m JOIN tm_groups AS g ON g.id = m.group_id WHERE m.id = ?`, s.input.MembershipID).
				Scan(&groupID, &userID, &status, &deadline, &owner, &closed)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrNotFound
			}
			if err != nil {
				return nil, fmt.Errorf("authorize invitation decision: %w", err)
			}
			authorized := s.input.Actor == userID
			if s.input.Event == RevokeInvitation || s.input.Event == removeMembership {
				authorized = s.input.Actor == owner
			} else if s.input.Event == expireInvitation {
				authorized = s.input.Actor == ""
			}
			if closed || !authorized {
				return nil, ErrDenied
			}
			if s.input.Event == removeMembership {
				if status != string(Active) || groupID != s.input.GroupID || userID != s.input.UserID || userID == owner {
					return nil, ErrConflict
				}
				var assigned bool
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_tasks WHERE group_id = ? AND assignee = ?)`,
					groupID, s.input.MembershipID).Scan(&assigned); err != nil {
					return nil, fmt.Errorf("check member task assignments: %w", err)
				}
				if assigned {
					return nil, ErrConflict
				}
				return decide(view)
			}
			if !deadline.Valid {
				return nil, ErrConflict
			}
			expiry, err := time.Parse(time.RFC3339Nano, deadline.String)
			if err != nil {
				return nil, fmt.Errorf("decode membership deadline: %w", err)
			}
			now := s.fixture.clock()
			if now.IsZero() {
				return nil, transition.ErrInvalidLease
			}
			due := !now.Before(expiry)
			if status != string(Invited) || groupID != s.input.GroupID || userID != s.input.UserID ||
				!expiry.Equal(s.input.Deadline) || (s.input.Event == expireInvitation) != due {
				return nil, ErrConflict
			}
			return decide(view)
		})
}

func (f *Fixture) decisionIngress() (*transition.InputIngress[fixtureInput], error) {
	return transition.NewInputIngress[fixtureInput](f.store, "taskmanager-decisions", resolveMembershipInput, f.acceptDecision)
}

func (f *Fixture) acceptDecision(ctx context.Context, delivery transition.InputDelivery[fixtureInput]) (transition.InputOutcome, error) {
	input := delivery.Value
	if delivery.WorkflowID != input.MembershipID || (delivery.Kind != transition.HumanDecision && delivery.Kind != transition.ExternalEvent) {
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "invalid"}}, nil
	}
	if input.Event == expireInvitation && (input.Actor != "" || delivery.ID != expiryTimerID(input.MembershipID) ||
		delivery.Kind != transition.ExternalEvent) {
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "invalid"}}, nil
	}
	entity, err := f.entityForInput(ctx, membershipEntityKind, input.MembershipID, delivery.ID)
	if err != nil {
		return transition.InputOutcome{}, err
	}
	store := decisionAcceptanceStore{fixture: f, input: input}
	acceptor, err := transition.NewAcceptor[string, string, string, fixtureInput](
		store, &f.machines, resolveMembershipInput, resolveMembershipEvent, newExecutionID, f.clock,
	)
	if err != nil {
		return transition.InputOutcome{}, err
	}
	previous := string(Invited)
	if input.Event == removeMembership {
		previous = string(Active)
	}
	execution, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, fixtureInput]{
		Entity: entity, Key: delivery.ID, Operation: transition.Transition, MachineID: "taskmanager-membership", MachineVersion: "1",
		HasPrevious: true, Previous: previous, HasEvent: true, Event: string(input.Event), EventCodec: membershipEventCodec,
		Input: input, InputCodec: membershipInputCodec, Notification: transition.NotifyInherit,
	})
	switch {
	case errors.Is(err, transition.ErrBusy) && input.Event == expireInvitation:
		return transition.InputOutcome{}, err
	case errors.Is(err, ErrDenied):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "denied"}}, nil
	case errors.Is(err, ErrConflict), errors.Is(err, transition.ErrBusy), errors.Is(err, transition.ErrConflict), errors.Is(err, fsm.ErrTerminalState):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "conflict"}}, nil
	case errors.Is(err, ErrNotFound):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "not-found"}}, nil
	case err != nil:
		return transition.InputOutcome{}, err
	default:
		return transition.InputOutcome{Receipt: execution.ID}, nil
	}
}

// Decide accepts an invitee decision or owner revocation as a typed input.
func (f *Fixture) Decide(ctx context.Context, actor, membershipID, key string, decision InvitationDecision) (string, error) {
	if actor == "" || membershipID == "" || key == "" ||
		(decision != AcceptInvitation && decision != DeclineInvitation && decision != RevokeInvitation) {
		return "", ErrInvalidRequest
	}
	var known bool
	if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_users WHERE id = ?)`, actor).Scan(&known); err != nil {
		return "", fmt.Errorf("authenticate decision actor: %w", err)
	}
	if !known {
		return "", ErrDenied
	}
	var input fixtureInput
	var deadline sql.NullString
	var status MembershipStatus
	var owner string
	var closed bool
	err := f.db.QueryRowContext(ctx, `SELECT m.group_id, m.user_id, m.deadline, m.status, g.owner_id, g.closed
		FROM tm_memberships m JOIN tm_groups g ON g.id = m.group_id WHERE m.id = ?`, membershipID).
		Scan(&input.GroupID, &input.UserID, &deadline, &status, &owner, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read decision membership: %w", err)
	}
	authorized := actor == input.UserID && status != Removed
	if decision == RevokeInvitation {
		authorized = actor == owner
	}
	if closed || !authorized {
		return "", ErrDenied
	}
	if !deadline.Valid {
		return "", ErrConflict
	}
	input.Deadline, err = time.Parse(time.RFC3339Nano, deadline.String)
	if err != nil {
		return "", fmt.Errorf("decode membership deadline: %w", err)
	}
	input.Actor, input.MembershipID, input.Event = actor, membershipID, decision
	id, err := inputID(actor, input.GroupID, key)
	if err != nil {
		return "", err
	}
	kind := transition.HumanDecision
	if decision == RevokeInvitation {
		kind = transition.ExternalEvent
	}
	ingress, err := f.decisionIngress()
	if err != nil {
		return "", err
	}
	outcome, err := ingress.Submit(ctx, transition.InputRequest[fixtureInput]{
		ID: id, WorkflowID: membershipID, Kind: kind, Codec: membershipInputCodec, Value: input,
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
		case "not-found":
			return "", ErrNotFound
		default:
			return "", ErrInvalidRequest
		}
	}
	return outcome.Receipt, nil
}
