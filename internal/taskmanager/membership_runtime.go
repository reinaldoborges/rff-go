package taskmanager

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type MembershipStatus string

const (
	Invited  MembershipStatus = "Invited"
	Active   MembershipStatus = "Active"
	Declined MembershipStatus = "Declined"
	Expired  MembershipStatus = "Expired"
	Revoked  MembershipStatus = "Revoked"
	Removed  MembershipStatus = "Removed"
)

type Membership struct {
	ID       string
	GroupID  string
	UserID   string
	Status   MembershipStatus
	Deadline time.Time
	Reason   string
}

type fixtureInput struct {
	Actor        string
	GroupID      string
	UserID       string
	MembershipID string
	Deadline     time.Time
	Event        InvitationDecision
	Task         *taskInput `json:",omitempty"`
}

type jsonCodec[T any] struct{}

func (jsonCodec[T]) Encode(_ context.Context, value T) ([]byte, error) { return json.Marshal(value) }
func (jsonCodec[T]) Decode(_ context.Context, data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}
func (c jsonCodec[T]) Equal(ctx context.Context, data []byte, incoming T) (bool, error) {
	stored, err := c.Decode(ctx, data)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(stored, incoming), nil
}

var membershipInputCodec = transition.CodecVersion{Name: "taskmanager-membership-input", Version: "1"}
var membershipEventCodec = transition.CodecVersion{Name: "taskmanager-membership-event", Version: "1"}
var taskInputCodec = transition.CodecVersion{Name: "taskmanager-task-input", Version: "1"}
var taskEventCodec = transition.CodecVersion{Name: "taskmanager-task-event", Version: "1"}

func resolveMembershipInput(version transition.CodecVersion) (transition.Codec[fixtureInput], error) {
	if version != membershipInputCodec && version != taskInputCodec {
		return nil, errors.New("unknown membership input codec")
	}
	return jsonCodec[fixtureInput]{}, nil
}

func resolveMembershipEvent(version transition.CodecVersion) (transition.Codec[string], error) {
	if version != membershipEventCodec && version != taskEventCodec {
		return nil, errors.New("unknown membership event codec")
	}
	return jsonCodec[string]{}, nil
}

func (f *Fixture) initializeMembershipRuntime() error {
	policy := fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{time.Second}}
	handler := fsm.NoopHandler[string, string, string, fixtureInput]{}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, fixtureInput]{
		ID: "taskmanager-membership", Version: "1",
		InitialStates:  []string{string(Invited)},
		TerminalStates: []string{string(Declined), string(Expired), string(Revoked), string(Removed)},
		Handlers: map[string]fsm.Handler[string, string, string, fixtureInput]{
			string(Invited): handler, string(Active): handler, string(Declined): handler,
			string(Expired): handler, string(Revoked): handler, string(Removed): handler,
		},
		SelectInitial: func(fixtureInput) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: string(Invited), Policy: policy, Notify: true}, nil
		},
		SelectTransition: func(source, event string, input fixtureInput) (fsm.Selection[string], error) {
			if event != string(input.Event) {
				return fsm.Selection[string]{}, ErrConflict
			}
			if source == string(Active) && input.Event == removeMembership {
				return fsm.Selection[string]{Target: string(Removed), Policy: policy}, nil
			}
			if source != string(Invited) {
				return fsm.Selection[string]{}, ErrConflict
			}
			var target MembershipStatus
			switch input.Event {
			case AcceptInvitation:
				target = Active
			case DeclineInvitation:
				target = Declined
			case RevokeInvitation:
				target = Revoked
			case expireInvitation:
				target = Expired
			default:
				return fsm.Selection[string]{}, ErrConflict
			}
			return fsm.Selection[string]{Target: string(target), Policy: policy}, nil
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "membership-hook-failed"}, fsm.DispositionIntervention
		},
	})
	if err != nil {
		return err
	}
	if err := f.machines.Register(machine); err != nil {
		return err
	}
	if err := f.registerTaskMachine(policy); err != nil {
		return err
	}
	f.runner, err = transition.NewHookRunner[string, string, string, fixtureInput](&f.machines, resolveMembershipInput, resolveMembershipEvent)
	return err
}

type invitationAcceptanceStore struct {
	fixture *Fixture
	input   fixtureInput
}

func (s invitationAcceptanceStore) Accept(ctx context.Context, entity, key string,
	decide transition.AcceptanceDecisionFunc[string, string],
) (transition.Execution[string, string], bool, error) {
	return s.fixture.store.AcceptAtomic(ctx, entity, key,
		func(ctx context.Context, tx *sql.Tx, view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if view.Existing != nil {
				return decide(view)
			}
			var authorized bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
				SELECT 1 FROM tm_groups AS g JOIN tm_users AS u ON u.id = ?
				WHERE g.id = ? AND g.owner_id = ? AND g.closed = 0
			)`, s.input.UserID, s.input.GroupID, s.input.Actor).Scan(&authorized); err != nil {
				return nil, fmt.Errorf("authorize invitation: %w", err)
			}
			if !authorized {
				return nil, ErrDenied
			}
			if !s.fixture.clock().Before(s.input.Deadline) {
				return nil, ErrConflict
			}
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
				SELECT 1 FROM tm_memberships WHERE id = ? OR
				(group_id = ? AND user_id = ? AND status IN ('Invited', 'Active'))
			)`, s.input.MembershipID, s.input.GroupID, s.input.UserID).Scan(&exists); err != nil {
				return nil, fmt.Errorf("check invitation membership: %w", err)
			}
			if exists {
				return nil, ErrConflict
			}
			return decide(view)
		})
}

func newExecutionID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return "membership:" + hex.EncodeToString(id[:]), nil
}

func (f *Fixture) acceptInvitation(ctx context.Context, delivery transition.InputDelivery[fixtureInput]) (transition.InputOutcome, error) {
	input := delivery.Value
	if delivery.Kind != transition.Command || delivery.WorkflowID != input.MembershipID {
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "invalid"}}, nil
	}
	entity, err := f.entityForInput(ctx, membershipEntityKind, input.MembershipID, delivery.ID)
	if err != nil {
		return transition.InputOutcome{}, err
	}
	store := invitationAcceptanceStore{fixture: f, input: input}
	acceptor, err := transition.NewAcceptor[string, string, string, fixtureInput](
		store, &f.machines, resolveMembershipInput, resolveMembershipEvent, newExecutionID, f.clock,
	)
	if err != nil {
		return transition.InputOutcome{}, err
	}
	execution, _, err := acceptor.Accept(ctx, transition.Request[string, string, string, fixtureInput]{
		Entity: entity, Key: delivery.ID, Operation: transition.Initialize,
		MachineID: "taskmanager-membership", MachineVersion: "1",
		Input: input, InputCodec: membershipInputCodec, Notification: transition.NotifyInherit,
	})
	switch {
	case errors.Is(err, ErrDenied):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "denied"}}, nil
	case errors.Is(err, ErrConflict), errors.Is(err, transition.ErrBusy), errors.Is(err, transition.ErrConflict):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "conflict"}}, nil
	case err != nil:
		return transition.InputOutcome{}, err
	default:
		return transition.InputOutcome{Receipt: execution.ID}, nil
	}
}
