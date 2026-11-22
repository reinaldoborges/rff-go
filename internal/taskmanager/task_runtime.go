package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func (f *Fixture) registerTaskMachine(policy fsm.Policy) error {
	handler := fsm.NoopHandler[string, string, string, fixtureInput]{}
	config := fsm.Config[string, string, string, fixtureInput]{
		ID: "taskmanager-task", Version: "1",
		InitialStates: []string{string(TaskOpen)}, TerminalStates: []string{string(TaskDone), string(TaskCancelled)},
		Handlers: map[string]fsm.Handler[string, string, string, fixtureInput]{
			string(TaskOpen): handler, string(TaskInProgress): handler, string(TaskDone): handler, string(TaskCancelled): handler,
		},
		SelectInitial: func(input fixtureInput) (fsm.Selection[string], error) {
			if input.Task == nil || input.Task.Event != "" {
				return fsm.Selection[string]{}, ErrConflict
			}
			return fsm.Selection[string]{Target: string(TaskOpen), Policy: policy}, nil
		},
		SelectTransition: func(source, event string, input fixtureInput) (fsm.Selection[string], error) {
			if input.Task == nil || event != string(input.Task.Event) {
				return fsm.Selection[string]{}, ErrConflict
			}
			var target TaskStatus
			switch {
			case source == string(TaskOpen) && input.Task.Event == StartTask:
				target = TaskInProgress
			case source == string(TaskInProgress) && input.Task.Event == CompleteTask:
				target = TaskDone
			case (source == string(TaskOpen) || source == string(TaskInProgress)) && input.Task.Event == CancelTask:
				target = TaskCancelled
			default:
				return fsm.Selection[string]{}, ErrConflict
			}
			return fsm.Selection[string]{Target: string(target), Policy: policy}, nil
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "task-hook-failed"}, fsm.DispositionIntervention
		},
	}
	for _, version := range []string{"1", "2"} {
		config.Version = version
		machine, err := fsm.NewMachine(config)
		if err != nil {
			return err
		}
		if err := f.machines.Register(machine); err != nil {
			return err
		}
	}
	return nil
}

type taskAcceptanceStore struct {
	fixture  *Fixture
	input    fixtureInput
	previous string
}

func (s taskAcceptanceStore) Accept(ctx context.Context, entity, key string,
	decide transition.AcceptanceDecisionFunc[string, string],
) (transition.Execution[string, string], bool, error) {
	return s.fixture.acceptInputAtomic(ctx, entity, key, false,
		func(ctx context.Context, tx *sql.Tx, view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if view.Existing != nil {
				return decide(view)
			}
			var authorized bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_memberships m
				JOIN tm_groups g ON g.id = m.group_id AND g.closed = 0
				WHERE m.group_id = ? AND m.user_id = ? AND m.status = 'Active')`,
				s.input.GroupID, s.input.Actor).Scan(&authorized); err != nil {
				return nil, fmt.Errorf("authorize task request: %w", err)
			}
			if !authorized {
				return nil, ErrDenied
			}
			var groupID, status string
			err := tx.QueryRowContext(ctx, `SELECT group_id, status FROM tm_tasks WHERE id = ?`, s.input.Task.ID).Scan(&groupID, &status)
			if s.input.Task.Event == "" {
				if err == nil {
					return nil, ErrConflict
				}
				if !errors.Is(err, sql.ErrNoRows) {
					return nil, fmt.Errorf("check task identity: %w", err)
				}
			} else {
				if errors.Is(err, sql.ErrNoRows) {
					return nil, ErrConflict
				}
				if err != nil {
					return nil, fmt.Errorf("check task state: %w", err)
				}
				if groupID != s.input.GroupID || status != s.previous {
					return nil, ErrConflict
				}
			}
			return decide(view)
		})
}

func (f *Fixture) taskIngress() (*transition.InputIngress[fixtureInput], error) {
	return transition.NewInputIngress[fixtureInput](f.store, "taskmanager-tasks", resolveMembershipInput, f.acceptTask)
}

func (f *Fixture) acceptTask(ctx context.Context, delivery transition.InputDelivery[fixtureInput]) (transition.InputOutcome, error) {
	input := delivery.Value
	if input.Task == nil || delivery.Kind != transition.Command || delivery.WorkflowID != taskEntity(input.Task.ID) {
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "invalid"}}, nil
	}
	entity, err := f.entityForInput(ctx, taskEntityKind, input.Task.ID, delivery.ID)
	if err != nil {
		return transition.InputOutcome{}, err
	}
	var previous string
	if input.Task.Event != "" {
		err := f.db.QueryRowContext(ctx, `SELECT status FROM tm_tasks WHERE id = ?`, input.Task.ID).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return transition.InputOutcome{}, fmt.Errorf("read task state for acceptance: %w", err)
		}
	}

	store := taskAcceptanceStore{fixture: f, input: input, previous: previous}
	acceptor, err := transition.NewAcceptor[string, string, string, fixtureInput](
		store, &f.machines, resolveMembershipInput, resolveMembershipEvent, newExecutionID, f.clock,
	)
	if err != nil {
		return transition.InputOutcome{}, err
	}
	request := transition.Request[string, string, string, fixtureInput]{
		Entity: entity, Key: delivery.ID, MachineID: "taskmanager-task", MachineVersion: "1",
		Input: input, InputCodec: taskInputCodec, Notification: transition.NotifyInherit,
	}
	if input.Task.Event == "" {
		request.Operation = transition.Initialize
	} else {
		request.Operation, request.HasPrevious, request.Previous = transition.Transition, true, previous
		request.HasEvent, request.Event, request.EventCodec = true, string(input.Task.Event), taskEventCodec
	}
	execution, _, err := acceptor.Accept(ctx, request)
	var rejected *inputRejection
	switch {
	case errors.As(err, &rejected):
		return transition.InputOutcome{Failure: &rejected.failure}, nil
	case errors.Is(err, ErrDenied):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "denied"}}, nil
	case errors.Is(err, ErrConflict), errors.Is(err, transition.ErrBusy), errors.Is(err, transition.ErrConflict), errors.Is(err, fsm.ErrTerminalState):
		return transition.InputOutcome{Failure: &fsm.Failure{Code: "conflict"}}, nil
	case err != nil:
		return transition.InputOutcome{}, err
	default:
		return transition.InputOutcome{Receipt: execution.ID}, nil
	}
}
