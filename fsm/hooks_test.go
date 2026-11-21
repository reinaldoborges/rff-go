package fsm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

type captureHandler struct {
	fsm.NoopHandler[string, state, string, int]
	got fsm.ExecutionContext[string, state, string, int]
}

func (h *captureHandler) Enter(_ context.Context, execution fsm.ExecutionContext[string, state, string, int]) error {
	h.got = execution
	return nil
}

func TestHookContextDistinguishesAbsenceFromZeroValues(t *testing.T) {
	handler := &captureHandler{}
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID: "task", Version: "1", InitialStates: []state{""},
		Handlers: map[state]fsm.Handler[string, state, string, int]{"": handler},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: ""}, nil
		},
		SelectTransition: func(source state, _ string, _ int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: source}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	initial, err := fsm.NewExecutionContext("entity", fsm.None[state](), state(""), fsm.None[string](), 7, "key", fsm.AttemptMetadata{Number: 1})
	if err != nil {
		t.Fatal(err)
	}
	selectedHandler, err := machine.HandlerFor("")
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedHandler.Enter(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	if _, present := handler.got.Previous(); present {
		t.Fatal("initial context has a previous state")
	}
	if _, present := handler.got.Event(); present {
		t.Fatal("initial context has an event")
	}
	if handler.got.EntityID() != "entity" || handler.got.Input() != 7 || handler.got.IdempotencyKey() != "key" || handler.got.Attempt().Number != 1 {
		t.Fatal("hook context lost accepted execution data")
	}

	ordinary, err := fsm.NewExecutionContext("entity", fsm.Some(state("")), state(""), fsm.Some(""), 0, "key", fsm.AttemptMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	if value, present := ordinary.Previous(); !present || value != "" {
		t.Fatal("zero-valued previous state was treated as absent")
	}
	if value, present := ordinary.Event(); !present || value != "" {
		t.Fatal("zero-valued event was treated as absent")
	}
	if _, err := fsm.NewExecutionContext("entity", fsm.Some(state("")), state(""), fsm.None[string](), 0, "key", fsm.AttemptMetadata{}); !errors.Is(err, fsm.ErrInvalidContext) {
		t.Fatalf("mismatched previous/event should be rejected: %v", err)
	}
}
