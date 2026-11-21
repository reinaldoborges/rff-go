package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestTerminalStateRejectsOrdinaryTransitions(t *testing.T) {
	called := false
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID:             "task",
		Version:        "1",
		InitialStates:  []state{"done"},
		TerminalStates: []state{"done"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"done": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "done"}, nil
		},
		SelectTransition: func(state, string, int) (fsm.Selection[state], error) {
			called = true
			return fsm.Selection[state]{Target: "done"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Select("done", "reopen", 0); !errors.Is(err, fsm.ErrTerminalState) {
		t.Fatalf("expected terminal state error, got %v", err)
	}
	if called {
		t.Fatal("terminal transition invoked the selector")
	}
}
