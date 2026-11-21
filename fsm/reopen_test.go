package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestTerminalStateAllowsOnlyExplicitReopening(t *testing.T) {
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID:             "task",
		Version:        "1",
		InitialStates:  []state{"done"},
		TerminalStates: []state{"done"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"done": fsm.NoopHandler[string, state, string, int]{},
			"open": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "done"}, nil
		},
		Reopens: func(source state, event string, input int) bool {
			return source == "done" && event == "reopen" && input == 1
		},
		SelectTransition: func(state, string, int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "open"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Select("done", "other", 1); !errors.Is(err, fsm.ErrTerminalState) {
		t.Fatalf("ordinary transition should be rejected, got %v", err)
	}
	selected, err := machine.Select("done", "reopen", 1)
	if err != nil || selected.Target != "open" {
		t.Fatalf("explicit reopening failed: selection=%+v error=%v", selected, err)
	}
}
