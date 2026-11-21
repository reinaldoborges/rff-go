package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

type state string

func TestInitialSelectionUsesDeclaredInitialState(t *testing.T) {
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID:            "task",
		Version:       "1",
		InitialStates: []state{"open"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"open": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(input int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "open", Notify: input > 0}, nil
		},
		SelectTransition: func(source state, event string, input int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: source}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	selected, err := machine.SelectInitial(1)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Target != "open" || !selected.Notify {
		t.Fatalf("unexpected selection: %+v", selected)
	}
}

func TestInitialSelectionRejectsNonInitialTarget(t *testing.T) {
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID:            "task",
		Version:       "1",
		InitialStates: []state{"open"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"open": fsm.NoopHandler[string, state, string, int]{},
			"done": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "done"}, nil
		},
		SelectTransition: func(source state, _ string, _ int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: source}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := machine.SelectInitial(0); !errors.Is(err, fsm.ErrInvalidInitialState) {
		t.Fatalf("expected invalid initial state, got %v", err)
	}
}

func TestTransitionSelectionRejectsUnknownTarget(t *testing.T) {
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID:            "task",
		Version:       "1",
		InitialStates: []state{"open"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"open": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "open"}, nil
		},
		SelectTransition: func(source state, event string, input int) (fsm.Selection[state], error) {
			if source != "open" || event != "finish" || input != 42 {
				t.Fatal("selector did not receive the original source, event, and input")
			}
			return fsm.Selection[state]{Target: "unknown"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Select("open", "finish", 42); !errors.Is(err, fsm.ErrUnknownState) {
		t.Fatalf("expected unknown state, got %v", err)
	}
}
