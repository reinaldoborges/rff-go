package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestRegistryUsesExactMachineVersion(t *testing.T) {
	newMachine := func(version string, target state) *fsm.Machine[string, state, string, int] {
		t.Helper()
		machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
			ID: "task", Version: version, InitialStates: []state{target},
			Handlers: map[state]fsm.Handler[string, state, string, int]{
				target: fsm.NoopHandler[string, state, string, int]{},
			},
			SelectInitial: func(int) (fsm.Selection[state], error) {
				return fsm.Selection[state]{Target: target}, nil
			},
			SelectTransition: func(source state, _ string, _ int) (fsm.Selection[state], error) {
				return fsm.Selection[state]{Target: source}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return machine
	}

	var registry fsm.Registry[string, state, string, int]
	if err := registry.Register(newMachine("1", "open")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(newMachine("2", "new")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(newMachine("1", "open")); !errors.Is(err, fsm.ErrDuplicateMachine) {
		t.Fatalf("expected duplicate version error, got %v", err)
	}
	old, err := registry.Lookup("task", "1")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := old.SelectInitial(0)
	if err != nil || selected.Target != "open" {
		t.Fatalf("wrong version selected: %+v, %v", selected, err)
	}
	if _, err := registry.Lookup("task", "3"); !errors.Is(err, fsm.ErrMachineNotFound) {
		t.Fatalf("expected missing version error, got %v", err)
	}
}
