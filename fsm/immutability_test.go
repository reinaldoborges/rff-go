package fsm_test

import (
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestMachineRetainsItsRegisteredStates(t *testing.T) {
	initial := []state{"open"}
	handlers := map[state]fsm.Handler[string, state, string, int]{
		"open": fsm.NoopHandler[string, state, string, int]{},
	}
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID:            "task",
		Version:       "1",
		InitialStates: initial,
		Handlers:      handlers,
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "open"}, nil
		},
		SelectTransition: func(source state, _ string, _ int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: source}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	initial[0] = "changed"
	delete(handlers, "open")
	if _, err := machine.SelectInitial(0); err != nil {
		t.Fatalf("initial registration changed: %v", err)
	}
	if _, err := machine.Select("open", "", 0); err != nil {
		t.Fatalf("state registration changed: %v", err)
	}
}
