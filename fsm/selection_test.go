package fsm_test

import (
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestSelectionPreservesPolicyAndSelectorErrors(t *testing.T) {
	denied := errors.New("event is not allowed")
	delays := []time.Duration{time.Second, time.Minute}
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
		ID: "task", Version: "1", InitialStates: []state{"open"},
		Handlers: map[state]fsm.Handler[string, state, string, int]{
			"open": fsm.NoopHandler[string, state, string, int]{},
			"done": fsm.NoopHandler[string, state, string, int]{},
		},
		SelectInitial: func(int) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: "open"}, nil
		},
		SelectTransition: func(_ state, event string, _ int) (fsm.Selection[state], error) {
			if event != "finish" {
				return fsm.Selection[state]{}, denied
			}
			return fsm.Selection[state]{
				Target: "done", Notify: true,
				Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Second, OverallTimeout: time.Hour, RetryDelays: delays},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Select("open", "invalid", 0); !errors.Is(err, denied) {
		t.Fatalf("selector rejection was lost: %v", err)
	}
	selected, err := machine.Select("open", "finish", 0)
	if err != nil {
		t.Fatal(err)
	}
	delays[0] = 9 * time.Second
	if selected.Target != "done" || !selected.Notify || selected.Policy.MaxAttempts != 3 ||
		selected.Policy.AttemptTimeout != time.Second || selected.Policy.OverallTimeout != time.Hour ||
		selected.Policy.RetryDelays[0] != time.Second {
		t.Fatalf("selection was not preserved: %+v", selected)
	}
}
