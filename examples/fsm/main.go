// Command fsm demonstrates pure state-machine selection without a runtime.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

type state string
type event string

const (
	open   state = "open"
	done   state = "done"
	finish event = "finish"
)

func run() error {
	policy := fsm.Policy{
		MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute,
	}
	machine, err := fsm.NewMachine(fsm.Config[string, state, event, struct{}]{
		ID:             "example-task",
		Version:        "1",
		InitialStates:  []state{open},
		TerminalStates: []state{done},
		Handlers: map[state]fsm.Handler[string, state, event, struct{}]{
			open: fsm.NoopHandler[string, state, event, struct{}]{},
			done: fsm.NoopHandler[string, state, event, struct{}]{},
		},
		SelectInitial: func(struct{}) (fsm.Selection[state], error) {
			return fsm.Selection[state]{Target: open, Policy: policy}, nil
		},
		SelectTransition: func(source state, received event, _ struct{}) (fsm.Selection[state], error) {
			if source != open || received != finish {
				return fsm.Selection[state]{}, fmt.Errorf("event %q is invalid from %q", received, source)
			}
			return fsm.Selection[state]{Target: done, Policy: policy}, nil
		},
	})
	if err != nil {
		return err
	}
	initial, err := machine.SelectInitial(struct{}{})
	if err != nil {
		return err
	}
	transition, err := machine.Select(initial.Target, finish, struct{}{})
	if err != nil {
		return err
	}
	fmt.Printf("%s -> %s\n", initial.Target, transition.Target)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
