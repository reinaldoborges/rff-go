package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestMachineRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fsm.Config[string, state, string, int])
	}{
		{"missing ID", func(c *fsm.Config[string, state, string, int]) { c.ID = "" }},
		{"missing version", func(c *fsm.Config[string, state, string, int]) { c.Version = "" }},
		{"no initial state", func(c *fsm.Config[string, state, string, int]) { c.InitialStates = nil }},
		{"unregistered initial state", func(c *fsm.Config[string, state, string, int]) { c.InitialStates = []state{"missing"} }},
		{"missing handler", func(c *fsm.Config[string, state, string, int]) { c.Handlers["open"] = nil }},
		{"typed nil handler", func(c *fsm.Config[string, state, string, int]) {
			var handler *captureHandler
			c.Handlers["open"] = handler
		}},
		{"missing initial selector", func(c *fsm.Config[string, state, string, int]) { c.SelectInitial = nil }},
		{"missing transition selector", func(c *fsm.Config[string, state, string, int]) { c.SelectTransition = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := fsm.Config[string, state, string, int]{
				ID:            "task",
				Version:       "1",
				InitialStates: []state{"open"},
				Handlers: map[state]fsm.Handler[string, state, string, int]{
					"open": fsm.NoopHandler[string, state, string, int]{},
				},
				SelectInitial: func(int) (fsm.Selection[state], error) {
					return fsm.Selection[state]{Target: "open"}, nil
				},
				SelectTransition: func(source state, _ string, _ int) (fsm.Selection[state], error) {
					return fsm.Selection[state]{Target: source}, nil
				},
			}
			tt.change(&config)
			if _, err := fsm.NewMachine(config); !errors.Is(err, fsm.ErrInvalidMachine) {
				t.Fatalf("expected invalid machine, got %v", err)
			}
		})
	}
}
