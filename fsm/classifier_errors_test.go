package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestUnavailableOrInvalidFailureClassifierIsExplicit(t *testing.T) {
	config := fsm.Config[string, state, string, int]{
		ID: "task", Version: "1", InitialStates: []state{"open"},
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
	machine, err := fsm.NewMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := machine.ClassifyFailure(errors.New("failed")); !errors.Is(err, fsm.ErrClassifierUnavailable) {
		t.Fatalf("expected unavailable classifier, got %v", err)
	}
	config.ClassifyFailure = func(error) (fsm.Failure, fsm.Disposition) {
		return fsm.Failure{}, "unsupported"
	}
	machine, err = fsm.NewMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := machine.ClassifyFailure(errors.New("failed")); !errors.Is(err, fsm.ErrInvalidClassification) {
		t.Fatalf("expected invalid classification, got %v", err)
	}
}
