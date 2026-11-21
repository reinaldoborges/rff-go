package fsm_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestMachineUsesPinnedFailureClassifier(t *testing.T) {
	raw := errors.New("private upstream error")
	machine, err := fsm.NewMachine(fsm.Config[string, state, string, int]{
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
		ClassifyFailure: func(err error) (fsm.Failure, fsm.Disposition) {
			if err != raw {
				t.Fatal("classifier received the wrong error")
			}
			return fsm.Failure{Code: "dependency", Message: "temporarily unavailable"}, fsm.DispositionRetry
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	failure, disposition, err := machine.ClassifyFailure(raw)
	if err != nil || failure.Code != "dependency" || failure.Message != "temporarily unavailable" || disposition != fsm.DispositionRetry {
		t.Fatalf("unexpected classification: %+v %q %v", failure, disposition, err)
	}
}
