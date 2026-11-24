package fsm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

func TestContinuationBehaviorIsVersionedAndExplicit(t *testing.T) {
	config := fsm.Config[string, string, string, int]{
		ID: "plan", Version: "v1", InitialStates: []string{"due"},
		Handlers: map[string]fsm.Handler[string, string, string, int]{
			"due": fsm.NoopHandler[string, string, string, int]{},
			"end": fsm.NoopHandler[string, string, string, int]{},
		},
		ContinuationPolicies: map[string]fsm.Policy{
			"due": {MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
		},
		SelectInitial: func(int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due"}, nil
		},
		SelectTransition: func(string, string, int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "end"}, nil
		},
	}

	missing, err := fsm.NewMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.EvaluateContinuation(context.Background(), "plan-1", "due"); !errors.Is(err, fsm.ErrContinuationBehaviorUnavailable) {
		t.Fatalf("missing behavior must block: %v", err)
	}
	config.Continue = func(_ context.Context, entity, state string) (fsm.ContinuationDecision[string, int], error) {
		if entity != "plan-1" || state != "due" {
			t.Fatalf("unexpected continuation context: %q %q", entity, state)
		}
		return fsm.ContinuationDecision[string, int]{Continue: true, Event: "charge", Input: 42}, nil
	}
	machine, err := fsm.NewMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Lookup("plan", "v2"); !errors.Is(err, fsm.ErrMachineNotFound) {
		t.Fatalf("missing version must not use a different behavior: %v", err)
	}
	machine, err = registry.Lookup("plan", "v1")
	if err != nil {
		t.Fatal(err)
	}
	decision, err := machine.EvaluateContinuation(context.Background(), "plan-1", "due")
	if err != nil || !decision.Continue || decision.Event != "charge" || decision.Input != 42 {
		t.Fatalf("continuation decision: %+v, %v", decision, err)
	}
	if _, err := machine.EvaluateContinuation(context.Background(), "plan-1", "end"); !errors.Is(err, fsm.ErrContinuationNotEnabled) {
		t.Fatalf("non-opted-in state must not evaluate: %v", err)
	}
}

func TestContinuationBehaviorCanStopOrReportFailure(t *testing.T) {
	failure := errors.New("durable data temporarily unavailable")
	config := fsm.Config[string, string, string, int]{
		ID: "plan", Version: "v1", InitialStates: []string{"due"},
		Handlers: map[string]fsm.Handler[string, string, string, int]{
			"due": fsm.NoopHandler[string, string, string, int]{},
		},
		ContinuationPolicies: map[string]fsm.Policy{
			"due": {MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
		},
		SelectInitial: func(int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due"}, nil
		},
		SelectTransition: func(string, string, int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{Target: "due"}, nil
		},
		Continue: func(_ context.Context, entity, _ string) (fsm.ContinuationDecision[string, int], error) {
			if entity == "unavailable" {
				return fsm.ContinuationDecision[string, int]{}, failure
			}
			return fsm.ContinuationDecision[string, int]{}, nil
		},
	}
	machine, err := fsm.NewMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := machine.EvaluateContinuation(context.Background(), "done", "due"); err != nil || decision.Continue {
		t.Fatalf("no-continuation decision: %+v, %v", decision, err)
	}
	if _, err := machine.EvaluateContinuation(context.Background(), "unavailable", "due"); !errors.Is(err, failure) {
		t.Fatalf("evaluation failure was not preserved: %v", err)
	}
}
