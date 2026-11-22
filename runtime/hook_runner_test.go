package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type entryHandler struct {
	fsm.NoopHandler[string, string, string, int]
	seen  *fsm.ExecutionContext[string, string, string, int]
	calls *int
}

func (h entryHandler) Enter(_ context.Context, execution fsm.ExecutionContext[string, string, string, int]) error {
	*h.seen = execution
	*h.calls++
	return nil
}

func TestHookRunnerRehydratesPinnedInitialExecution(t *testing.T) {
	var seen fsm.ExecutionContext[string, string, string, int]
	var calls int
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, int]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, int]{
			"open": entryHandler{seen: &seen, calls: &calls},
		},
		SelectInitial: func(int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("selector must not run")
		},
		SelectTransition: func(string, string, int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("selector must not run")
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{}, fsm.DispositionIntervention
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	inputRef := transition.CodecVersion{Name: "input", Version: "1"}
	runner, err := transition.NewHookRunner[string, string, string, int](
		&registry,
		func(ref transition.CodecVersion) (transition.Codec[int], error) {
			if ref != inputRef {
				return nil, errors.New("unknown codec")
			}
			return intCodec{}, nil
		},
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	record := transition.Execution[string, string]{
		ID: "execution", Entity: "entity", Key: "request",
		Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
		Target: "open", Input: transition.EncodedValue{Codec: inputRef, Data: []byte("7")},
	}
	attempt := fsm.AttemptMetadata{
		ExecutionID: "execution", Number: 1, LeaseGeneration: 2,
		ActivityID: "activity", ActivityIdempotencyKey: "activity-key",
	}
	result, err := runner.Run(context.Background(), record, transition.HookCall{Kind: transition.HookEnter, Attempt: attempt})
	if err != nil || !result.Success || calls != 1 {
		t.Fatalf("entry hook did not run: result=%+v calls=%d error=%v", result, calls, err)
	}
	if _, ok := seen.Previous(); ok {
		t.Fatal("initial entry unexpectedly has a previous state")
	}
	if _, ok := seen.Event(); ok {
		t.Fatal("initial entry unexpectedly has an event")
	}
	if seen.EntityID() != "entity" || seen.Next() != "open" || seen.Input() != 7 ||
		seen.IdempotencyKey() != "request" || seen.Attempt() != attempt {
		t.Fatalf("hook context differs from persisted snapshot")
	}
}
