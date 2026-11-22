package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type transitionHandler struct {
	fsm.NoopHandler[string, string, string, int]
	kinds     *[]string
	failure   *fsm.Failure
	execution *fsm.ExecutionContext[string, string, string, int]
}

func (h transitionHandler) Transition(_ context.Context, execution fsm.ExecutionContext[string, string, string, int]) error {
	*h.kinds = append(*h.kinds, "transition")
	*h.execution = execution
	return nil
}

func (h transitionHandler) Retry(_ context.Context, _ fsm.ExecutionContext[string, string, string, int], failure fsm.Failure) error {
	*h.kinds = append(*h.kinds, "retry")
	*h.failure = failure
	return nil
}

func (h transitionHandler) Recover(_ context.Context, _ fsm.ExecutionContext[string, string, string, int], failure fsm.Failure) error {
	*h.kinds = append(*h.kinds, "recover")
	*h.failure = failure
	return nil
}

func TestHookRunnerDispatchesTransitionAndFailureHooks(t *testing.T) {
	var kinds []string
	var failure fsm.Failure
	var seen fsm.ExecutionContext[string, string, string, int]
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, int]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, int]{
			"open": fsm.NoopHandler[string, string, string, int]{},
			"done": transitionHandler{kinds: &kinds, failure: &failure, execution: &seen},
		},
		SelectInitial: func(int) (fsm.Selection[string], error) { return fsm.Selection[string]{}, errors.New("selector reran") },
		SelectTransition: func(string, string, int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, errors.New("selector reran")
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) { return fsm.Failure{}, fsm.DispositionIntervention },
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, int](
		&registry, func(transition.CodecVersion) (transition.Codec[int], error) { return intCodec{}, nil },
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	record := transition.Execution[string, string]{
		ID: "execution", Entity: "entity", Key: "request",
		Operation: transition.Transition, MachineID: "task", MachineVersion: "1",
		Previous: "open", HasPrevious: true, Target: "done",
		Input: transition.EncodedValue{Data: []byte("7")}, Event: transition.EncodedValue{Data: []byte("finish")},
	}
	attempt := fsm.AttemptMetadata{
		ExecutionID: "execution", Number: 1, LeaseGeneration: 1,
		ActivityID: "activity", ActivityIdempotencyKey: "activity-key",
	}
	priorFailure := fsm.Failure{Code: "retryable", Message: "safe"}
	for _, call := range []transition.HookCall{
		{Kind: transition.HookTransition, Attempt: attempt},
		{Kind: transition.HookRetry, Attempt: attempt, Failure: &priorFailure},
		{Kind: transition.HookRecover, Attempt: attempt, Failure: &priorFailure},
	} {
		if result, err := runner.Run(context.Background(), record, call); err != nil || !result.Success {
			t.Fatalf("hook %s failed: result=%+v error=%v", call.Kind, result, err)
		}
	}
	previous, previousOK := seen.Previous()
	event, eventOK := seen.Event()
	if previous != "open" || !previousOK || event != "finish" || !eventOK || seen.Next() != "done" || seen.Input() != 7 {
		t.Fatal("transition hook did not receive the accepted snapshot")
	}
	if len(kinds) != 3 || kinds[0] != "transition" || kinds[1] != "retry" || kinds[2] != "recover" || failure != priorFailure {
		t.Fatalf("wrong hooks or failure envelope: hooks=%v failure=%+v", kinds, failure)
	}
}
