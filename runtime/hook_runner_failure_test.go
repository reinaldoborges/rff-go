package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type errorHandler struct {
	fsm.NoopHandler[string, string, string, int]
	enter func(context.Context) error
}

func (h errorHandler) Enter(ctx context.Context, _ fsm.ExecutionContext[string, string, string, int]) error {
	return h.enter(ctx)
}

func newFailingHookRunner(t *testing.T, handler errorHandler) (*transition.HookRunner[string, string, string, int], transition.Execution[string, string], transition.HookCall) {
	return newHookRunnerWithCodec(t, handler, func(transition.CodecVersion) (transition.Codec[int], error) {
		return intCodec{}, nil
	})
}

func newHookRunnerWithCodec(t *testing.T, handler errorHandler, input transition.CodecResolver[int]) (*transition.HookRunner[string, string, string, int], transition.Execution[string, string], transition.HookCall) {
	t.Helper()
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, int]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers:      map[string]fsm.Handler[string, string, string, int]{"open": handler},
		SelectInitial: func(int) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition: func(string, string, int) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, nil
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "safe"}, fsm.DispositionRetry
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, int]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, int](
		&registry,
		input,
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	record := transition.Execution[string, string]{
		ID: "execution", Entity: "entity", Key: "request",
		Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
		Target: "open", Input: transition.EncodedValue{Data: []byte("7")},
	}
	call := transition.HookCall{Kind: transition.HookEnter, Attempt: fsm.AttemptMetadata{
		ExecutionID: "execution", Number: 1, LeaseGeneration: 1,
		ActivityID: "activity", ActivityIdempotencyKey: "activity-key",
	}}
	return runner, record, call
}

func TestHookRunnerMapsLiveFailureToSafeData(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{
		enter: func(context.Context) error { return errors.New("secret application error") },
	})
	result, err := runner.Run(context.Background(), record, call)
	if err != nil || result.Success || result.Failure.Code != "safe" || result.Disposition != fsm.DispositionRetry {
		t.Fatalf("failure not classified safely: result=%+v error=%v", result, err)
	}
}

func TestCancelledHookCannotReportSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner, record, call := newFailingHookRunner(t, errorHandler{
		enter: func(context.Context) error {
			cancel()
			return nil
		},
	})
	result, err := runner.Run(ctx, record, call)
	if !errors.Is(err, context.Canceled) || result.Success {
		t.Fatalf("cancelled attempt reported a result: result=%+v error=%v", result, err)
	}
}
