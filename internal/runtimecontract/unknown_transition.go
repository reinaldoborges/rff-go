package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type unknownTransitionClassifier struct{}

func (unknownTransitionClassifier) ClassifyHookFailure(_ transition.Execution[string, string], err error) (transition.HookResult, error) {
	if !errors.Is(err, transition.ErrAttemptUnknown) {
		return transition.HookResult{}, err
	}
	return transition.HookResult{Failure: fsm.Failure{Code: "unknown"}, Disposition: fsm.DispositionIntervention}, nil
}

func runUnknownTransitionRetry(t *testing.T, newStore func(*testing.T) ControlStore) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Transition, MachineID: "machine", MachineVersion: "1",
			Previous: "open", HasPrevious: true, Target: "done",
			Input:  transition.EncodedValue{Codec: ref, Data: []byte("payload")},
			Event:  transition.EncodedValue{Codec: ref, Data: []byte("finish")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "1", InitialStates: []string{"open"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{
			"open": fsm.NoopHandler[string, string, string, string]{},
			"done": fsm.NoopHandler[string, string, string, string]{},
		},
		SelectInitial: func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
			return fsm.Selection[string]{}, nil
		},
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "unknown"}, fsm.DispositionIntervention
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownTransitionClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	lost, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, call, err := store.InspectHook(ctx, lost, clock)
	if err != nil || call.Kind != transition.HookTransition {
		t.Fatalf("expected transition work: %+v, %v", call, err)
	}
	first, err := store.StartHook(ctx, lost, clock)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	for range 2 {
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	record, err := store.InspectExecution(ctx, "execution")
	if err != nil || record.Status != transition.NeedsIntervention {
		t.Fatalf("unknown transition did not require intervention: %+v, %v", record, err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	authorized := false
	controls, err := transition.NewAuthorizedHookControls(store, runner, func(_ context.Context, authorization transition.ControlAuthorization) error {
		if authorization.ExecutionID != "execution" || authorization.RequestID != "retry" || authorization.Action != transition.ControlRetry {
			return errors.New("unexpected control authorization")
		}
		authorized = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controls.Hook(ctx, lease, clock, transition.HookControl{ID: "retry", Action: transition.RetryIntervention}); err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("retry bypassed authorization")
	}
	retry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, call, err = store.InspectHook(ctx, retry, clock)
	if err != nil || call.Kind != transition.HookRetry {
		t.Fatalf("authorized retry did not dispatch Retry first: %+v, %v", call, err)
	}
	if _, err := store.StartHook(ctx, retry, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, retry, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, call, err = store.InspectHook(ctx, resumed, clock)
	if err != nil || call.Kind != transition.HookTransition || resumed.ActivityID != lost.ActivityID || resumed.Generation <= lost.Generation {
		t.Fatalf("transition was not redelivered under its original identity: %+v call=%+v error=%v", resumed, call, err)
	}
	second, err := store.StartHook(ctx, resumed, clock)
	if err != nil || second.Attempt.Number <= first.Attempt.Number ||
		second.Attempt.ActivityIdempotencyKey != first.Attempt.ActivityIdempotencyKey {
		t.Fatalf("redelivery changed attempt identity: first=%+v second=%+v error=%v", first, second, err)
	}
}
