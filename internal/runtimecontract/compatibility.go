package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// CompatibilityStore combines runtime ports needed for compatibility checks.
type CompatibilityStore interface {
	Store
	transition.CompatibilityStore[string, string]
	transition.CheckedHookControlStore[string, string]
}

// RunCompatibility checks isolation and restoration of pinned behavior.
func RunCompatibility(t *testing.T, newStore func(*testing.T) CompatibilityStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &timerClock{now: now}
	calls := 0
	var registry fsm.Registry[string, string, string, string]
	register := func(version string) {
		t.Helper()
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "machine", Version: version, InitialStates: []string{"open"},
			Handlers:         map[string]fsm.Handler[string, string, string, string]{"open": handler{calls: &calls}},
			SelectInitial:    func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
			SelectTransition: func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
			ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
				return fsm.Failure{Code: "failed"}, fsm.DispositionIntervention
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(machine); err != nil {
			t.Fatal(err)
		}
	}
	register("2")
	for _, item := range []struct{ id, version string }{{"a", "1"}, {"b", "2"}} {
		_, _, err := store.Accept(ctx, item.id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: item.id, Entity: item.id, Key: "key",
				Operation: transition.Initialize, MachineID: "machine", MachineVersion: item.version, Target: "open",
				Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.AcquireTransitionLease(ctx, item.id, item.id, "coordinator", clock.Now, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, lease, clock.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable machine was not reported: %v", err)
	}
	blocked, err := store.ReadActiveExecution(ctx, "a")
	if err != nil || blocked.Status != transition.Waiting || blocked.Compatibility == nil ||
		blocked.Compatibility.Component != transition.CompatibilityMachine || blocked.Compatibility.Version != "1" {
		t.Fatalf("missing safe compatibility diagnostic: %+v, %v", blocked, err)
	}
	progress, err := store.ReadHookProgress(ctx, "a")
	if err != nil || len(progress.Attempts) != 0 {
		t.Fatalf("incompatible behavior consumed an attempt: %+v, %v", progress, err)
	}
	if err := worker.RunOne(ctx); err != nil || calls != 1 {
		t.Fatalf("incompatible execution blocked independent work: calls=%d error=%v", calls, err)
	}
	manager, err := transition.NewCompatibilityCoordinator(store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	stillBlocked, err := store.ReadActiveExecution(ctx, "a")
	if err != nil || stillBlocked.Compatibility == nil {
		t.Fatalf("missing behavior cleared its block: %+v, %v", stillBlocked, err)
	}
	register("1")
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := store.ReadActiveExecution(ctx, "a")
	if err != nil || restored.Compatibility != nil || restored.Status != transition.Waiting ||
		!restored.Deadline.Equal(blocked.Deadline) {
		t.Fatalf("restoration changed phase or deadline: %+v, %v", restored, err)
	}
	if err := worker.RunOne(ctx); err != nil || calls != 2 {
		t.Fatalf("restored execution did not proceed: calls=%d error=%v", calls, err)
	}
	_, _, err = store.Accept(ctx, "c", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "c", Entity: "c", Key: "key",
			Operation: transition.Initialize, MachineID: "machine", MachineVersion: "3", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "c", "c", "coordinator", clock.Now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock.Now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock.Now); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{
		Failure: fsm.Failure{Code: "failed"}, Disposition: fsm.DispositionIntervention,
	}, clock.Now); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterHookIntervention(ctx, lease, clock.Now); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable intervention behavior was not reported: %v", err)
	}
	intervention, err := store.ReadActiveExecution(ctx, "c")
	if err != nil || intervention.Status != transition.NeedsIntervention || intervention.Compatibility == nil ||
		intervention.Compatibility.Component != transition.CompatibilityMachine {
		t.Fatalf("intervention did not discover unavailable behavior: %+v, %v", intervention, err)
	}
	register("3")
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err = store.ReadActiveExecution(ctx, "c")
	if err != nil || restored.Status != transition.NeedsIntervention || restored.Compatibility != nil ||
		!restored.Deadline.Equal(intervention.Deadline) {
		t.Fatalf("restoration bypassed intervention or changed its deadline: %+v, %v", restored, err)
	}
	_, _, err = store.Accept(ctx, "d", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "d", Entity: "d", Key: "key",
			Operation: transition.Initialize, MachineID: "machine", MachineVersion: "4", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable waiting behavior was not reported: %v", err)
	}
	waiting, err := store.ReadActiveExecution(ctx, "d")
	if err != nil || waiting.Status != transition.Waiting || waiting.Compatibility == nil ||
		waiting.Compatibility.Component != transition.CompatibilityMachine {
		t.Fatalf("waiting work did not discover unavailable behavior: %+v, %v", waiting, err)
	}
	register("4")
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err = store.ReadActiveExecution(ctx, "d")
	if err != nil || restored.Status != transition.Waiting || restored.Compatibility != nil ||
		!restored.Deadline.Equal(waiting.Deadline) {
		t.Fatalf("waiting restoration changed phase or deadline: %+v, %v", restored, err)
	}
	_, _, err = store.Accept(ctx, "e", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "e", Entity: "e", Key: "key",
			Operation: transition.Initialize, MachineID: "machine", MachineVersion: "5", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err = store.AcquireTransitionLease(ctx, "e", "e", "coordinator", clock.Now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err = store.ClaimHook(ctx, "worker", clock.Now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock.Now); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{
		Failure: fsm.Failure{Code: "failed"}, Disposition: fsm.DispositionRetry,
	}, clock.Now); err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable retry behavior was not reported: %v", err)
	}
	retry, err := store.ReadActiveExecution(ctx, "e")
	progress, progressErr := store.ReadHookProgress(ctx, "e")
	if err != nil || progressErr != nil || retry.Status != transition.Running || retry.Compatibility == nil ||
		progress.Dispatch.Kind != transition.HookRetry || len(progress.Attempts) != 1 || progress.BudgetUsed != 1 {
		t.Fatalf("retry discovery changed phase or attempt history: execution=%+v progress=%+v errors=%v, %v",
			retry, progress, err, progressErr)
	}
	register("5")
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err = store.ReadActiveExecution(ctx, "e")
	if err != nil || restored.Status != transition.Running || restored.Compatibility != nil ||
		!restored.Deadline.Equal(retry.Deadline) {
		t.Fatalf("retry restoration changed phase or deadline: %+v, %v", restored, err)
	}
	t.Run("quiesced recovery", func(t *testing.T) {
		store := newStore(t)
		manager, err := transition.NewCompatibilityCoordinator(store, runner, transition.CoordinatorConfig{
			ID: "coordinator", LeaseTTL: time.Hour, Clock: clock.Now,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = store.Accept(ctx, "f", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "f", Entity: "f", Key: "key",
				Operation: transition.Initialize, MachineID: "machine", MachineVersion: "6", Target: "open",
				Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.AcquireTransitionLease(ctx, "f", "f", "coordinator", clock.Now, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, lease, clock.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimHook(ctx, "worker", clock.Now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, claim, clock.Now); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, claim, transition.HookResult{
			Failure: fsm.Failure{Code: "failed"}, Disposition: fsm.DispositionIntervention,
		}, clock.Now); err != nil {
			t.Fatal(err)
		}
		if err := store.EnterHookIntervention(ctx, lease, clock.Now); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ApplyCheckedHookControl(ctx, lease, clock.Now,
			transition.HookControl{ID: "abort", Action: transition.AbortIntervention},
			func(transition.Execution[string, string], transition.HookCall) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := manager.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("unavailable recovery behavior was not reported: %v", err)
		}
		recovery, err := store.ReadActiveExecution(ctx, "f")
		progress, progressErr := store.ReadHookProgress(ctx, "f")
		if err != nil || progressErr != nil || recovery.Status != transition.Aborting || recovery.Compatibility == nil ||
			progress.Dispatch.Kind != transition.HookRecover || len(progress.Attempts) != 1 {
			t.Fatalf("recovery discovery changed phase or attempt history: execution=%+v progress=%+v errors=%v, %v",
				recovery, progress, err, progressErr)
		}
		register("6")
		if err := manager.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		restored, err := store.ReadActiveExecution(ctx, "f")
		if err != nil || restored.Status != transition.Aborting || restored.Compatibility != nil ||
			!restored.CycleDeadline.Equal(recovery.CycleDeadline) {
			t.Fatalf("recovery restoration changed phase or deadline: %+v, %v", restored, err)
		}
	})
}
