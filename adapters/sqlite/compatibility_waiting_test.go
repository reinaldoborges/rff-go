package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestWaitingExecutionDiscoversCompatibilityWithoutWorker(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "waiting-discovery.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, MachineID: "task", MachineVersion: "1", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "string", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
	manager, err := transition.NewCompatibilityCoordinator(store, runner, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable behavior was not reported: %v", err)
	}
	blocked, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || blocked.Status != transition.Waiting || blocked.Compatibility == nil ||
		blocked.Compatibility.Component != transition.CompatibilityMachine {
		t.Fatalf("waiting execution did not expose unavailable behavior: %+v, %v", blocked, err)
	}
	_, _, err = store.Accept(ctx, "entity", "other", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if !view.Active {
			t.Fatal("blocked execution lost its reservation")
		}
		return nil, transition.ErrBusy
	})
	if !errors.Is(err, transition.ErrBusy) {
		t.Fatalf("blocked execution accepted competing work: %v", err)
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers:         map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
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
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "lost-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var missing fsm.Registry[string, string, string, string]
	unavailableRunner, err := transition.NewHookRunner[string, string, string, string](&missing, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	unavailable, err := transition.NewCompatibilityCoordinator(store, unavailableRunner, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := unavailable.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable behavior was not reported: %v", err)
	}
	if _, err := store.StartHook(ctx, claim, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("discovery did not fence the unstarted claim: %v", err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := store.StartHook(ctx, resumed, clock)
	if err != nil || attempt.Attempt.Number != 1 {
		t.Fatalf("unstarted claim consumed an attempt: %+v, %v", attempt, err)
	}
}

func TestPendingRetryDiscoversCompatibilityWithoutWorker(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "retry-discovery.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, MachineID: "task", MachineVersion: "1", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "string", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{
		Failure: fsm.Failure{Code: "failed"}, Disposition: fsm.DispositionRetry,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := transition.NewCompatibilityCoordinator(store, runner, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable retry behavior was not reported: %v", err)
	}
	blocked, err := store.ReadActiveExecution(ctx, "execution")
	progress, progressErr := store.ReadHookProgress(ctx, "execution")
	if err != nil || progressErr != nil || blocked.Status != transition.Running || blocked.Compatibility == nil ||
		progress.Dispatch.Kind != transition.HookRetry || len(progress.Attempts) != 1 || progress.BudgetUsed != 1 {
		t.Fatalf("pending retry changed while discovering compatibility: execution=%+v progress=%+v errors=%v, %v",
			blocked, progress, err, progressErr)
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers:         map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
		SelectInitial:    func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "failed"}, fsm.DispositionRetry
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || before.Compatibility != nil || before.Status != transition.Running ||
		!before.Deadline.Equal(blocked.Deadline) {
		t.Fatalf("restoring retry behavior changed phase or deadline: %+v, %v", before, err)
	}
	progress, err = store.ReadHookProgress(ctx, "execution")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, retry, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockQuiescedCompatibility(ctx, lease, clock, before, &progress, transition.CompatibilityDiagnostic{
		Component: transition.CompatibilityMachine, Name: "task", Version: "1",
	}); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale discovery blocked a started retry: %v", err)
	}
	var missing fsm.Registry[string, string, string, string]
	unavailableRunner, err := transition.NewHookRunner[string, string, string, string](&missing, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	unavailable, err := transition.NewCompatibilityCoordinator(store, unavailableRunner, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := unavailable.Reconcile(ctx); err != nil {
		t.Fatalf("scan interrupted a started retry: %v", err)
	}
	if err := store.ReportHook(ctx, retry, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := unavailable.Reconcile(ctx); err != nil {
		t.Fatalf("scan blocked completed successful work: %v", err)
	}
	after, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || after.Compatibility != nil {
		t.Fatalf("scan blocked a started or successful retry: %+v, %v", after, err)
	}
}
