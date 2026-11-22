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

func TestIncompatibleExecutionBlocksWithoutStoppingOthersAndRecovers(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "compatibility.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	var registry fsm.Registry[string, string, string, string]
	calls := 0
	register := func(version string) {
		t.Helper()
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "task", Version: version, InitialStates: []string{"open"},
			Handlers:      map[string]fsm.Handler[string, string, string, string]{"open": recordingEntry{calls: &calls}},
			SelectInitial: func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
			SelectTransition: func(string, string, string) (fsm.Selection[string], error) {
				return fsm.Selection[string]{}, nil
			},
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
	now := time.Now().UTC()
	for _, execution := range []struct{ id, version string }{{"a", "1"}, {"b", "2"}} {
		_, _, err := store.Accept(ctx, execution.id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: execution.id, Entity: execution.id, Key: "key",
				Operation: transition.Initialize, MachineID: "task", MachineVersion: execution.version,
				Target: "open", Input: transition.EncodedValue{Codec: transition.CodecVersion{Name: "string", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, execution.id, execution.id, "coordinator", time.Now, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, fence, time.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	newWorker := func() *transition.HookWorker[string, string, string, string] {
		t.Helper()
		worker, err := transition.NewHookWorker[string, string, string, string](store, runner, transition.WorkerConfig{
			ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
		})
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}
	worker := newWorker()
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("missing machine was not reported: %v", err)
	}
	blocked, err := store.ReadActiveExecution(ctx, "a")
	if err != nil || blocked.Status != transition.Waiting || blocked.Compatibility == nil ||
		blocked.Compatibility.Component != transition.CompatibilityMachine || blocked.Compatibility.Version != "1" {
		t.Fatalf("missing persisted compatibility diagnostic: execution=%+v error=%v", blocked, err)
	}
	progress, err := store.ReadHookProgress(ctx, "a")
	if err != nil || len(progress.Attempts) != 0 {
		t.Fatalf("incompatible work consumed an attempt: progress=%+v error=%v", progress, err)
	}
	_, _, err = store.Accept(ctx, "a", "other", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if !view.Active {
			t.Fatal("incompatible execution lost its reservation")
		}
		return nil, transition.ErrBusy
	})
	if !errors.Is(err, transition.ErrBusy) {
		t.Fatalf("reservation did not reject another request: %v", err)
	}
	if err := worker.RunOne(ctx); err != nil || calls != 1 {
		t.Fatalf("compatible execution could not proceed: calls=%d error=%v", calls, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err = store.ReadActiveExecution(ctx, "a")
	if err != nil || blocked.Compatibility == nil {
		t.Fatalf("compatibility block did not survive restart: execution=%+v error=%v", blocked, err)
	}
	manager, err := transition.NewCompatibilityCoordinator[string, string, string, string](store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	stillBlocked, err := store.ReadActiveExecution(ctx, "a")
	if err != nil || stillBlocked.Compatibility == nil {
		t.Fatalf("missing behavior cleared the block: execution=%+v error=%v", stillBlocked, err)
	}
	register("1")
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := store.ReadActiveExecution(ctx, "a")
	if err != nil || restored.Compatibility != nil || restored.Status != transition.Waiting || !restored.Deadline.Equal(blocked.Deadline) {
		t.Fatalf("revalidation changed phase/deadline or retained block: execution=%+v error=%v", restored, err)
	}
	if err := newWorker().RunOne(ctx); err != nil || calls != 2 {
		t.Fatalf("restored execution did not proceed: calls=%d error=%v", calls, err)
	}
	t.Run("unknown attempt requires its pinned classifier", func(t *testing.T) {
		_, _, err := store.Accept(ctx, "c", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "c", Entity: "c", Key: "key",
				Operation: transition.Initialize, MachineID: "task", MachineVersion: "3",
				Target: "open", Input: transition.EncodedValue{Codec: transition.CodecVersion{Name: "string", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, "c", "c", "coordinator", fixedClock(now), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimHook(ctx, "lost-worker", fixedClock(now), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, claim, fixedClock(now)); err != nil {
			t.Fatal(err)
		}
		later := fixedClock(now.Add(2 * time.Minute))
		coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{
			ID: "coordinator", LeaseTTL: time.Hour, Clock: later,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("missing classifier was not reported: %v", err)
		}
		blocked, err := store.ReadActiveExecution(ctx, "c")
		if err != nil || blocked.Compatibility == nil || blocked.Compatibility.Version != "3" {
			t.Fatalf("unknown attempt compatibility was not recorded: execution=%+v error=%v", blocked, err)
		}
		register("3")
		manager, err := transition.NewCompatibilityCoordinator[string, string, string, string](store, runner, transition.CoordinatorConfig{
			ID: "coordinator", LeaseTTL: time.Hour, Clock: later,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		progress, err := store.ReadHookProgress(ctx, "c")
		if err != nil || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.BudgetUsed != 1 {
			t.Fatalf("unknown attempt was not counted: progress=%+v error=%v", progress, err)
		}
	})
}
