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

func TestIncompatibleStateDiagnosticDoesNotBlockOtherWork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Now().UTC()
	for _, item := range []struct{ id, target string }{{"a", "incompatible"}, {"b", "compatible"}, {"c", "incompatible"}} {
		_, _, err := store.Accept(ctx, item.id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: item.id, Entity: item.id, Key: "key",
				Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
				Target: item.target, Input: transition.EncodedValue{Codec: transition.CodecVersion{Name: "string", Version: "1"}, Data: []byte("payload")},
				Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, item.id, item.id, "coordinator", time.Now, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, fence, time.Now, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, selectiveStateCodec{})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"incompatible", "compatible"},
		Handlers: map[string]fsm.Handler[string, string, string, string]{
			"incompatible": recordingEntry{calls: &calls}, "compatible": recordingEntry{calls: &calls},
		},
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
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
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
		t.Fatalf("incompatible state was not reported: %v", err)
	}
	diagnostic, err := store.ReadCompatibility(ctx, "a")
	if err != nil || diagnostic == nil || diagnostic.Component != transition.CompatibilityState {
		t.Fatalf("state diagnostic unavailable: diagnostic=%+v error=%v", diagnostic, err)
	}
	if err := worker.RunOne(ctx); err != nil || calls != 1 {
		t.Fatalf("compatible execution could not proceed: calls=%d error=%v", calls, err)
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("second incompatible state was not reported: %v", err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "c", "c", "coordinator", time.Now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			if string(source.Target) != "incompatible" {
				t.Fatalf("converter lost old state encoding: %q", source.Target)
			}
			return transition.MigrationSnapshot[string]{
				MachineID: "task", MachineVersion: "1", Target: "compatible",
				Input: source.Input, Event: source.Event, Policy: source.Policy,
			}, nil
		},
		func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, fence, time.Now, transition.MigrationRequest{ID: "fix-state", MachineID: "task", MachineVersion: "1"}); err != nil {
		t.Fatalf("could not migrate unavailable state encoding: %v", err)
	}
	if err := worker.RunOne(ctx); err != nil || calls != 2 {
		t.Fatalf("migrated state could not run: calls=%d error=%v", calls, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
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
	if err := newWorker().RunOne(ctx); err != nil || calls != 3 {
		t.Fatalf("restored state could not run: calls=%d error=%v", calls, err)
	}
}
