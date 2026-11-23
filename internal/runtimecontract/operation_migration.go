package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type OperationMigrationStore interface {
	OperationStore
	transition.MigrationStore[string, string]
}

// RunOperationMigration checks operation binding preservation and claim fencing.
func RunOperationMigration(t *testing.T, newStore func(*testing.T) OperationMigrationStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "request",
			MachineID: "machine", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	binding := restartBinding()
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("operation start: started=%v err=%v", started, err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, binding.Identity, transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt")}); err != nil {
		t.Fatal(err)
	}
	unknown := binding
	unknown.Identity.Key = "unknown"
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, unknown, equal); err != nil || !started {
		t.Fatalf("unknown operation start: started=%v err=%v", started, err)
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "2", InitialStates: []string{"open"},
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
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			return transition.MigrationSnapshot[string]{MachineID: "machine", MachineVersion: "2", Target: "open", Input: source.Input, Policy: source.Policy}, nil
		},
		func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, fence, clock, transition.MigrationRequest{ID: "unverified", MachineID: "machine", MachineVersion: "2"}); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("migration without operation verification was accepted: %v", err)
	}
	validated := false
	verifiedMigrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			return transition.MigrationSnapshot[string]{MachineID: "machine", MachineVersion: "2", Target: "open", Input: source.Input, Policy: source.Policy}, nil
		},
		func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			return nil
		},
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
			if len(source.Operations) != 2 {
				return errors.New("operation bindings missing from compatibility verification")
			}
			validated = true
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifiedMigrator.Migrate(ctx, fence, clock, transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"})
	if err != nil || !validated {
		t.Fatalf("compatible operation migration failed: validated=%v err=%v", validated, err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || inspection.Result == nil || string(inspection.Result.Data) != "receipt" {
		t.Fatalf("migration changed operation result: %+v %v", inspection, err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, unknown.Identity, transition.OperationResult{Codec: unknown.ResultCodec, NoResult: true}); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("migration failed to fence old operation claim: %v", err)
	}
}
