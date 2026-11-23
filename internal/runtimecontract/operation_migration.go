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
	transition.OperationReconciliationStore
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
			Continuation: &transition.ContinuationSnapshot{
				MachineID: "machine", MachineVersion: "1",
				Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{time.Second}},
			},
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
	assessment := transition.OperationAssessment{
		Outcome: transition.OperationOutcomeSucceeded,
		Evidence: transition.OperationEvidence{
			Identity: binding.Identity, Outcome: transition.OperationOutcomeSucceeded,
			Provider: "provider", Scope: "merchant", Reference: "charge-reference",
			InputFingerprint: "input", Version: "1", Provenance: "lookup",
			BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
			ObservedAt: now, ValidUntil: now.Add(time.Minute),
		},
		Result: &transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt")},
	}
	if _, err := store.ReconcileTrackedOperation(ctx, lease, clock, binding, assessment, equal); err != nil {
		t.Fatal(err)
	}
	compensation := binding
	compensation.Identity.Key = "refund"
	compensation.Compensation = true
	compensation.Prerequisites = []string{binding.Identity.Key}
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, compensation, equal); err != nil || !started {
		t.Fatalf("compensation start: started=%v err=%v", started, err)
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
			if source.Continuation == nil || source.Continuation.MachineVersion != "1" || len(source.Operations) != 3 {
				t.Fatal("converter did not receive both pinned continuation and operations")
			}
			source.Continuation.MachineVersion = "tampered"
			source.Continuation.Policy.RetryDelays[0] = 2 * time.Second
			source.Operations[0].Binding.Input[0] = 'X'
			return transition.MigrationSnapshot[string]{MachineID: "machine", MachineVersion: "2", Target: "open", Input: source.Input, Policy: source.Policy}, nil
		},
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
			if source.Continuation == nil || source.Continuation.MachineVersion != "1" || source.Continuation.Policy.RetryDelays[0] != time.Second {
				return errors.New("continuation binding changed before domain verification")
			}
			for i := range source.Operations {
				if source.Operations[i].Binding.Identity.Key == "refund" {
					source.Operations[i].Binding.Prerequisites[0] = "tampered"
				}
				if source.Operations[i].Binding.Identity == binding.Identity {
					history := source.Operations[i].History
					if len(history) == 0 || history[len(history)-1].Assessment.Result == nil {
						return errors.New("operation evidence missing")
					}
					history[len(history)-1].Assessment.Evidence.Reference = "tampered"
					history[len(history)-1].Assessment.Result.Data[0] = 'X'
				}
			}
			return nil
		},
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
			if len(source.Operations) != 3 || string(source.Operations[0].Binding.Input) != "amount=100" ||
				source.Continuation == nil || source.Continuation.MachineVersion != "1" {
				return errors.New("operation bindings missing from compatibility verification")
			}
			for _, operation := range source.Operations {
				if operation.Binding.Identity.Key == "refund" && (len(operation.Binding.Prerequisites) != 1 || operation.Binding.Prerequisites[0] != binding.Identity.Key) {
					return errors.New("operation prerequisites changed before operation verification")
				}
				if operation.Binding.Identity == binding.Identity {
					if len(operation.History) == 0 || operation.History[len(operation.History)-1].Assessment.Result == nil {
						return errors.New("operation evidence missing")
					}
					last := operation.History[len(operation.History)-1].Assessment
					if last.Evidence.Reference != "charge-reference" || string(last.Result.Data) != "receipt" {
						return errors.New("operation evidence changed before operation verification")
					}
				}
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
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Continuation == nil || execution.Continuation.MachineVersion != "1" ||
		execution.Continuation.Policy.RetryDelays[0] != time.Second {
		t.Fatalf("migration changed pinned continuation: %+v %v", execution.Continuation, err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || inspection.Result == nil || string(inspection.Result.Data) != "receipt" {
		t.Fatalf("migration changed operation result: %+v %v", inspection, err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, unknown.Identity, transition.OperationResult{Codec: unknown.ResultCodec, NoResult: true}); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("migration failed to fence old operation claim: %v", err)
	}
}
