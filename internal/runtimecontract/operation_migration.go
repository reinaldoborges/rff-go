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

// RunMigrationWithoutProvider proves an unknown call cannot migrate on a
// verifier's unsupported assertion when the original provider was not pinned.
func RunMigrationWithoutProvider(t *testing.T, newStore func(*testing.T) OperationMigrationStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "request",
			MachineID: "machine", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("input")},
			Status: transition.Waiting, Deadline: now.Add(time.Hour), Policy: policy,
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
	binding.Provider = ""
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, operationInputEqual); err != nil || !started {
		t.Fatalf("start unknown call: started=%v err=%v", started, err)
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
		},
		func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, fence, clock, transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"}); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("migration accepted unknown operation without a pinned provider: %v", err)
	}
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
	if err := store.CompleteTrackedOperation(ctx, lease, clock, binding.Identity, transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt-ref:charge-100")}); err != nil {
		t.Fatal(err)
	}
	changedProvider := binding
	changedProvider.Provider = "other-provider"
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, changedProvider, equal); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed provider did not conflict: %v", err)
	}
	assessment := transition.OperationAssessment{
		Outcome: transition.OperationOutcomeSucceeded,
		Evidence: transition.OperationEvidence{
			Identity: binding.Identity, Outcome: transition.OperationOutcomeSucceeded,
			Provider: binding.Provider, Scope: "merchant", Reference: "charge-reference",
			InputFingerprint: "input", Version: "1", Provenance: "lookup",
			BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
			ObservedAt: now, ValidUntil: now.Add(time.Minute),
		},
		Result: &transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt-ref:charge-100")},
	}
	wrongProvider := assessment
	wrongProvider.Evidence.Provider = "other-provider"
	if _, err := store.ReconcileTrackedOperation(ctx, lease, clock, binding, wrongProvider, equal); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("evidence from another provider was accepted: %v", err)
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
	changedUnknown := unknown
	changedUnknown.Provider = "other-provider"
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, changedUnknown, equal); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("unknown operation accepted another provider: %v", err)
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
	for _, unavailable := range []string{"provider", "behavior", "codec", "reference", "prerequisite", "evidence"} {
		for _, mode := range []string{"missing", "incompatible"} {
			unavailable := unavailable
			available := map[string]bool{
				"provider:" + binding.Provider: true, "behavior:" + binding.BehaviorVersion: true,
				"codec:" + binding.ResultCodec.Name + "/" + binding.ResultCodec.Version: true,
				"reference:receipt-ref:charge-100":                                      true, "prerequisite:charge": true,
				"evidence:charge-reference": true,
			}
			switch unavailable {
			case "provider":
				delete(available, "provider:"+binding.Provider)
			case "behavior":
				delete(available, "behavior:"+binding.BehaviorVersion)
			case "codec":
				delete(available, "codec:"+binding.ResultCodec.Name+"/"+binding.ResultCodec.Version)
			case "reference":
				delete(available, "reference:receipt-ref:charge-100")
			case "prerequisite":
				delete(available, "prerequisite:charge")
			case "evidence":
				delete(available, "evidence:charge-reference")
			}
			if mode == "incompatible" {
				available[unavailable+":different"] = true
			}
			validator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
				func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
					return transition.MigrationSnapshot[string]{MachineID: "machine", MachineVersion: "2", Target: "open", Input: source.Input, Policy: source.Policy}, nil
				},
				func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
					return nil
				},
				func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
					for _, operation := range source.Operations {
						b := operation.Binding
						if !available["provider:"+b.Provider] || !available["behavior:"+b.BehaviorVersion] ||
							!available["codec:"+b.ResultCodec.Name+"/"+b.ResultCodec.Version] {
							return transition.ErrCompatibility
						}
						if b.Compensation && (len(b.Prerequisites) != 1 || !available["prerequisite:"+b.Prerequisites[0]]) {
							return transition.ErrCompatibility
						}
						if b.Identity == binding.Identity {
							if operation.Result == nil || !available["reference:"+string(operation.Result.Data)] {
								return transition.ErrCompatibility
							}
							if len(operation.History) == 0 || !available["evidence:"+operation.History[len(operation.History)-1].Assessment.Evidence.Reference] {
								return transition.ErrCompatibility
							}
						}
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			_, err = validator.Migrate(ctx, fence, clock, transition.MigrationRequest{ID: mode + "-" + unavailable, MachineID: "machine", MachineVersion: "2"})
			if !errors.Is(err, transition.ErrCompatibility) {
				t.Fatalf("%s %s did not block migration: %v", mode, unavailable, err)
			}
			record, err := store.ReadActiveExecution(ctx, "execution")
			if err != nil || record.MachineVersion != "1" || len(record.Migrations) != 0 || record.Compatibility == nil {
				t.Fatalf("%s %s changed migration state: %+v %v", mode, unavailable, record, err)
			}
		}
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
					if last.Evidence.Reference != "charge-reference" || string(last.Result.Data) != "receipt-ref:charge-100" {
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
	if err != nil || inspection.Result == nil || string(inspection.Result.Data) != "receipt-ref:charge-100" {
		t.Fatalf("migration changed operation result: %+v %v", inspection, err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, unknown.Identity, transition.OperationResult{Codec: unknown.ResultCodec, NoResult: true}); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("migration failed to fence old operation claim: %v", err)
	}
}
