package runtimecontract

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// MigrationStore combines the runtime ports needed by the migration contract.
type MigrationStore interface {
	Store
	transition.MigrationStore[string, string]
	transition.CompatibilityStore[string, string]
	transition.CheckedHookControlStore[string, string]
}

// RunMigration checks domain verification, fencing, and original request replay.
func RunMigration(t *testing.T, newStore func(*testing.T) MigrationStore) {
	t.Helper()
	t.Run("rejected commit phase gates", func(t *testing.T) {
		RunRejectedPhase(t, newStore)
	})
	t.Run("abort recovery rejects migration", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
			ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
		})
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
			Failure: fsm.Failure{Code: "failed"}, Disposition: fsm.DispositionIntervention,
		}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ApplyCheckedHookControl(ctx, lease, clock,
			transition.HookControl{ID: "abort", Action: transition.AbortIntervention},
			func(transition.Execution[string, string], transition.HookCall) error { return nil }); err != nil {
			t.Fatal(err)
		}
		before, err := store.ReadActiveExecution(ctx, "execution")
		if err != nil || before.Status != transition.Aborting || !before.RecoveryStarted {
			t.Fatalf("abort did not enter recovery: %+v, %v", before, err)
		}
		calls := 0
		_, err = store.ApplyExecutionMigration(ctx, lease, clock,
			transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"},
			func(context.Context, transition.MigrationSource[string]) (transition.MigrationSnapshot[string], error) {
				calls++
				return transition.MigrationSnapshot[string]{}, nil
			}, func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string], transition.Execution[string, string], transition.HookCall) error {
				t.Fatal("migration validation ran during abort recovery")
				return nil
			})
		if !errors.Is(err, transition.ErrInvalidHook) || calls != 0 {
			t.Fatalf("abort recovery allowed migration: conversions=%d error=%v", calls, err)
		}
		after, err := store.ReadActiveExecution(ctx, "execution")
		if err != nil || after.Status != before.Status || after.RecoveryStarted != before.RecoveryStarted ||
			!after.EffectiveDeadline().Equal(before.EffectiveDeadline()) {
			t.Fatalf("rejected migration changed abort recovery: %+v, %v", after, err)
		}
	})
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	selections := 0
	var registry fsm.Registry[string, string, string, string]
	for _, version := range []string{"1", "2"} {
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "machine", Version: version, InitialStates: []string{"open"},
			Handlers: map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
			SelectInitial: func(string) (fsm.Selection[string], error) {
				selections++
				return fsm.Selection[string]{Target: "open", Policy: fsm.Policy{
					MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				}}, nil
			},
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
	originalAvailable := true
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version.Name != "input" || version.Version != "1" && version.Version != "2" ||
			version.Version == "1" && !originalAvailable {
			return nil, transition.ErrCodecNotFound
		}
		return stringCodec{}, nil
	}
	ref := transition.CodecVersion{Name: "input", Version: "1"}
	acceptor, err := transition.NewAcceptor[string, string, string, string](
		store, &registry, resolve, resolve, func() (string, error) { return "execution", nil }, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1",
		Input: "original", InputCodec: ref, Notification: transition.NotifyEnable,
	}
	original, created, err := acceptor.Accept(ctx, request)
	if err != nil || !created {
		t.Fatalf("accept original request: created=%v error=%v", created, err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", original.ID, "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "old-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	failedVerification := errors.New("domain changed")
	blockVerification := true
	conversions := 0
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner,
		func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			conversions++
			return transition.MigrationSnapshot[string]{
				MachineID: "machine", MachineVersion: "2", Target: "open",
				Input: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "2"}, Data: []byte("converted")},
				Event: source.Event, Policy: source.Policy,
			}, nil
		}, func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			if blockVerification {
				return failedVerification
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	migration := transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2", Parameters: []byte("convert")}
	allowed := false
	denied := errors.New("migration denied")
	authorized, err := transition.NewAuthorizedMigration[string](migrator, func(_ context.Context, request transition.ControlAuthorization) error {
		if request.ExecutionID != original.ID || request.Action != transition.ControlMigrate ||
			request.Migration == nil || request.Migration.MachineVersion != "2" {
			t.Fatalf("incorrect migration authorization: %+v", request)
		}
		if !allowed {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authorized.Migrate(ctx, lease, clock, migration); !errors.Is(err, denied) || conversions != 0 {
		t.Fatalf("unauthorized migration was not denied: conversions=%d error=%v", conversions, err)
	}
	allowed = true
	if _, err := authorized.Migrate(ctx, lease, clock, migration); !errors.Is(err, failedVerification) {
		t.Fatalf("domain verification failure was not reported: %v", err)
	}
	blocked, err := store.ReadActiveExecution(ctx, original.ID)
	if err != nil || blocked.MachineVersion != "1" || blocked.Compatibility == nil ||
		blocked.Compatibility.Component != transition.CompatibilityMigration {
		t.Fatalf("failed verification changed the source: %+v, %v", blocked, err)
	}
	manager, err := transition.NewCompatibilityCoordinator(store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	blocked, err = store.ReadActiveExecution(ctx, original.ID)
	if err != nil || blocked.Compatibility == nil {
		t.Fatalf("ordinary revalidation bypassed domain verification: %+v, %v", blocked, err)
	}
	blockVerification = false
	if _, err := authorized.Migrate(ctx, lease, clock, migration); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("pre-migration claim was not fenced: %v", err)
	}
	migrated, err := store.ReadActiveExecution(ctx, original.ID)
	if err != nil || migrated.ID != original.ID || migrated.MachineVersion != "2" ||
		!migrated.Notify || !migrated.Deadline.Equal(original.Deadline) ||
		!reflect.DeepEqual(migrated.Policy, original.Policy) {
		t.Fatalf("migration lost identity, choice, deadline, or policy: %+v, %v", migrated, err)
	}
	competing := request
	competing.Key = "competing"
	if _, _, err := acceptor.Accept(ctx, competing); !errors.Is(err, transition.ErrBusy) || selections != 1 {
		t.Fatalf("migration lost entity reservation: selections=%d error=%v", selections, err)
	}
	replayed, created, err := acceptor.Accept(ctx, request)
	if err != nil || created || replayed.ID != original.ID || selections != 1 {
		t.Fatalf("original request did not replay: %+v created=%v error=%v", replayed, created, err)
	}
	request.Input = "converted"
	if _, _, err := acceptor.Accept(ctx, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed original request did not conflict: %v", err)
	}
	request.Input = "original"
	originalAvailable = false
	if _, _, err := acceptor.Accept(ctx, request); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("active replay did not report unavailable original codec: %v", err)
	}
	originalAvailable = true
	allowed = false
	if _, err := authorized.Migrate(ctx, lease, clock, migration); !errors.Is(err, denied) {
		t.Fatalf("migration replay bypassed authorization: %v", err)
	}
	allowed = true
	if _, err := authorized.Migrate(ctx, lease, clock, migration); err != nil || conversions != 2 {
		t.Fatalf("migration replay reran conversion: conversions=%d error=%v", conversions, err)
	}
	migration.Parameters = []byte("different")
	if _, err := authorized.Migrate(ctx, lease, clock, migration); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed migration replay did not conflict: %v", err)
	}
	previousClaim := claim
	claim, err = store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || claim.ActivityID != previousClaim.ActivityID || claim.Generation <= previousClaim.Generation {
		t.Fatalf("migration changed activity identity or failed to fence the old claim: old=%+v new=%+v error=%v", previousClaim, claim, err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkWorkSuccess(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	late := transition.MigrationRequest{ID: "late", MachineID: "machine", MachineVersion: "2"}
	if _, err := authorized.Migrate(ctx, lease, clock, late); !errors.Is(err, transition.ErrInvalidHook) || conversions != 2 {
		t.Fatalf("commit-pending migration was not rejected before conversion: conversions=%d error=%v", conversions, err)
	}
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
	commits, err := transition.NewCommitCoordinator[string, string](store, &host{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := authorized.Migrate(ctx, lease, clock, late); !errors.Is(err, transition.ErrInvalidHook) || conversions != 2 {
		t.Fatalf("notifying migration was not rejected before conversion: conversions=%d error=%v", conversions, err)
	}
	notifications, err := transition.NewNotificationCoordinator[string, string](store, &repairHandoff{available: true}, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := notifications.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	request.Input = "original"
	originalAvailable = false
	if _, _, err := acceptor.Accept(ctx, request); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("terminal replay did not report unavailable original codec: %v", err)
	}
	originalAvailable = true
	terminal, created, err := acceptor.Accept(ctx, request)
	if err != nil || created || terminal.ID != original.ID || terminal.Status != transition.Complete || selections != 1 {
		t.Fatalf("terminal original identity was not preserved: %+v created=%v error=%v", terminal, created, err)
	}
}
