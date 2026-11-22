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

func TestMigrationPreservesOriginalReplayAndFencesUnstartedClaim(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "migration.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	selections := 0
	var registry fsm.Registry[string, string, string, string]
	for _, version := range []string{"1", "2"} {
		machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
			ID: "task", Version: version, InitialStates: []string{"open"},
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
	ref := transition.CodecVersion{Name: "string", Version: "1"}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	now := time.Now().UTC()
	clock := fixedClock(now)
	acceptor, err := transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) {
		return "execution", nil
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.Request[string, string, string, string]{
		Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
		Input: "payload", InputCodec: ref, Notification: transition.NotifyEnable,
	}
	accepted, created, err := acceptor.Accept(ctx, request)
	if err != nil || !created {
		t.Fatalf("accept: created=%v error=%v", created, err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", accepted.ID, "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	oldClaim, err := store.ClaimHook(ctx, "old-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	verificationFailure := errors.New("domain source changed")
	verificationBlocked := true
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](
		store, runner,
		func(_ context.Context, execution transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			calls++
			return transition.MigrationSnapshot[string]{
				MachineID: "task", MachineVersion: "2", Target: "open",
				Input: transition.EncodedValue{Codec: execution.Input.Codec, Data: []byte("transformed")},
				Event: execution.Event, Policy: execution.Policy,
			}, nil
		},
		func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string]) error {
			if verificationBlocked {
				return verificationFailure
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	migration := transition.MigrationRequest{ID: "migration", MachineID: "task", MachineVersion: "2", Parameters: []byte("conversion")}
	denied := errors.New("not authorized")
	allowed := false
	authorizedMigrator, err := transition.NewAuthorizedMigration[string](migrator, func(_ context.Context, request transition.ControlAuthorization) error {
		if request.ExecutionID != accepted.ID || request.Action != transition.ControlMigrate ||
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
	if _, err := authorizedMigrator.Migrate(ctx, fence, clock, migration); !errors.Is(err, denied) || calls != 0 {
		t.Fatalf("unauthorized migration was not denied: calls=%d error=%v", calls, err)
	}
	allowed = true
	if _, err := authorizedMigrator.Migrate(ctx, fence, clock, migration); !errors.Is(err, verificationFailure) {
		t.Fatalf("failed domain verification was not reported: %v", err)
	}
	blocked, err := store.ReadActiveExecution(ctx, accepted.ID)
	if err != nil || blocked.MachineVersion != "1" || blocked.Compatibility == nil ||
		blocked.Compatibility.Component != transition.CompatibilityMigration {
		t.Fatalf("failed migration did not preserve and block the original snapshot: execution=%+v error=%v", blocked, err)
	}
	manager, err := transition.NewCompatibilityCoordinator[string, string, string, string](store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	blocked, err = store.ReadActiveExecution(ctx, accepted.ID)
	if err != nil || blocked.Compatibility == nil {
		t.Fatalf("revalidation cleared a failed migration without domain verification: execution=%+v error=%v", blocked, err)
	}
	verificationBlocked = false
	receipt, err := authorizedMigrator.Migrate(ctx, fence, clock, migration)
	if err != nil || receipt.Request.ID != migration.ID {
		t.Fatalf("migration failed: receipt=%+v error=%v", receipt, err)
	}
	if _, err := store.StartHook(ctx, oldClaim, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("old worker claim survived migration: %v", err)
	}
	migrated, err := store.ReadActiveExecution(ctx, accepted.ID)
	if err != nil || migrated.MachineVersion != "2" || migrated.ID != accepted.ID || !migrated.Notify || !migrated.Deadline.Equal(accepted.Deadline) {
		t.Fatalf("migrated execution lost identity or deadline: execution=%+v error=%v", migrated, err)
	}
	replayed, created, err := acceptor.Accept(ctx, request)
	if err != nil || created || replayed.ID != accepted.ID || selections != 1 {
		t.Fatalf("original request did not replay: execution=%+v created=%v error=%v", replayed, created, err)
	}
	changed := request
	changed.Input = "transformed"
	if _, _, err := acceptor.Accept(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed original request did not conflict: %v", err)
	}
	allowed = false
	if _, err := authorizedMigrator.Migrate(ctx, fence, clock, migration); !errors.Is(err, denied) {
		t.Fatalf("migration replay bypassed authorization: %v", err)
	}
	allowed = true
	if _, err := authorizedMigrator.Migrate(ctx, fence, clock, migration); err != nil || calls != 2 {
		t.Fatalf("migration replay reran conversion: calls=%d error=%v", calls, err)
	}
	migration.Parameters = []byte("different")
	if _, err := migrator.Migrate(ctx, fence, clock, migration); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed migration replay did not conflict: %v", err)
	}
	migration.Parameters = []byte("conversion")
	claim, err := store.ClaimHook(ctx, "new-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	operation, err := store.MarkWorkSuccess(ctx, fence, clock)
	if err != nil {
		t.Fatal(err)
	}
	late := migration
	late.ID = "late"
	if _, err := migrator.Migrate(ctx, fence, clock, late); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("commit-pending execution was migrated: %v", err)
	}
	if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
		ExecutionID: accepted.ID, OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, fence, clock, late); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("notifying execution was migrated: %v", err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	notifier, err := transition.NewNotificationCoordinator[string, string](store, outbox, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := notifier.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, fence, clock, migration); err != nil || calls != 2 {
		t.Fatalf("terminal migration replay failed: calls=%d error=%v", calls, err)
	}
	_, _, err = store.Accept(ctx, "second", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		second := accepted
		second.ID, second.Entity = "second", "second"
		return &second, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	secondFence, err := store.AcquireTransitionLease(ctx, "second", "second", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, secondFence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	activeClaim, err := store.ClaimHook(ctx, "active-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, activeClaim, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, secondFence, clock, migration); err != nil {
		t.Fatalf("migration could not fence an active attempt: %v", err)
	}
	if err := store.ReportHook(ctx, activeClaim, transition.HookResult{Success: true}, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("fenced worker reported after migration: %v", err)
	}
	progress, err := store.ReadHookProgress(ctx, "second")
	if err != nil || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.BudgetUsed != 1 {
		t.Fatalf("fenced attempt was not counted as unknown: progress=%+v error=%v", progress, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadHookProgress(ctx, "second")
	if err != nil || progress.Attempts[0].Result == nil || !progress.Attempts[0].Unknown {
		t.Fatalf("unknown attempt was not classified after migration: progress=%+v error=%v", progress, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	acceptor, err = transition.NewAcceptor[string, string, string, string](store, &registry, resolve, resolve, func() (string, error) {
		return "other", nil
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	replayed, created, err = acceptor.Accept(ctx, request)
	if err != nil || created || replayed.Status != transition.Complete || replayed.ID != accepted.ID || selections != 1 {
		t.Fatalf("migrated terminal replay did not survive restart: execution=%+v created=%v error=%v", replayed, created, err)
	}
}
