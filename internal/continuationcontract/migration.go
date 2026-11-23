package continuationcontract

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type MigrationStore interface {
	AtomicStore
	transition.MigrationStore[string, string]
	transition.ContinuationDriveStore[string, string]
}

type stringCodec struct{}

func (stringCodec) Encode(_ context.Context, value string) ([]byte, error) { return []byte(value), nil }
func (stringCodec) Decode(_ context.Context, data []byte) (string, error)  { return string(data), nil }
func (stringCodec) Equal(_ context.Context, data []byte, value string) (bool, error) {
	return string(data) == value, nil
}

func RunMigration(t *testing.T, store MigrationStore) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{time.Second}}
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "origin", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
			MachineID: "plan", MachineVersion: "1", Policy: policy,
			Input:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Continuation: &transition.ContinuationSnapshot{MachineID: "plan", MachineVersion: "1", Policy: policy},
			AcceptedAt:   now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "origin", "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.MigrationRequest{ID: "migration", MachineID: "plan", MachineVersion: "2"}
	convert := func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
		if source.Continuation == nil || source.Continuation.MachineVersion != "1" {
			t.Fatalf("converter did not receive pinned origin behavior: %+v", source.Continuation)
		}
		return transition.MigrationSnapshot[string]{
			MachineID: "plan", MachineVersion: "2", Target: "due", Policy: source.Policy, Input: source.Input,
		}, nil
	}
	notCompatible := errors.New("destination cannot honor pinned continuation")
	allow := false
	validate := func(_ context.Context, source transition.MigrationSource[string], _ transition.MigrationSnapshot[string]) error {
		if source.Continuation == nil || source.Continuation.MachineVersion != "1" {
			t.Fatal("migration lost pinned continuation")
		}
		if !allow {
			return notCompatible
		}
		return nil
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "plan", Version: "2", InitialStates: []string{"due"},
		Handlers:         map[string]fsm.Handler[string, string, string, string]{"due": fsm.NoopHandler[string, string, string, string]{}},
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
	migrator, err := transition.NewExecutionMigrator[string, string, string, string](store, runner, convert, validate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, lease, clock, request); !errors.Is(err, notCompatible) {
		t.Fatalf("unproven destination was accepted: %v", err)
	}
	allow = true
	if _, err := migrator.Migrate(ctx, lease, clock, request); err != nil {
		t.Fatal(err)
	}
	migrated, err := store.ReadActiveExecution(ctx, "origin")
	if err != nil || migrated.MachineVersion != "2" || migrated.Continuation == nil ||
		migrated.Continuation.MachineVersion != "1" || migrated.Continuation.Policy.MaxAttempts != 2 {
		t.Fatalf("migration changed the pinned continuation: %+v, %v", migrated, err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("migration did not fence the old coordinator: %v", err)
	}
	earlier := func() time.Time { return now.Add(-time.Second) }
	if _, err := store.DispatchHook(ctx, lease, earlier, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("old coordinator regained its fence after clock rollback: %v", err)
	}
	lease, err = store.AcquireTransitionLease(ctx, "entity", "origin", "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	operation, err := store.MarkWorkSuccess(ctx, lease, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitAtomic(ctx, lease, clock, func(_ context.Context, _ *sql.Tx, _ transition.Execution[string, string]) (transition.CommitReceipt, error) {
		return transition.CommitReceipt{ExecutionID: "origin", OperationID: operation, Outcome: transition.CommitSucceeded}, nil
	}); err != nil {
		t.Fatal(err)
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 || markers[0].MachineVersion != "1" || markers[0].Policy.MaxAttempts != 2 {
		t.Fatalf("commit lost pinned v1 behavior: %+v, %v", markers, err)
	}
	if ready, err := store.ContinuationReady(ctx, "origin"); err != nil || !ready {
		t.Fatalf("migrated continuation not ready: ready=%v err=%v", ready, err)
	}
	driver, err := transition.NewContinuationDriver(store, &registry, transition.ContinuationDriverConfig[string, string, string, string]{
		Clock: clock, LeaseDuration: time.Minute,
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
		ResolveEvent: resolve, ResolveInput: resolve,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Drive(ctx, markers[0], "worker"); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("missing original continuation version did not block: %v", err)
	}
	calls := 0
	original, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "plan", Version: "1", InitialStates: []string{"due"},
		Handlers:             map[string]fsm.Handler[string, string, string, string]{"due": fsm.NoopHandler[string, string, string, string]{}},
		ContinuationPolicies: map[string]fsm.Policy{"due": policy},
		SelectInitial:        func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition:     func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "failed"}, fsm.DispositionIntervention
		},
		Continue: func(_ context.Context, entity, state string) (fsm.ContinuationDecision[string, string], error) {
			if entity != "entity" || state != "due" {
				t.Fatalf("wrong continuation inputs: %q, %q", entity, state)
			}
			calls++
			return fsm.ContinuationDecision[string, string]{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(original); err != nil {
		t.Fatal(err)
	}
	if err := driver.Drive(ctx, markers[0], "worker"); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadContinuation(ctx, "origin")
	if err != nil || calls != 1 || progress.Attempts != 1 || progress.Phase != transition.ContinuationStopped {
		t.Fatalf("migrated execution did not evaluate v1 continuation: calls=%d progress=%+v err=%v", calls, progress, err)
	}
}
