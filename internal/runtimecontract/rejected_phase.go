package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunRejectedPhase checks fencing and control gates before automatic recovery.
func RunRejectedPhase(t *testing.T, newStore func(*testing.T) MigrationStore) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
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
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	receipt := transition.CommitReceipt{
		ExecutionID: "execution", OperationID: execution.CommitOperationID, Outcome: transition.CommitDenied,
		Failure: &fsm.Failure{Code: "invariant"},
	}
	stale := lease
	stale.Generation++
	if err := store.RecordCommitReceipt(ctx, stale, clock, receipt); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale fence recorded rejection: %v", err)
	}
	pending, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || pending.CommitReceipt != nil || pending.Status != transition.Running {
		t.Fatalf("stale rejection changed execution: %+v, %v", pending, err)
	}
	if err := store.RecordCommitReceipt(ctx, lease, clock, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyCheckedHookControl(ctx, lease, clock,
		transition.HookControl{ID: "retry", Action: transition.RetryIntervention},
		func(transition.Execution[string, string], transition.HookCall) error { return nil }); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("rejected commit accepted retry: %v", err)
	}
	conversions := 0
	_, err = store.ApplyExecutionMigration(ctx, lease, clock,
		transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"},
		func(context.Context, transition.MigrationSource[string]) (transition.MigrationSnapshot[string], error) {
			conversions++
			return transition.MigrationSnapshot[string]{}, nil
		}, func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string], transition.Execution[string, string], transition.HookCall) error {
			t.Fatal("rejected commit reached migration validation")
			return nil
		})
	if !errors.Is(err, transition.ErrInvalidHook) || conversions != 0 {
		t.Fatalf("rejected commit accepted migration: conversions=%d error=%v", conversions, err)
	}
	commits, err := transition.NewCommitCoordinator[string, string](store, rejectionHost{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || recovered.Status != transition.Aborting || recovered.CommitReceipt == nil ||
		recovered.CommitReceipt.Outcome != transition.CommitDenied {
		t.Fatalf("rejection did not enter recovery: %+v, %v", recovered, err)
	}
}
