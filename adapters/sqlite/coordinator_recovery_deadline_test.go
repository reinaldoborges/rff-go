package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestRecoveryDeadlineRequiresInterventionAndRenewsOnAuthorizedRetry(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "recovery-deadline.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Second), Policy: fsm.Policy{
				MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Second,
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, deadlineClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	first, err := applyTestHookControl(store, ctx, fence, clock, transition.HookControl{ID: "abort", Action: transition.AbortIntervention})
	if err != nil {
		t.Fatal(err)
	}
	now = first.Deadline
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.NeedsIntervention || !execution.RecoveryStarted {
		t.Fatalf("expired recovery did not retain its phase and reservation: execution=%+v error=%v", execution, err)
	}
	second, err := applyTestHookControl(store, ctx, fence, clock, transition.HookControl{ID: "retry-recovery", Action: transition.RetryIntervention})
	if err != nil || !second.Deadline.Equal(now.Add(time.Second)) {
		t.Fatalf("recovery retry did not get a fresh deadline: receipt=%+v error=%v", second, err)
	}
	execution, err = store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.Aborting || len(execution.Controls) != 2 ||
		!execution.Controls[0].Deadline.Equal(first.Deadline) || !execution.CycleDeadline.Equal(second.Deadline) {
		t.Fatalf("recovery control history was not preserved: execution=%+v error=%v", execution, err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, call, err := store.InspectHook(ctx, claim, clock)
	if err != nil || call.Kind != transition.HookRecover {
		t.Fatalf("retry did not resume recovery: call=%+v error=%v", call, err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatalf("recovery could not start within renewed deadline: %v", err)
	}
}
