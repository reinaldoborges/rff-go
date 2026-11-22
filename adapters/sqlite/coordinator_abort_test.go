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

func TestAbortRecoveryRequiresSuccessAndCanResumeAfterExhaustion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "abort.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Status: transition.Waiting,
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
	failure := fsm.Failure{Code: "work-failed"}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Failure: failure, Disposition: fsm.DispositionIntervention}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyHookControl(ctx, fence, clock, transition.HookControl{ID: "abort", Action: transition.AbortIntervention}); err != nil {
		t.Fatal(err)
	}
	recovery, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, call, err := store.InspectHook(ctx, recovery, clock)
	if err != nil || call.Kind != transition.HookRecover || call.Failure == nil || *call.Failure != failure {
		t.Fatalf("abort did not dispatch recovery with safe failure: call=%+v error=%v", call, err)
	}
	if _, err := store.StartHook(ctx, recovery, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, recovery, transition.HookResult{
		Failure: fsm.Failure{Code: "recover-failed"}, Disposition: fsm.DispositionRetry,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.NeedsIntervention {
		t.Fatalf("exhausted recovery did not require intervention: execution=%+v error=%v", execution, err)
	}
	if _, err := store.ApplyHookControl(ctx, fence, clock, transition.HookControl{ID: "retry-recovery", Action: transition.RetryIntervention}); err != nil {
		t.Fatal(err)
	}
	retried, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || retried.ActivityID != recovery.ActivityID {
		t.Fatalf("recovery retry changed identity: lease=%+v error=%v", retried, err)
	}
	attempt, err := store.StartHook(ctx, retried, clock)
	if err != nil || attempt.Attempt.Number != 3 {
		t.Fatalf("recovery retry did not retain history: attempt=%+v error=%v", attempt, err)
	}
	if err := store.ReportHook(ctx, retried, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadActiveExecution(ctx, "execution"); !errors.Is(err, transition.ErrExecutionNotActive) {
		t.Fatalf("successful recovery did not release reservation: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, nil
	})
	if err != nil || created || replayed.Status != transition.Aborted {
		t.Fatalf("durable aborted outcome did not replay: execution=%+v created=%v error=%v", replayed, created, err)
	}
	if _, err := store.ApplyHookControl(ctx, fence, clock, transition.HookControl{ID: "abort", Action: transition.AbortIntervention}); err != nil {
		t.Fatalf("lost abort control response could not replay after settlement: %v", err)
	}
}
