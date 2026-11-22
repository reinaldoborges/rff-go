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

type deadlineClassifier struct{}

func (deadlineClassifier) ClassifyHookFailure(_ transition.Execution[string, string], err error) (transition.HookResult, error) {
	if !errors.Is(err, transition.ErrTransitionDeadline) {
		return transition.HookResult{}, err
	}
	return transition.HookResult{Failure: fsm.Failure{Code: "deadline"}, Disposition: fsm.DispositionIntervention}, nil
}

func TestCoordinatorSettlesExpiredUnstartedWorkWithoutWorker(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "deadline.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
				MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Second,
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
	oldClaim, err := store.ClaimHook(ctx, "lost-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.NeedsIntervention || execution.Failure == nil ||
		execution.Failure.Code != "deadline" || execution.FailureDisposition != fsm.DispositionIntervention {
		t.Fatalf("expired work did not enter intervention: execution=%+v error=%v", execution, err)
	}
	if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("expired work was claimable after intervention: %v", err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	control := transition.HookControl{ID: "retry-control", Action: transition.RetryIntervention}
	receipt, err := store.ApplyHookControl(ctx, fence, clock, control)
	if err != nil || !receipt.Deadline.Equal(now.Add(time.Second)) {
		t.Fatalf("retry did not get a fresh deadline: receipt=%+v error=%v", receipt, err)
	}
	now = now.Add(500 * time.Millisecond)
	replay, err := store.ApplyHookControl(ctx, fence, clock, control)
	if err != nil || !replay.Deadline.Equal(receipt.Deadline) {
		t.Fatalf("control replay changed deadline: receipt=%+v error=%v", replay, err)
	}
	if _, err := store.ApplyHookControl(ctx, fence, clock, transition.HookControl{ID: control.ID, Action: transition.AbortIntervention}); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed control replay did not conflict: %v", err)
	}
	stale := fence
	stale.Generation++
	if _, err := store.ApplyHookControl(ctx, stale, clock, control); err != nil {
		t.Fatalf("recorded control could not replay under a stale fence: %v", err)
	}
	if _, err := store.ApplyHookControl(ctx, stale, clock, transition.HookControl{ID: "different", Action: transition.RetryIntervention}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale fence accepted a new control: %v", err)
	}
	if _, err := store.StartHook(ctx, oldClaim, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("old unstarted claim survived intervention: %v", err)
	}
	execution, err = store.ReadActiveExecution(ctx, "execution")
	if err != nil || !execution.Deadline.Equal(now.Add(-500*time.Millisecond)) || !execution.CycleDeadline.Equal(receipt.Deadline) {
		t.Fatalf("retry did not preserve original and current deadlines: execution=%+v error=%v", execution, err)
	}
	lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatalf("worker could not start within renewed control deadline: %v", err)
	}
}
