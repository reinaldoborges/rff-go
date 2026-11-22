package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunRetryBudget checks unstarted claim loss and the shared hook-attempt budget.
func RunRetryBudget(t *testing.T, newStore func(*testing.T) ControlStore) {
	t.Helper()
	t.Run("failed work and retry preserve selection", func(t *testing.T) {
		RunRetrySelection(t, newStore)
	})
	t.Run("retry schedule exhaustion", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 5, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
					RetryDelays: []time.Duration{time.Second, 3 * time.Second},
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
		for i, delay := range []time.Duration{time.Second, 3 * time.Second, 3 * time.Second} {
			claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			_, call, err := store.InspectHook(ctx, claim, clock)
			if err != nil || (i == 0 && call.Kind != transition.HookEnter) || (i > 0 && call.Kind != transition.HookRetry) {
				t.Fatalf("unexpected hook before retry %d: %+v, %v", i, call, err)
			}
			if _, err := store.StartHook(ctx, claim, clock); err != nil {
				t.Fatal(err)
			}
			if err := store.ReportHook(ctx, claim, transition.HookResult{
				Failure: fsm.Failure{Code: "retryable"}, Disposition: fsm.DispositionRetry,
			}, clock); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			now = now.Add(delay - time.Nanosecond)
			if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
				t.Fatalf("retry %d ran before its configured delay: %v", i, err)
			}
			now = now.Add(time.Nanosecond)
		}
		claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatalf("retry after repeated final delay was not due: %v", err)
		}
		_, call, err := store.InspectHook(ctx, claim, clock)
		if err != nil || call.Kind != transition.HookRetry {
			t.Fatalf("final retry was not the retry hook: %+v, %v", call, err)
		}
	})
	t.Run("empty retry schedule requires intervention", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 5, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
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
			Failure: fsm.Failure{Code: "retryable"}, Disposition: fsm.DispositionRetry,
		}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record, err := store.InspectExecution(ctx, "execution")
		if err != nil || record.Status != transition.NeedsIntervention {
			t.Fatalf("empty retry schedule did not require intervention: %+v, %v", record, err)
		}
		if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
			t.Fatalf("empty retry schedule started work: %v", err)
		}
	})
	t.Run("unstarted claim loss", func(t *testing.T) {
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
		lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
		lost, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
		reclaimed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil || reclaimed.ActivityID != lost.ActivityID || reclaimed.Generation <= lost.Generation {
			t.Fatalf("unstarted claim was not reclaimed under a new fence: %+v, %v", reclaimed, err)
		}
		if _, err := store.StartHook(ctx, lost, clock); !errors.Is(err, transition.ErrStaleActivityLease) {
			t.Fatalf("lost claim could still start: %v", err)
		}
		started, err := store.StartHook(ctx, reclaimed, clock)
		if err != nil || started.Attempt.Number != 1 || started.Attempt.ActivityIdempotencyKey != lost.ActivityID {
			t.Fatalf("lost unstarted claim consumed an attempt or changed identity: %+v, %v", started, err)
		}
	})
	t.Run("retry hook consumes budget", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
				AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
					RetryDelays: []time.Duration{0},
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
		first, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, first, clock); err != nil {
			t.Fatal(err)
		}
		failure := fsm.Failure{Code: "work-failed"}
		if err := store.ReportHook(ctx, first, transition.HookResult{Failure: failure, Disposition: fsm.DispositionRetry}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		retry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, retry, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, retry, transition.HookResult{Success: true}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record, err := store.ReadActiveExecution(ctx, "execution")
		if err != nil || record.Status != transition.NeedsIntervention || record.Failure == nil || *record.Failure != failure {
			t.Fatalf("exhausted execution did not retain failure: %+v, %v", record, err)
		}
		if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
			t.Fatalf("work ran after shared budget exhaustion: %v", err)
		}
		lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		originalDeadline := now.Add(time.Hour)
		now = now.Add(time.Second)
		control := transition.HookControl{ID: "retry", Action: transition.RetryIntervention}
		receipt, err := store.ApplyCheckedHookControl(ctx, lease, clock, control,
			func(transition.Execution[string, string], transition.HookCall) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if !receipt.AcceptedAt.Equal(now) || !receipt.Deadline.Equal(now.Add(time.Hour)) {
			t.Fatalf("new control cycle has the wrong deadline: %+v", receipt)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		resumed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		started, err := store.StartHook(ctx, resumed, clock)
		if err != nil || started.Attempt.Number != 3 {
			t.Fatalf("new control cycle lost attempt history: %+v, %v", started, err)
		}
		now = now.Add(time.Second)
		replayed, err := store.ApplyCheckedHookControl(ctx, lease, clock, control,
			func(transition.Execution[string, string], transition.HookCall) error { return nil })
		progress, progressErr := store.ReadHookProgress(ctx, "execution")
		execution, executionErr := store.ReadActiveExecution(ctx, "execution")
		if err != nil || !replayed.AcceptedAt.Equal(receipt.AcceptedAt) || !replayed.Deadline.Equal(receipt.Deadline) ||
			progressErr != nil || progress.BudgetUsed != 1 || len(progress.Attempts) != 3 ||
			executionErr != nil || !execution.Deadline.Equal(originalDeadline) ||
			!execution.CycleDeadline.Equal(receipt.Deadline) {
			t.Fatalf("control replay changed budget, deadlines, or history: receipt=%+v progress=%+v execution=%+v errors=%v, %v, %v",
				replayed, progress, execution, err, progressErr, executionErr)
		}
	})
	t.Run("abort recovery exhaustion and retry", func(t *testing.T) {
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
		lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		check := func(transition.Execution[string, string], transition.HookCall) error { return nil }
		if _, err := store.ApplyCheckedHookControl(ctx, lease, clock,
			transition.HookControl{ID: "abort", Action: transition.AbortIntervention}, check); err != nil {
			t.Fatal(err)
		}
		recovery, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, call, err := store.InspectHook(ctx, recovery, clock)
		if err != nil || call.Kind != transition.HookRecover || call.Failure == nil || *call.Failure != failure {
			t.Fatalf("abort did not invoke recovery with the failure: %+v, %v", call, err)
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
		record, err := store.InspectExecution(ctx, "execution")
		if err != nil || record.Status != transition.NeedsIntervention {
			t.Fatalf("failed recovery did not require intervention: %+v, %v", record, err)
		}
		if _, err := store.ApplyCheckedHookControl(ctx, lease, clock,
			transition.HookControl{ID: "retry-recovery", Action: transition.RetryIntervention}, check); err != nil {
			t.Fatal(err)
		}
		resumed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil || resumed.ActivityID != recovery.ActivityID {
			t.Fatalf("recovery retry changed identity: %+v, %v", resumed, err)
		}
		attempt, err := store.StartHook(ctx, resumed, clock)
		if err != nil || attempt.Attempt.Number != 3 {
			t.Fatalf("recovery retry lost attempt history: %+v, %v", attempt, err)
		}
		if err := store.ReportHook(ctx, resumed, transition.HookResult{Success: true}, clock); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record, err = store.InspectExecution(ctx, "execution")
		if err != nil || record.Status != transition.Aborted || record.CommitReceipt != nil {
			t.Fatalf("successful recovery did not settle without committing: %+v, %v", record, err)
		}
	})
}
