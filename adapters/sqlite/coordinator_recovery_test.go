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

type unknownClassifier struct{}

func (unknownClassifier) ClassifyHookFailure(_ transition.Execution[string, string], err error) (transition.HookResult, error) {
	if !errors.Is(err, transition.ErrAttemptUnknown) {
		return transition.HookResult{}, err
	}
	return transition.HookResult{Failure: fsm.Failure{Code: "unknown"}, Disposition: fsm.DispositionRetry}, nil
}

func TestCoordinatorClassifiesExpiredStartedWork(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "coordinator.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
			AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
				MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				RetryDelays: []time.Duration{time.Second},
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
	claim, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || !progress.Completed || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown ||
		progress.Result.Failure.Code != "unknown" || progress.BudgetUsed != 1 {
		t.Fatalf("unknown outcome was not durably classified: progress=%+v error=%v", progress, err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("Retry ran before its delay: %v", err)
	}
	now = now.Add(time.Second)
	retry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || retry.ActivityID == claim.ActivityID {
		t.Fatalf("Retry was not scheduled as a distinct hook: lease=%+v error=%v", retry, err)
	}
	if _, err := store.StartHook(ctx, retry, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, retry, transition.HookResult{
		Failure: fsm.Failure{Code: "retry-failed"}, Disposition: fsm.DispositionRetry,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("failed Retry was redelivered before its delay: %v", err)
	}
	now = now.Add(time.Second)
	retriedRetry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || retriedRetry.ActivityID != retry.ActivityID || retriedRetry.Generation <= retry.Generation {
		t.Fatalf("failed Retry was not directly redelivered: lease=%+v error=%v", retriedRetry, err)
	}
	if _, err := store.StartHook(ctx, retriedRetry, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, retriedRetry, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	redelivery, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || redelivery.ActivityID != claim.ActivityID || redelivery.Generation <= claim.Generation {
		t.Fatalf("original hook was not redelivered with its identity: lease=%+v error=%v", redelivery, err)
	}
}
