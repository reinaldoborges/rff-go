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

func TestFailedRetryHookIsRedeliveredDirectlyWithPinnedDelay(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "retry.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Status: transition.Waiting,
			Deadline: now.Add(time.Hour), Policy: fsm.Policy{
				MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				RetryDelays: []time.Duration{time.Second, 3 * time.Second},
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	failure := fsm.Failure{Code: "retryable"}
	result := transition.HookResult{Failure: failure, Disposition: fsm.DispositionRetry}
	if err := store.ReportHook(ctx, claim, result, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	retryAt := now.Add(time.Second)
	retryID, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{
		Kind: transition.HookRetry, RetryCycle: 1, DueAt: retryAt, Failure: &failure,
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.ClaimHook(ctx, "worker", fixedClock(retryAt), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, retry, fixedClock(retryAt)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, retry, result, fixedClock(retryAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, fixedClock(retryAt), transition.HookDispatch{
		Kind: transition.HookRetry, RetryCycle: 2, DueAt: retryAt.Add(3 * time.Second), Failure: &failure,
	}); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("nested Retry hook was not rejected: %v", err)
	}
	if err := store.RedeliverHook(ctx, fence, fixedClock(retryAt), retryID, retry.Generation, retryAt); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("retry was accepted before its pinned delay: %v", err)
	}
	redeliveryAt := retryAt.Add(3 * time.Second)
	if err := store.RedeliverHook(ctx, fence, fixedClock(retryAt), retryID, retry.Generation, redeliveryAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimHook(ctx, "worker", fixedClock(retryAt), time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("retry ran before its due time: %v", err)
	}
	reclaimed, err := store.ClaimHook(ctx, "worker", fixedClock(redeliveryAt), time.Minute)
	if err != nil || reclaimed.ActivityID != retryID || reclaimed.Generation <= retry.Generation {
		t.Fatalf("retry hook identity or fence changed incorrectly: claim=%+v error=%v", reclaimed, err)
	}
	attempt, err := store.StartHook(ctx, reclaimed, fixedClock(redeliveryAt))
	if err != nil || attempt.Attempt.Number != 3 || attempt.Attempt.ActivityIdempotencyKey != retryID {
		t.Fatalf("retry hook did not preserve identity: attempt=%+v error=%v", attempt, err)
	}
	if err := store.ReportHook(ctx, reclaimed, transition.HookResult{Success: true}, fixedClock(redeliveryAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, fixedClock(redeliveryAt), transition.HookDispatch{
		Kind: transition.HookEnter, RetryCycle: 2, DueAt: redeliveryAt,
	}); err != nil {
		t.Fatalf("dispatch after Retry redelivery: %v", err)
	}
}
