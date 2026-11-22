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

func TestExpiredStartedHookIsNotClaimedAgainWithoutReconciliation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "unknown.db")
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
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Target: "open",
			Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{time.Second}},
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
	first, err := store.StartHook(ctx, claim, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.StartHook(ctx, claim, fixedClock(now))
	if err != nil || replayed != first {
		t.Fatalf("replayed start consumed an additional attempt: first=%+v replay=%+v error=%v", first, replayed, err)
	}
	unknown := transition.HookResult{Failure: fsm.Failure{Code: "worker-lost"}, Disposition: fsm.DispositionRetry}
	if err := store.ResolveUnknownHook(ctx, fence, fixedClock(now.Add(time.Second)), claim.ActivityID, claim.Generation, unknown); !errors.Is(err, transition.ErrActivityLeaseHeld) {
		t.Fatalf("live attempt was marked unknown: %v", err)
	}
	if _, err := store.ClaimHook(ctx, "other-worker", fixedClock(now.Add(time.Minute)), time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("unknown started attempt was redelivered without reconciliation: %v", err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, fixedClock(now.Add(time.Minute))); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("expired worker reported a result: %v", err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || progress.Attempts[0].Result != nil {
		t.Fatalf("unknown attempt was not retained: progress=%+v error=%v", progress, err)
	}
	if err := store.ResolveUnknownHook(ctx, fence, fixedClock(now.Add(time.Minute)), claim.ActivityID, claim.Generation, unknown); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveUnknownHook(ctx, fence, fixedClock(now.Add(time.Minute)), claim.ActivityID, claim.Generation, unknown); err != nil {
		t.Fatalf("unknown outcome reconciliation did not replay: %v", err)
	}
	changed := transition.HookResult{Failure: fsm.Failure{Code: "different"}, Disposition: fsm.DispositionIntervention}
	if err := store.ResolveUnknownHook(ctx, fence, fixedClock(now.Add(time.Minute)), claim.ActivityID, claim.Generation, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed unknown outcome did not conflict: %v", err)
	}
	progress, err = store.ReadHookProgress(ctx, "execution")
	if err != nil || !progress.Completed || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.Result != unknown {
		t.Fatalf("unknown outcome was not recorded: progress=%+v error=%v", progress, err)
	}
	retryAt := now.Add(time.Minute + time.Second)
	if _, err := store.DispatchHook(ctx, fence, fixedClock(now.Add(time.Minute)), transition.HookDispatch{
		Kind: transition.HookRetry, RetryCycle: 1, DueAt: retryAt, Failure: &unknown.Failure,
	}); err != nil {
		t.Fatal(err)
	}
	retryClaim, err := store.ClaimHook(ctx, "retry-worker", fixedClock(retryAt), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.StartHook(ctx, retryClaim, fixedClock(retryAt))
	if err != nil || retry.Attempt.Number != 2 {
		t.Fatalf("retry hook did not consume shared budget: attempt=%+v error=%v", retry, err)
	}
	if err := store.ReportHook(ctx, retryClaim, transition.HookResult{Success: true}, fixedClock(retryAt)); err != nil {
		t.Fatal(err)
	}
	if err := store.RedeliverHook(ctx, fence, fixedClock(retryAt), claim.ActivityID, claim.Generation, retryAt); err != nil {
		t.Fatal(err)
	}
	redelivery, err := store.ClaimHook(ctx, "recovery-worker", fixedClock(retryAt), time.Minute)
	if err != nil || redelivery.ActivityID != claim.ActivityID || redelivery.Generation <= retryClaim.Generation {
		t.Fatalf("redelivery changed identity or failed to advance its fence: lease=%+v error=%v", redelivery, err)
	}
	restarted, err := store.StartHook(ctx, redelivery, fixedClock(retryAt))
	if err != nil || restarted.Attempt.Number != 3 || restarted.Attempt.ActivityIdempotencyKey != first.Attempt.ActivityIdempotencyKey {
		t.Fatalf("redelivery did not retain identity and attempt history: attempt=%+v error=%v", restarted, err)
	}
	if err := store.RedeliverHook(ctx, fence, fixedClock(retryAt), claim.ActivityID, claim.Generation, retryAt); err != nil {
		t.Fatalf("redelivery replay failed after start: %v", err)
	}
	progress, err = store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 3 || progress.Completed {
		t.Fatalf("redelivery replay changed work: progress=%+v error=%v", progress, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	progress, err = store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 3 || !progress.Attempts[0].Unknown || progress.ActivityID != claim.ActivityID ||
		!progress.Started || progress.BudgetUsed != 3 || progress.LeaseExpiresAt.IsZero() {
		t.Fatalf("reopened store lost recovery history: progress=%+v error=%v", progress, err)
	}
}
