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

func TestOldDispatchReplayDoesNotReplaceNewerHook(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "replay.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
			Deadline: now.Add(time.Hour), Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	first := transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}
	firstID, err := store.DispatchHook(ctx, fence, fixedClock(now), first)
	if err != nil {
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
	if err := store.ReportHook(ctx, claim, transition.HookResult{Failure: failure, Disposition: fsm.DispositionRetry}, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	secondID, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{
		Kind: transition.HookRetry, RetryCycle: 1, DueAt: now, Failure: &failure,
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err = store.ClaimHook(ctx, "worker", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	replayedID, err := store.DispatchHook(ctx, fence, fixedClock(now), first)
	if err != nil || replayedID != firstID {
		t.Fatalf("old dispatch did not replay: id=%q error=%v", replayedID, err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || progress.ActivityID != secondID || !progress.Completed || len(progress.Attempts) != 2 {
		t.Fatalf("old dispatch replaced current work: progress=%+v error=%v", progress, err)
	}
	if _, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{Kind: transition.HookEnter, RetryCycle: 1, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err = store.ClaimHook(ctx, "worker", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, fixedClock(now)); !errors.Is(err, transition.ErrAttemptBudget) {
		t.Fatalf("successful retry hook did not consume the shared attempt budget: %v", err)
	}
}
