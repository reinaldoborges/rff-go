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

func TestDurableWorkSuccessCannotBeDispatchedAgain(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "success.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
			Deadline: now.Add(time.Hour), Policy: fsm.Policy{MaxAttempts: 3, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkWorkSuccess(ctx, fence, fixedClock(now)); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("marked success without work: %v", err)
	}
	first, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{Kind: transition.HookEnter, DueAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkWorkSuccess(ctx, fence, fixedClock(now)); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("marked success before hook completion: %v", err)
	}
	claim, err := store.ClaimHook(ctx, "worker", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, fixedClock(now)); err != nil {
		t.Fatal(err)
	}
	stale := fence
	stale.Generation++
	if _, err := store.MarkWorkSuccess(ctx, stale, fixedClock(now)); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale coordinator marked success: %v", err)
	}
	if _, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{Kind: transition.HookEnter, RetryCycle: 1, DueAt: now}); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("durably successful work was dispatched again: %v", err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || progress.ActivityID != first || !progress.Completed || !progress.Result.Success {
		t.Fatalf("successful work marker changed: progress=%+v error=%v", progress, err)
	}
}
