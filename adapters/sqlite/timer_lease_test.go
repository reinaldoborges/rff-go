package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerClaimRenewalAndStaleReports(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "claims.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	for _, id := range []string{"a", "b"} {
		_, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
			ID: id, WorkflowID: "workflow", Deadline: now, Event: "event",
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, first, err := store.ClaimDueTimer(ctx, "one", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if _, err := store.RenewTimer(ctx, first, clock, time.Minute); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * time.Second)
	_, second, err := store.ClaimDueTimer(ctx, "two", clock, time.Minute)
	if err != nil || second.ID != "b" {
		t.Fatalf("renewed claim was stolen: lease=%+v error=%v", second, err)
	}
	if err := store.CompleteTimer(ctx, first, clock); err != nil {
		t.Fatalf("renewed claim could not complete using its original lease: %v", err)
	}
	now = now.Add(time.Minute)
	_, reclaimed, err := store.ClaimDueTimer(ctx, "three", clock, time.Minute)
	if err != nil || reclaimed.ID != "b" || reclaimed.Generation <= second.Generation {
		t.Fatalf("expired claim not reclaimed: lease=%+v error=%v", reclaimed, err)
	}
	if err := store.CompleteTimer(ctx, second, clock); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale claim completed timer: %v", err)
	}
	if err := store.CompleteTimer(ctx, reclaimed, clock); err != nil {
		t.Fatal(err)
	}
}

func TestTimerCompatibilityBlockFencesStaleClaims(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "blocked.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "timer", WorkflowID: "workflow", Deadline: now, Event: "event",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	_, claim, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := transition.CompatibilityDiagnostic{Component: transition.CompatibilityEventCodec, Name: "event", Version: "1"}
	if err := store.BlockTimerCompatibility(ctx, claim, clock, diagnostic); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenewTimer(ctx, claim, clock, time.Minute); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("blocked timer renewed its stale claim: %v", err)
	}
	if err := store.CompleteTimer(ctx, claim, clock); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("blocked timer accepted a stale completion: %v", err)
	}
	if _, _, err := store.ClaimDueTimer(ctx, "other", clock, time.Minute); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("blocked timer was reclaimed: %v", err)
	}
	changed := diagnostic
	changed.Version = "2"
	blocked, err := store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 1 {
		t.Fatalf("blocked timer not found: %+v, %v", blocked, err)
	}
	if err := store.ClearTimerCompatibility(ctx, "timer", blocked[0].Generation, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale revalidation cleared the block: %v", err)
	}
	if err := store.ClearTimerCompatibility(ctx, "timer", blocked[0].Generation, diagnostic); err != nil {
		t.Fatal(err)
	}
	_, resumed, err := store.ClaimDueTimer(ctx, "other", clock, time.Minute)
	if err != nil || resumed.Generation <= claim.Generation {
		t.Fatalf("revalidated timer was not reclaimed with a new fence: %+v, %v", resumed, err)
	}
	if err := store.BlockTimerCompatibility(ctx, resumed, clock, diagnostic); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearTimerCompatibility(ctx, "timer", blocked[0].Generation, diagnostic); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("old revalidation cleared a newer identical block: %v", err)
	}
	latest, err := store.BlockedTimers(ctx)
	if err != nil || len(latest) != 1 {
		t.Fatalf("new compatibility block was lost: %+v, %v", latest, err)
	}
	if err := store.ClearTimerCompatibility(ctx, "timer", latest[0].Generation, diagnostic); err != nil {
		t.Fatal(err)
	}
	_, final, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTimer(ctx, final, clock); err != nil {
		t.Fatal(err)
	}
}
