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
