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

func TestStaleCoordinatorCannotDispatchHook(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "fence.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.AcquireTransitionLease(ctx, "entity", "execution", "old", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.AcquireTransitionLease(ctx, "entity", "execution", "new", fixedClock(now.Add(time.Minute)), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := transition.HookDispatch{Kind: transition.HookEnter, DueAt: now.Add(time.Minute)}
	if _, err := store.DispatchHook(ctx, old, fixedClock(now.Add(time.Minute)), dispatch); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale coordinator dispatched a hook: %v", err)
	}
	if _, err := store.DispatchHook(ctx, current, fixedClock(now.Add(time.Minute)), dispatch); err != nil {
		t.Fatalf("current coordinator could not dispatch: %v", err)
	}
}
