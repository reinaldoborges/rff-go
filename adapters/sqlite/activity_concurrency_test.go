package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestConcurrentWorkersClaimOnlyOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "claim.db")
	first, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := errors.Join(first.Close(), second.Close()); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, err = first.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Status: transition.Waiting,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := first.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, store := range []*sqlite.Store[string, string]{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.ClaimHook(ctx, "worker", fixedClock(now), time.Minute)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success, unavailable := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, transition.ErrNoHookActivity):
			unavailable++
		default:
			t.Errorf("unexpected claim error: %v", err)
		}
	}
	if success != 1 || unavailable != 1 {
		t.Fatalf("successful=%d unavailable=%d", success, unavailable)
	}
}
