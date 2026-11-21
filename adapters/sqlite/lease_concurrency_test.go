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

func TestConcurrentTransitionLeaseAcquisitionHasOneOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shared-leases.db")
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
	_, _, err = first.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Status: transition.Waiting,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i, store := range []*sqlite.Store[string, string]{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.AcquireTransitionLease(ctx, "entity", "execution", []string{"a", "b"}[i], fixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), time.Minute)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success, held := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, transition.ErrLeaseHeld):
			held++
		default:
			t.Errorf("unexpected acquisition error: %v", err)
		}
	}
	if success != 1 || held != 1 {
		t.Fatalf("success=%d held=%d", success, held)
	}
}
