package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTransitionLeaseReclaimSurvivesReopenAndRejectsStaleRenewal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "leases.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Status: transition.Waiting,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator-a", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator-a", fixedClock(now.Add(time.Second)), time.Minute)
	if err != nil || replayed.Generation != first.Generation || !replayed.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("acquisition retry changed lease: lease=%+v error=%v", replayed, err)
	}
	if _, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator-b", fixedClock(now.Add(time.Second)), time.Minute); !errors.Is(err, transition.ErrLeaseHeld) {
		t.Fatalf("unexpired lease must not be stolen: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	second, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator-b", fixedClock(now.Add(time.Minute)), time.Minute)
	if err != nil || second.Generation <= first.Generation {
		t.Fatalf("reclaim did not advance fence: first=%+v second=%+v error=%v", first, second, err)
	}
	if _, err := store.RenewTransitionLease(ctx, first, fixedClock(now.Add(time.Minute)), time.Minute); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale coordinator renewal must fail: %v", err)
	}
	renewed, err := store.RenewTransitionLease(ctx, second, fixedClock(now.Add(90*time.Second)), time.Minute)
	if err != nil || renewed.Generation != second.Generation || !renewed.ExpiresAt.Equal(now.Add(150*time.Second)) {
		t.Fatalf("current lease renewal failed: lease=%+v error=%v", renewed, err)
	}
}

func TestVersionOneStoreMigratesWithoutLosingAcceptance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "migration.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Status: transition.Waiting,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE rff_hook_work; DROP TABLE rff_transition_leases; UPDATE rff_schema_version SET version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	record, created, err := store.Accept(ctx, "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing == nil || !view.Active {
			t.Fatal("migration lost accepted execution or reservation")
		}
		return nil, nil
	})
	if err != nil || created || record.ID != "execution" {
		t.Fatalf("migration changed acceptance: record=%+v created=%v error=%v", record, created, err)
	}
	if _, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), time.Minute); err != nil {
		t.Fatalf("migrated store cannot acquire a lease: %v", err)
	}

}

func fixedClock(now time.Time) func() time.Time {
	return func() time.Time { return now }
}
