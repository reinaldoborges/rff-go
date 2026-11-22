package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestStoreMigrationPreservesTransitionLease(t *testing.T) {
	for _, migration := range []struct {
		name string
		sql  string
	}{
		{"version two", `DROP TABLE rff_notification_work; DROP TABLE rff_hook_work; UPDATE rff_schema_version SET version = 2`},
		{"version three", `DROP TABLE rff_notification_work; UPDATE rff_schema_version SET version = 3`},
		{"version four", `UPDATE rff_schema_version SET version = 4`},
	} {
		t.Run(migration.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "old.db")
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
			lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Minute)
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
			if _, err := db.Exec(migration.sql); err != nil {
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
			renewed, err := store.RenewTransitionLease(ctx, lease, fixedClock(now.Add(time.Second)), time.Minute)
			if err != nil || renewed.Generation != lease.Generation {
				t.Fatalf("migration lost transition fence: lease=%+v error=%v", renewed, err)
			}
		})
	}
}
