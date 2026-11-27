package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/selectionevidence"
)

func TestSelectionHandoffPlans(t *testing.T) {
	path := filepath.Join(t.TempDir(), "selection-plans.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	selectionevidence.Run(t, db, store, "sqlite", func(id string, now time.Time) {
		createNotificationWork(t, context.Background(), store, id, now)
	})
}
