package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
)

func TestOldSchemaIsRejectedWithoutMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
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
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `UPDATE rff_schema_version SET version = 11`); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err == nil {
		store.Close()
		t.Fatal("old schema accepted")
	}
	if !errors.Is(err, sqlite.ErrUnsupportedSchema) {
		t.Fatalf("unexpected error: %v", err)
	}
	var version int
	if err := db.QueryRowContext(context.Background(), `SELECT version FROM rff_schema_version`).Scan(&version); err != nil || version != 11 {
		t.Fatalf("schema version changed: %d, %v", version, err)
	}
}
