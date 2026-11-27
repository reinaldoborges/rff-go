package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
)

func TestOpenRejectsMissingIdentityIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-index.db")
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
	_, err = db.ExecContext(context.Background(), `PRAGMA writable_schema = ON;
		DELETE FROM sqlite_master WHERE type = 'index' AND name = 'sqlite_autoindex_rff_timers_1';
		PRAGMA writable_schema = OFF`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if !errors.Is(err, sqlite.ErrUnsupportedSchema) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("missing identity index accepted: %v", err)
	}
}
