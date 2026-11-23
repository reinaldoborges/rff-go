package sqlite_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
)

func TestContinuationAtomicContract(t *testing.T) {
	continuationcontract.RunAtomic(t, func(t *testing.T) (continuationcontract.AtomicStore, *sql.DB) {
		path := os.Getenv("RFF_SQLITE_ATOMIC_RESTART")
		if path == "" {
			path = filepath.Join(t.TempDir(), "atomic.db")
		}
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
		return store, db
	})
}
