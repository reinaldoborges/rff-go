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

func TestContinuationMissingOrigin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-origin.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
	continuationcontract.RunMissingOrigin(t, store, db)
}

func TestContinuationHookIdentityCompatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hook-identity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
	continuationcontract.RunHookIdentityCompatibility(t, store, db)
}
