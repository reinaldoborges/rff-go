package mysql_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
)

func TestContinuationAtomicContract(t *testing.T) {
	continuationcontract.RunAtomic(t, func(t *testing.T) (continuationcontract.AtomicStore, *sql.DB) {
		if dsn := os.Getenv("RFF_MYSQL_ATOMIC_RESTART"); dsn != "" {
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
			return store, db
		}
		db, config := newMySQLDatabase(t)
		store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store, db
	})
}
