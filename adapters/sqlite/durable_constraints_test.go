package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
)

func TestDurableIdentityByteConstraints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.db")
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
	for _, table := range []string{"rff_timers", "rff_inputs"} {
		query := "INSERT INTO " + table + " (id, record) VALUES (?, ?)"
		if table == "rff_timers" {
			query = "INSERT INTO rff_timers (id, deadline_seconds, deadline_nanoseconds, completed, compatibility_blocked, record) VALUES (?, 0, 0, 0, 0, ?)"
		}
		for _, id := range []string{"a\x00b", "a\x00c", strings.Repeat("x", 256)} {
			if _, err := db.ExecContext(context.Background(), query, id, []byte("{}")); err != nil {
				t.Fatalf("%s insert %q: %v", table, id, err)
			}
		}
		for _, id := range []string{"", "a\x00b", "a\x00" + strings.Repeat("x", 255)} {
			if _, err := db.ExecContext(context.Background(), query, id, []byte("{}")); err == nil {
				t.Fatalf("%s accepted duplicate or invalid ID %q", table, id)
			}
		}
	}
}
