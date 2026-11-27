package mysql_test

import (
	"strings"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
)

func TestDurableIdentityKeysEnforceFullByteUniqueness(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, table := range []string{"rff_timers", "rff_inputs"} {
		first := strings.Repeat("x", 254) + "a"
		second := strings.Repeat("x", 254) + "b"
		for _, id := range []string{first, second, "Case", "case", "x\x00a", "x\x00b", "é", "e\u0301"} {
			query := "INSERT INTO " + table + " (id, record) VALUES (?, ?)"
			if table == "rff_timers" {
				query = "INSERT INTO rff_timers (id, deadline_seconds, deadline_nanoseconds, completed, compatibility_blocked, record) VALUES (?, 0, 0, 0, 0, ?)"
			}
			if _, err := db.ExecContext(t.Context(), query, id, []byte("{}")); err != nil {
				t.Fatalf("%s insert %q: %v", table, id, err)
			}
		}
		query := "INSERT INTO " + table + " (id, record) VALUES (?, ?)"
		if table == "rff_timers" {
			query = "INSERT INTO rff_timers (id, deadline_seconds, deadline_nanoseconds, completed, compatibility_blocked, record) VALUES (?, 0, 0, 0, 0, ?)"
		}
		if _, err := db.ExecContext(t.Context(), query, first, []byte("{}")); err == nil {
			t.Fatalf("%s accepted duplicate full ID", table)
		}
	}
}
