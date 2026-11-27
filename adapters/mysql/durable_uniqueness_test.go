package mysql_test

import (
	"strings"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
)

func TestExecutionIdentityIndexesAreUniqueAndFullValue(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, query := range []string{
		`INSERT INTO rff_executions (id, entity, request_key, record) VALUES (?, 'entity', 'key', '{}')`,
		`INSERT INTO rff_active (execution_id, entity) VALUES (?, 'entity')`,
		`INSERT INTO rff_hook_work (execution_id, work) VALUES (?, '{}')`,
		`INSERT INTO rff_notification_work (execution_id, work) VALUES (?, '{}')`,
	} {
		first, second := strings.Repeat("x", 255)+"a", strings.Repeat("x", 255)+"b"
		for _, id := range []string{first, second} {
			if _, err := db.ExecContext(t.Context(), query, id); err != nil {
				t.Fatalf("%s distinct ID %q: %v", query, id, err)
			}
		}
		if _, err := db.ExecContext(t.Context(), query, first); err == nil {
			t.Fatalf("%s accepted duplicate ID", query)
		}
	}
}
