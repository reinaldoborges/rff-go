package sqlite_test

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
)

func TestUnsupportedSchemaFailsWithoutWriting(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"future adapter version", `UPDATE rff_schema_version SET version = 99`},
		{"incomplete tables", `DROP TABLE rff_active`},
		{"pre-operation store", `DROP TABLE rff_operations; UPDATE rff_schema_version SET version = 9`},
		{"continuation-only version ten", `DROP TABLE rff_operations; UPDATE rff_schema_version SET version = 10`},
		{"missing continuation table", `DROP TABLE rff_continuations`},
		{"missing unique constraint", `DROP TABLE rff_executions; CREATE TABLE rff_executions (id TEXT PRIMARY KEY NOT NULL, entity BLOB NOT NULL, request_key TEXT NOT NULL, record BLOB NOT NULL)`},
		{"unreadable record", `INSERT INTO rff_executions (id, entity, request_key, record) VALUES ('id', X'65', 'key', X'ff')`},
		{"future record version", `INSERT INTO rff_executions (id, entity, request_key, record) VALUES ('id', X'65', 'key', '{"SchemaVersion":99,"ID":"id","Key":"key"}')`},
		{"dangling reservation", `INSERT INTO rff_active (entity, execution_id) VALUES (X'65', 'missing')`},
		{"missing reservation", `INSERT INTO rff_executions (id, entity, request_key, record) VALUES ('id', X'65', 'key', '{"SchemaVersion":1,"ID":"id","Entity":"ZQ==","Key":"key","Status":"Waiting"}')`},
		{"unreadable lease", `INSERT INTO rff_transition_leases (entity, execution_id, owner, generation, expires_at) VALUES (X'65', 'missing', 'owner', 0, 'not-a-time')`},
		{"unreadable hook work", `INSERT INTO rff_hook_work (
			execution_id, work, dispatch_kind, started, completed, available_at_seconds, available_at_nanoseconds, lease_owner_present
		) VALUES ('missing', X'ff', 'enter', 0, 0, 0, 0, 0)`},
		{"unreadable notification work", `INSERT INTO rff_notification_work (execution_id, work) VALUES ('missing', X'ff')`},
		{"unreadable timer", `INSERT INTO rff_timers (id, record) VALUES ('timer', X'ff')`},
		{"new timer record in old adapter schema", `UPDATE rff_schema_version SET version = 8;
			INSERT INTO rff_timers (id, record) VALUES ('timer',
			'{"SchemaVersion":2,"ID":"timer","WorkflowID":"workflow","Deadline":"2026-01-01T00:00:00Z","Event":{"Codec":{"Name":"event","Version":"1"}}}')`},
		{"unreadable input", `INSERT INTO rff_inputs (id, record) VALUES ('input', X'ff')`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schema.db")
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
			if _, err := db.Exec(tt.sql); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if !errors.Is(err, sqlite.ErrUnsupportedSchema) {
				if err == nil {
					store.Close()
				}
				t.Fatalf("expected unsupported schema, got %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("opening an unsupported store changed its database")
			}
		})
	}
}
