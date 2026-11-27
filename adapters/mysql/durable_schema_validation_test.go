package mysql_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestSelectionSchemaFailuresDoNotRewriteData(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"malformed projection", `ALTER TABLE rff_inputs MODIFY outcome_present BIGINT UNSIGNED NOT NULL`},
		{"missing index", `ALTER TABLE rff_timers DROP INDEX rff_timer_deadline_lookup`},
		{"malformed index", `ALTER TABLE rff_timers DROP INDEX rff_timer_deadline_lookup, ADD INDEX rff_timer_deadline_lookup (id)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, config := newMySQLDatabase(t)
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = store.AcceptInput(ctx, "retained", func(*transition.InputRecord) (*transition.InputRecord, error) {
				return &transition.InputRecord{
					SchemaVersion: 1, ID: "retained", WorkflowID: "workflow", Channel: "channel",
					Kind: transition.Command, Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}},
				}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			beforeInputs, beforeTimers := tableDDL(t, db, "rff_inputs"), tableDDL(t, db, "rff_timers")
			var beforeRecord []byte
			if err := db.QueryRowContext(ctx, `SELECT record FROM rff_inputs WHERE id = ?`, "retained").Scan(&beforeRecord); err != nil {
				t.Fatal(err)
			}
			store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if !errors.Is(err, mysql.ErrUnsupportedSchema) {
				if err == nil {
					store.Close()
				}
				t.Fatalf("malformed schema accepted: %v", err)
			}
			var afterRecord []byte
			if err := db.QueryRowContext(ctx, `SELECT record FROM rff_inputs WHERE id = ?`, "retained").Scan(&afterRecord); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(beforeRecord, afterRecord) || beforeInputs != tableDDL(t, db, "rff_inputs") ||
				beforeTimers != tableDDL(t, db, "rff_timers") {
				t.Fatal("failed initialization changed schema or retained authority")
			}
		})
	}
}

func tableDDL(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var name, ddl string
	if err := db.QueryRowContext(context.Background(), "SHOW CREATE TABLE `"+table+"`").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	return ddl
}

func TestOpenRejectsMalformedDurableIdentitySchema(t *testing.T) {
	for _, statement := range []string{
		`ALTER TABLE rff_inputs DROP INDEX rff_input_id_lookup`,
		`ALTER TABLE rff_timers MODIFY row_id BIGINT UNSIGNED NOT NULL`,
	} {
		t.Run(statement, func(t *testing.T) {
			db, config := newMySQLDatabase(t)
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
			store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if !errors.Is(err, mysql.ErrUnsupportedSchema) {
				if err == nil {
					store.Close()
				}
				t.Fatalf("malformed schema accepted: %v", err)
			}
		})
	}
}
