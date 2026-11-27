package mysql_test

import (
	"errors"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
)

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
