package mysql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestContinuationSchemaReopensWithCurrentTimer(t *testing.T) {
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = store.ScheduleTimer(context.Background(), "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{
			SchemaVersion: transition.CurrentTimerSchemaVersion, ID: "timer", WorkflowID: "workflow",
			Deadline: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			Event:    transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatalf("reopen with current timer: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
}

func TestCombinedSchemaRejectsIncompatibleVersionTwo(t *testing.T) {
	for _, missing := range []string{"rff_operations", "rff_continuations"} {
		t.Run(missing, func(t *testing.T) {
			db, config := newMySQLDatabase(t)
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `DROP TABLE `+missing); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `UPDATE rff_schema_version SET version = 2`); err != nil {
				t.Fatal(err)
			}
			if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
				t.Fatalf("incompatible version-two schema was not rejected: %v", err)
			}
			var version, count int
			if err := db.QueryRowContext(t.Context(), `SELECT version FROM rff_schema_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, missing).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if version != 2 || count != 0 {
				t.Fatalf("incompatible schema was modified: version=%d table count=%d", version, count)
			}
		})
	}
}
