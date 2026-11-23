package mysql_test

import (
	"context"
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
