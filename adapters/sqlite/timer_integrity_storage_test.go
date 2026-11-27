package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerFindingWriteFailureRetries(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	schedule := func() error {
		_, _, err := store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
				TimerReceipt: transition.TimerReceipt{ID: "timer", WorkflowID: "workflow", Deadline: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
				Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
		})
		return err
	}
	if err := schedule(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_finding BEFORE INSERT ON rff_integrity_findings BEGIN SELECT RAISE(FAIL, 'finding unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := schedule(); err == nil {
		t.Fatal("corrupt timer succeeded without finding storage")
	}
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || len(findings) != 0 {
		t.Fatalf("failed finding write left partial state: %+v %v", findings, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_finding`); err != nil {
		t.Fatal(err)
	}
	if err := schedule(); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("finding was not retried: %v", err)
	}
	findings, err = maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || len(findings) != 1 {
		t.Fatalf("retried finding: %+v %v", findings, err)
	}
}
