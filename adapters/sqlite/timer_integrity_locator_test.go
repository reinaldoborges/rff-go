package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerLocatorCannotReuseFindingHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	schedule := func(id string) (transition.TimerRecord, bool, error) {
		return store.ScheduleTimer(ctx, id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
				TimerReceipt: transition.TimerReceipt{ID: id, WorkflowID: "workflow", Deadline: now},
				Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
		})
	}
	record, _, err := schedule("old")
	if err != nil {
		t.Fatal(err)
	}
	good, err := json.Marshal(record)
	if err != nil {
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
	if _, _, err := schedule("old"); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("detect corruption: %v", err)
	}
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, good); err != nil {
		t.Fatal(err)
	}
	history, err := maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || len(history) != 1 {
		t.Fatalf("findings: %+v %v", history, err)
	}
	if err := maintenance.Resolve(ctx, "operator", "restore", history[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM rff_timers; UPDATE sqlite_sequence SET seq = 0 WHERE name = 'rff_timers'`); err != nil {
		t.Fatal(err)
	}
	_, created, err := schedule("new")
	if err == nil && created {
		if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, []byte("bad")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := schedule("new"); !errors.Is(err, transition.ErrIntegrity) {
			t.Fatalf("detect new corruption: %v", err)
		}
		newHistory, err := maintenance.Findings(ctx, "operator", "inspect")
		if err != nil || len(newHistory) != 2 || newHistory[1].Locator == history[0].Locator {
			t.Fatalf("reused protected locator: %+v %v", newHistory, err)
		}
	} else if err == nil {
		t.Fatal("new timer neither created nor rejected")
	} else if _, _, err := store.ClaimDueTimer(ctx, "worker", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("rejected allocation left timer work: %v", err)
	}
}
