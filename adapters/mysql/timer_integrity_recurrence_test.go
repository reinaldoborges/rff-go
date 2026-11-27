package mysql_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerIdentityMismatchRecursWithHistoryAndConcurrentDetection(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	other, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	record, _, err := store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt: transition.TimerReceipt{ID: "timer", WorkflowID: "workflow", Deadline: now},
			Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	good, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	record.ID = "different"
	bad, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, bad); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, current := range []*mysql.Store[string, string]{store, other} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := current.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
				return nil, errors.New("decision must not run")
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, transition.ErrIntegrity) {
			t.Fatalf("concurrent detection: %v", err)
		}
	}
	maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || len(findings) != 1 {
		t.Fatalf("concurrent finding deduplication: %+v %v", findings, err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, good); err != nil {
		t.Fatal(err)
	}
	findings, err = maintenance.Findings(ctx, "operator", "recheck")
	if err != nil {
		t.Fatal(err)
	}
	otherMaintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer otherMaintenance.Close()
	resolutions := make(chan error, 2)
	for i, current := range []*mysql.IntegrityMaintenance{maintenance, otherMaintenance} {
		reason := []string{"restore", "competing decision"}[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			resolutions <- current.Resolve(ctx, "operator", reason, findings[0])
		}()
	}
	wg.Wait()
	close(resolutions)
	var succeeded, conflicted int
	for err := range resolutions {
		if err == nil {
			succeeded++
		} else if errors.Is(err, transition.ErrConflict) {
			conflicted++
		} else {
			t.Fatalf("concurrent resolution: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("concurrent resolutions: successes=%d conflicts=%d", succeeded, conflicted)
	}
	audit, err := maintenance.Audit(ctx, "operator", "review", findings[0].ID)
	if err != nil || len(audit) != 1 {
		t.Fatalf("concurrent resolution audit: %+v %v", audit, err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, bad); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ClaimDueTimer(ctx, "worker", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("recurring corruption not reported: %v", err)
	}
	history, err := maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || len(history) != 2 || history[0].ResolvedAt == nil || history[1].ResolvedAt != nil ||
		history[0].Locator != history[1].Locator || history[0].ID == history[1].ID {
		t.Fatalf("recurrence history: %+v %v", history, err)
	}
	page, err := maintenance.FindingsPage(ctx, "operator", "inspect", 0, 1)
	if err != nil || len(page) != 1 || page[0].ID != history[0].ID {
		t.Fatalf("first finding page: %+v %v", page, err)
	}
	page, err = maintenance.FindingsPage(ctx, "operator", "inspect", page[0].ID, 1)
	if err != nil || len(page) != 1 || page[0].ID != history[1].ID {
		t.Fatalf("second finding page: %+v %v", page, err)
	}
	if _, err := db.Exec(`DELETE FROM rff_timers`); err != nil {
		t.Fatal(err)
	}
	history, err = maintenance.Findings(ctx, "operator", "inspect missing")
	if err != nil || len(history) != 2 {
		t.Fatalf("inspect missing timer: %+v %v", history, err)
	}
	if err := maintenance.Resolve(ctx, "operator", "missing", history[1]); err == nil {
		t.Fatal("missing record cleared a finding")
	}
	called := false
	_, _, err = store.ScheduleTimer(ctx, "independent", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		called = true
		return nil, nil
	})
	if !errors.Is(err, transition.ErrIntegrity) || called {
		t.Fatalf("unknown scope was treated as safe: called=%v error=%v", called, err)
	}
}
