package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestHiddenTimerProjectionCanChangeEarliestSelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hidden-timer.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	runtimecontract.CheckHiddenTimerProjection(t, store, db)
}

func TestTimerProjectionMismatchIsVisibleAndAuthorizedRepairRebuildsIt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "selection-integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 1, 1, 12, 0, 0, 123456789, time.UTC)
	scheduler, err := transition.NewTimerScheduler[string](store,
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "selected", WorkflowID: "workflow", Deadline: now, Event: "event",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}

	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `UPDATE rff_timers SET deadline_seconds = deadline_seconds - 1 WHERE id = 'selected'`); err != nil {
		t.Fatalf("seed projection mismatch: %v", err)
	}
	if _, _, err := store.ClaimDueTimer(ctx, "worker", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("candidate mismatch was not reported: %v", err)
	}

	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(_ context.Context, action, actor, _ string, findingID, locator int64) error {
		if actor != "authorized" || action == "resolve" && (findingID <= 0 || locator <= 0) {
			return errors.New("unauthorized maintenance")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "authorized", "inspect projection mismatch")
	if err != nil || len(findings) != 1 || findings[0].Kind != "timer" || findings[0].Category != "invalid_timer" {
		t.Fatalf("projection mismatch finding = %+v, %v", findings, err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE rff_timers SET deadline_seconds = deadline_seconds - 1 WHERE id = 'selected'`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "stale repair", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale projection observation was accepted: %v", err)
	}
	findings, err = maintenance.Findings(ctx, "authorized", "refresh projection observation")
	if err != nil || len(findings) != 1 {
		t.Fatalf("refreshed projection finding = %+v, %v", findings, err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "rebuild projection from timer record", findings[0]); err != nil {
		t.Fatalf("authorized projection repair: %v", err)
	}
	record, lease, err := store.ClaimDueTimer(ctx, "worker", func() time.Time { return now }, time.Minute)
	if err != nil || record.ID != "selected" || lease.Generation != 1 {
		t.Fatalf("repaired timer was not claimable from authority: record=%+v lease=%+v error=%v", record, lease, err)
	}
}

func TestTimerClaimRollsBackWhenAuthorityWriteFails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "selection-rollback.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	scheduler, err := transition.NewTimerScheduler[string](store,
		func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "rollback", WorkflowID: "workflow", Deadline: now, Event: "event",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `CREATE TRIGGER reject_timer_claim BEFORE UPDATE OF record ON rff_timers BEGIN SELECT RAISE(FAIL, 'claim rejected'); END`); err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return now }
	if _, _, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute); err == nil {
		t.Fatal("claim succeeded despite rejected authoritative write")
	}
	if _, err := admin.ExecContext(ctx, `DROP TRIGGER reject_timer_claim`); err != nil {
		t.Fatal(err)
	}
	record, lease, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil || record.ID != "rollback" || lease.Generation != 1 {
		t.Fatalf("failed claim left a partial generation: record=%+v lease=%+v error=%v", record, lease, err)
	}
}

func TestTimerStoreRejectsMissingDeadlineIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "selection-index.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`DROP INDEX rff_timer_deadline_lookup`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{}); !errors.Is(err, sqlite.ErrUnsupportedSchema) {
		t.Fatalf("store opened without required timer deadline index: %v", err)
	}
}
