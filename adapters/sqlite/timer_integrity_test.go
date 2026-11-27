package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerQuarantineRequiresFreshAuthorizedResolution(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	defer store.Close()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	schedule := func(id string) transition.TimerRecord {
		t.Helper()
		record, _, err := store.ScheduleTimer(ctx, id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
				TimerReceipt: transition.TimerReceipt{ID: id, WorkflowID: "workflow", Deadline: now},
				Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	original := schedule("a")
	schedule("b")
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = 'a'`, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("corruption not reported: %v", err)
	}
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(_ context.Context, action, actor, reason string, findingID, locator int64) error {
		if actor != "authorized" {
			return errors.New("denied")
		}
		if action == "resolve" && (findingID <= 0 || locator <= 0) || action == "audit" && findingID <= 0 {
			return errors.New("missing authorized scope")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	if _, err := maintenance.Findings(ctx, "denied", "investigate"); err == nil {
		t.Fatal("unauthorized inspection succeeded")
	}
	findings, err := maintenance.Findings(ctx, "authorized", "investigate")
	if err != nil || len(findings) != 1 {
		t.Fatalf("inspect: %+v %v", findings, err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = 'a'`, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = 'a'`, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("ABA observation accepted: %v", err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = 'a'`, encoded); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale observation accepted: %v", err)
	}
	if _, _, err := store.ScheduleTimer(ctx, "a", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		t.Fatal("quarantine bypassed")
		return nil, nil
	}); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("schedule bypassed quarantine: %v", err)
	}
	record, _, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil || record.ID != "b" {
		t.Fatalf("independent timer blocked: %+v %v", record, err)
	}
	findings, err = maintenance.Findings(ctx, "authorized", "recheck")
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "denied", "restore", findings[0]); err == nil {
		t.Fatal("unauthorized resolution succeeded")
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); err != nil {
		t.Fatalf("identical resolution replay failed: %v", err)
	}
	audit, err := maintenance.Audit(ctx, "authorized", "review", findings[0].ID)
	if err != nil || len(audit) != 1 || audit[0].Action != "resolve" || audit[0].Actor != "authorized" || audit[0].Reason != "restore" {
		t.Fatalf("resolution audit = %+v, error = %v", audit, err)
	}
	record, _, err = store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil || record.ID != "a" {
		t.Fatalf("restored timer not claimable: %+v %v", record, err)
	}
}

func TestTimerQuarantineBlocksLeaseReportsAndMaintenanceSurvivesFailedOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	record, _, err := store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt: transition.TimerReceipt{ID: "timer", WorkflowID: "workflow", Deadline: now},
			Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	record.Generation, record.Owner, record.LeaseExpiresAt = lease.Generation, lease.Owner, lease.ExpiresAt
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
	if err := store.CompleteTimer(ctx, lease, clock); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("corrupt completion did not record finding: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{}); !errors.Is(err, sqlite.ErrUnsupportedSchema) {
		t.Fatalf("normal initialization accepted corruption: %v", err)
	}
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || len(findings) != 1 {
		t.Fatalf("worker-free inspection: %+v %v", findings, err)
	}
	if err := maintenance.Resolve(ctx, "operator", "resolve", findings[0]); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("unreadable record resolved: %v", err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, good); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.RenewTimer(ctx, lease, clock, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("renewal bypassed quarantine: %v", err)
	}
	if err := store.CompleteTimer(ctx, lease, clock); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("completion bypassed quarantine: %v", err)
	}
	diagnostic := transition.CompatibilityDiagnostic{Component: transition.CompatibilityEventCodec, Name: "event", Version: "1"}
	if err := store.BlockTimerCompatibility(ctx, lease, clock, diagnostic); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("compatibility block bypassed quarantine: %v", err)
	}
	if err := store.ClearTimerCompatibility(ctx, lease.ID, lease.Generation, diagnostic); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("compatibility clear bypassed quarantine: %v", err)
	}
	findings, err = maintenance.Findings(ctx, "operator", "recheck")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_audit BEFORE INSERT ON rff_integrity_audit BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "operator", "resolve", findings[0]); err == nil {
		t.Fatal("resolution succeeded without audit")
	}
	findings, err = maintenance.Findings(ctx, "operator", "inspect")
	if err != nil || findings[0].ResolvedAt != nil {
		t.Fatalf("failed audit cleared quarantine: %+v %v", findings, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_resolution BEFORE UPDATE ON rff_integrity_findings BEGIN SELECT RAISE(FAIL, 'resolution unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "operator", "resolve", findings[0]); err == nil {
		t.Fatal("resolution succeeded despite rejected mutation")
	}
	audit, err := maintenance.Audit(ctx, "operator", "inspect", findings[0].ID)
	if err != nil || len(audit) != 0 {
		t.Fatalf("failed resolution retained audit: %+v %v", audit, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_resolution`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "operator", "resolve", findings[0]); err != nil {
		t.Fatal(err)
	}
}

func TestTimerCorruptionCreatesSafeFinding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := "private-timer-id"
	_, _, err = store.ScheduleTimer(ctx, id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{
			SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt:  transition.TimerReceipt{ID: id, WorkflowID: "private-workflow", Deadline: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
			Event:         transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = ?`, []byte("private-payload"), id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ScheduleTimer(ctx, id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		t.Fatal("decision called for corrupt timer")
		return nil, nil
	}); err == nil {
		t.Fatal("corrupt timer accepted")
	}
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "operator", "investigate")
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings = %+v, error = %v", findings, err)
	}
	if findings[0].Locator == 0 || findings[0].Category != "invalid_timer" || findings[0].Field != "record" || findings[0].ResolvedAt != nil {
		t.Fatalf("unexpected finding: %+v", findings[0])
	}
	encoded, err := json.Marshal(findings[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{id, "private-workflow", "private-payload"} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("finding exposed sensitive data")
		}
	}
}
