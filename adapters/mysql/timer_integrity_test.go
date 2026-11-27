package mysql_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerIntegrityMaintenance(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	schedule := func(id string) {
		t.Helper()
		if _, _, err := store.ScheduleTimer(ctx, id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
				TimerReceipt: transition.TimerReceipt{ID: id, WorkflowID: "private-workflow", Deadline: now},
				Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	schedule("a")
	schedule("b")
	record, lease, err := store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	good, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = ?`, []byte("private-payload"), "a"); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTimer(ctx, lease, clock); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("corruption not reported: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("normal initialization accepted corruption: %v", err)
	}
	maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(_ context.Context, action, actor, reason string, findingID, locator int64) error {
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
	if _, err := maintenance.Findings(ctx, "denied", "inspect"); err == nil {
		t.Fatal("unauthorized inspection succeeded")
	}
	findings, err := maintenance.Findings(ctx, "authorized", "inspect")
	if err != nil || len(findings) != 1 || findings[0].Category != "invalid_timer" || findings[0].Locator <= 0 {
		t.Fatalf("unsafe or unavailable findings: %+v %v", findings, err)
	}
	safe, err := json.Marshal(findings[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private-workflow", "private-payload"} {
		if strings.Contains(string(safe), private) {
			t.Fatal("finding exposed sensitive data")
		}
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("unreadable record resolved: %v", err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = ?`, good, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = ?`, []byte("private-payload"), "a"); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("ABA observation accepted: %v", err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ? WHERE id = ?`, good, "a"); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale observation accepted: %v", err)
	}
	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CompleteTimer(ctx, lease, clock); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("completion bypassed quarantine: %v", err)
	}
	if _, err := store.RenewTimer(ctx, lease, clock, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("renewal bypassed quarantine: %v", err)
	}
	diagnostic := transition.CompatibilityDiagnostic{Component: transition.CompatibilityEventCodec, Name: "event", Version: "1"}
	if err := store.BlockTimerCompatibility(ctx, lease, clock, diagnostic); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("compatibility block bypassed quarantine: %v", err)
	}
	if err := store.ClearTimerCompatibility(ctx, lease.ID, lease.Generation, diagnostic); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("compatibility clear bypassed quarantine: %v", err)
	}
	record, _, err = store.ClaimDueTimer(ctx, "worker", clock, time.Minute)
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
	if _, err := db.Exec(`CREATE TRIGGER reject_audit BEFORE INSERT ON rff_integrity_audit FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'audit unavailable'`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); err == nil {
		t.Fatal("resolution succeeded without audit")
	}
	stillOpen, err := maintenance.Findings(ctx, "authorized", "inspect")
	if err != nil || len(stillOpen) != 1 || stillOpen[0].ResolvedAt != nil {
		t.Fatalf("failed audit cleared quarantine: %+v %v", stillOpen, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_resolution BEFORE UPDATE ON rff_integrity_findings FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'resolution unavailable'`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); err == nil {
		t.Fatal("resolution succeeded despite rejected mutation")
	}
	emptyAudit, err := maintenance.Audit(ctx, "authorized", "inspect", findings[0].ID)
	if err != nil || len(emptyAudit) != 0 {
		t.Fatalf("failed resolution retained audit: %+v %v", emptyAudit, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_resolution`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Resolve(ctx, "authorized", "restore", findings[0]); err != nil {
		t.Fatalf("identical resolution replay failed: %v", err)
	}
	audit, err := maintenance.Audit(ctx, "authorized", "review", findings[0].ID)
	if err != nil || len(audit) != 1 || audit[0].Actor != "authorized" || audit[0].Action != "resolve" {
		t.Fatalf("audit = %+v, error = %v", audit, err)
	}
	if err := store.CompleteTimer(ctx, lease, clock); err != nil {
		t.Fatalf("valid claim rejected after resolution: %v", err)
	}
}

func TestTimerIntegrityRejectsIncompatibleRevisionDefault(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	defer store.Close()
	if _, err := db.Exec(`ALTER TABLE rff_timers ALTER COLUMN revision SET DEFAULT 0`); err != nil {
		t.Fatal(err)
	}
	called := false
	_, _, err = store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		called = true
		return nil, nil
	})
	if err == nil || called {
		t.Fatalf("unsafe revision default accepted: called=%v error=%v", called, err)
	}
}

func TestTimerIntegrityRejectsUnavailableFindingConstraint(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, _, err = store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt: transition.TimerReceipt{ID: "timer", WorkflowID: "workflow", Deadline: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
			Event:        transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "1"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = ?`, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE rff_integrity_findings DROP CHECK rff_finding_kind`); err != nil {
		t.Fatal(err)
	}
	called := false
	_, _, err = store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		called = true
		return nil, nil
	})
	if err == nil || called {
		t.Fatalf("unsafe finding storage accepted: called=%v error=%v", called, err)
	}
	maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(context.Context, string, string, string, int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	if _, err := maintenance.Findings(ctx, "operator", "inspect"); err == nil {
		t.Fatal("unavailable finding storage reported as healthy")
	}
}
