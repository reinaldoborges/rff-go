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

func TestIntegritySweepResumesBoundedCoverageAndContinuesPastFindings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity-sweep.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, id := range []string{"input-1", "input-2", "input-3"} {
		_, _, err := store.AcceptInput(ctx, id, func(*transition.InputRecord) (*transition.InputRecord, error) {
			return &transition.InputRecord{
				SchemaVersion: 1, ID: id, Channel: "channel", WorkflowID: "workflow",
				Kind: transition.Command, Value: transition.EncodedValue{
					Codec: transition.CodecVersion{Name: "input", Version: "1"},
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `UPDATE rff_inputs SET channel = X'00' WHERE id = 'input-2'`); err != nil {
		t.Fatal(err)
	}

	openMaintenance := func() *sqlite.IntegrityMaintenance {
		t.Helper()
		maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(_ context.Context, action, actor, reason string, _, _ int64) error {
			if (action != "sweep" && action != "inspect") || actor != "authorized" || reason != "scheduled check" {
				return errors.New("unauthorized integrity sweep")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return maintenance
	}
	maintenance := openMaintenance()
	first, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || inputSweepCoverage(first).Inspected != 1 {
		t.Fatalf("first bounded batch did not stop at one input: %+v", first)
	}
	if err := maintenance.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE rff_inputs SET channel = X'01' WHERE id = 'input-1'`); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.AcceptInput(ctx, "input-late", func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1, ID: "input-late", Channel: "channel", WorkflowID: "workflow",
			Kind: transition.Command, Value: transition.EncodedValue{
				Codec: transition.CodecVersion{Name: "input", Version: "1"},
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	maintenance = openMaintenance()
	defer maintenance.Close()
	second, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	if second.Healthy || second.Complete || inputSweepCoverage(second).Inspected != 2 {
		t.Fatalf("bad row did not produce visible, resumable progress: %+v", second)
	}
	third, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	coverage := inputSweepCoverage(third)
	if !third.Complete || third.Healthy || coverage.HighWaterMark != 3 ||
		coverage.Inspected != 3 || coverage.InspectedThrough != 3 {
		t.Fatalf("completed sweep reported incorrect inspected coverage: %+v", third)
	}
	findings, err := maintenance.Findings(ctx, "authorized", "scheduled check")
	if err != nil || len(findings) != 1 || findings[0].Kind != "input" || findings[0].Field != "projection" {
		t.Fatalf("sweep did not persist only the safe input finding: %+v, %v", findings, err)
	}
}

func inputSweepCoverage(progress transition.IntegritySweepProgress) transition.IntegritySweepCoverage {
	for _, coverage := range progress.Coverage {
		if coverage.Kind == "input" {
			return coverage
		}
	}
	return transition.IntegritySweepCoverage{}
}

func TestIntegritySweepCheckpointFailureDoesNotCommitFindingOrProgress(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity-sweep-failure.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, _, err = store.AcceptInput(ctx, "input", func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1, ID: "input", Channel: "channel", WorkflowID: "workflow",
			Kind: transition.Command, Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `UPDATE rff_inputs SET channel = X'00' WHERE id = 'input'`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE TRIGGER fail_sweep_checkpoint
		BEFORE UPDATE OF inspected_through ON rff_integrity_sweep_progress
		BEGIN SELECT RAISE(ABORT, 'checkpoint persistence failed'); END`); err != nil {
		t.Fatal(err)
	}
	maintenance := openSweepMaintenance(t, path)
	defer maintenance.Close()
	if _, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1); err == nil {
		t.Fatal("checkpoint persistence failure was hidden")
	}
	findings, err := maintenance.Findings(ctx, "authorized", "scheduled check")
	if err != nil || len(findings) != 0 {
		t.Fatalf("finding escaped a rolled-back batch: %+v, %v", findings, err)
	}
	if _, err := admin.ExecContext(ctx, `DROP TRIGGER fail_sweep_checkpoint`); err != nil {
		t.Fatal(err)
	}
	progress, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Healthy || !progress.Complete || inputSweepCoverage(progress).Inspected != 1 {
		t.Fatalf("failed checkpoint advanced or skipped its observation: %+v", progress)
	}
	findings, err = maintenance.Findings(ctx, "authorized", "scheduled check")
	if err != nil || len(findings) != 1 || findings[0].Kind != "input" {
		t.Fatalf("retry did not persist the safe finding: %+v, %v", findings, err)
	}
}

func TestIntegritySweepTreatsUnknownTimerBehaviorAsIncompleteNotCorrupt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity-sweep-compatibility.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 12, 1, 12, 0, 0, 0, time.UTC)
	_, _, err = store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{
			SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt:  transition.TimerReceipt{ID: "timer", WorkflowID: "workflow", Deadline: now.Add(-time.Second)},
			Event:         transition.EncodedValue{Codec: transition.CodecVersion{Name: "event", Version: "new"}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := store.ClaimDueTimer(ctx, "timer-worker", func() time.Time { return now }, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := transition.CompatibilityDiagnostic{
		Component: transition.CompatibilityEventCodec, Name: "event", Version: "new",
	}
	if err := store.BlockTimerCompatibility(ctx, lease, func() time.Time { return now }, diagnostic); err != nil {
		t.Fatal(err)
	}
	maintenance := openSweepMaintenance(t, path)
	defer maintenance.Close()
	progress, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !progress.Complete || progress.Healthy || !progress.Incomplete {
		t.Fatalf("unknown behavior was not reported as incomplete coverage: %+v", progress)
	}
	findings, err := maintenance.Findings(ctx, "authorized", "scheduled check")
	if err != nil || len(findings) != 0 {
		t.Fatalf("unknown behavior was misclassified as corruption: %+v, %v", findings, err)
	}
}

func TestConcurrentIntegritySweepersShareOneDurableCursor(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity-sweep-concurrent.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, id := range []string{"input-1", "input-2", "input-3"} {
		_, _, err := store.AcceptInput(ctx, id, func(*transition.InputRecord) (*transition.InputRecord, error) {
			return &transition.InputRecord{
				SchemaVersion: 1, ID: id, Channel: "channel", WorkflowID: "workflow",
				Kind: transition.Command, Value: transition.EncodedValue{
					Codec: transition.CodecVersion{Name: "input", Version: "1"},
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	first := openSweepMaintenance(t, path)
	defer first.Close()
	second := openSweepMaintenance(t, path)
	defer second.Close()
	type result struct {
		progress transition.IntegritySweepProgress
		err      error
	}
	results := make(chan result, 2)
	for _, maintenance := range []*sqlite.IntegrityMaintenance{first, second} {
		go func(maintenance *sqlite.IntegrityMaintenance) {
			progress, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
			results <- result{progress: progress, err: err}
		}(maintenance)
	}
	for i := 0; i < 2; i++ {
		if result := <-results; result.err != nil {
			t.Fatal(result.err)
		}
	}
	progress, err := first.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	if inputSweepCoverage(progress).Inspected != 3 {
		t.Fatalf("concurrent sweepers duplicated or skipped a checkpoint: %+v", progress)
	}
}

func TestIntegritySweepSchemaUpgradesExistingSQLiteStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity-sweep-migration.db")
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
	if _, err := admin.ExecContext(ctx, `DROP TABLE rff_integrity_sweep_progress;
		DROP TABLE rff_integrity_sweep_state;
		UPDATE rff_schema_version SET version = 17`); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatalf("upgrade existing store for integrity sweeps: %v", err)
	}
	defer store.Close()
	var version int
	if err := admin.QueryRowContext(ctx, `SELECT version FROM rff_schema_version`).Scan(&version); err != nil || version != 18 {
		t.Fatalf("integrity sweep schema version = %d, %v", version, err)
	}
}

func openSweepMaintenance(t *testing.T, path string) *sqlite.IntegrityMaintenance {
	t.Helper()
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(_ context.Context, action, actor, reason string, _, _ int64) error {
		if (action != "sweep" && action != "inspect") || actor != "authorized" || reason != "scheduled check" {
			return errors.New("unauthorized integrity sweep")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return maintenance
}
