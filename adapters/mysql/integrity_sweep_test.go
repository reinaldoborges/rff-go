package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestIntegritySweepResumesBoundedCoverageAndContinuesPastFindings(t *testing.T) {
	ctx := context.Background()
	store, dsn, admin := openSweepStore(t)
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
	if _, err := admin.ExecContext(ctx, `UPDATE rff_inputs SET channel = X'00' WHERE id = 'input-2'`); err != nil {
		t.Fatal(err)
	}
	openMaintenance := func() *mysql.IntegrityMaintenance {
		t.Helper()
		return openSweepMaintenance(t, dsn)
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
	t.Cleanup(func() {
		if err := maintenance.Close(); err != nil {
			t.Error(err)
		}
	})
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

func TestIntegritySweepIgnoresUnlinkedHistoricalExecutionAndReservation(t *testing.T) {
	ctx := context.Background()
	store, dsn, admin := openSweepStore(t)
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	createMySQLNotificationWork(t, ctx, store, "selected", "ready", now)
	if _, err := admin.ExecContext(ctx, `UPDATE rff_executions SET record = X'00' WHERE id = 'selected'`); err != nil {
		t.Fatal(err)
	}
	record := []byte(`{"SchemaVersion":1,"ID":"historical","Entity":"AQ==","Key":"request","Status":"Complete"}`)
	if _, err := admin.ExecContext(ctx, `INSERT INTO rff_executions
		(id, entity, request_key, record, selection_status, selection_compatibility)
		VALUES ('historical', X'01', 'request', ?, 'Complete', 0)`, record); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO rff_active (entity, execution_id)
		VALUES (X'03', 'historical')`); err != nil {
		t.Fatal(err)
	}

	maintenance := openSweepMaintenance(t, dsn)
	defer maintenance.Close()
	progress, err := maintenance.RunIntegritySweep(ctx, "authorized", "scheduled check", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !progress.Complete || progress.Healthy {
		t.Fatalf("linked corruption was not inspected or the sweep did not complete: %+v", progress)
	}
	for _, coverage := range progress.Coverage {
		if coverage.Kind == "execution" || coverage.Kind == "reservation" {
			t.Fatalf("sweep reported coverage outside selected work: %+v", progress.Coverage)
		}
	}
	findings, err := maintenance.Findings(ctx, "authorized", "scheduled check")
	if err != nil || len(findings) != 1 || findings[0].Kind != "execution" {
		t.Fatalf("sweep did not limit findings to linked work: %+v, %v", findings, err)
	}
}

func TestIntegritySweepCheckpointFailureDoesNotCommitFindingOrProgress(t *testing.T) {
	ctx := context.Background()
	store, dsn, admin := openSweepStore(t)
	_, _, err := store.AcceptInput(ctx, "input", func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1, ID: "input", Channel: "channel", WorkflowID: "workflow",
			Kind: transition.Command, Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE rff_inputs SET channel = X'00' WHERE id = 'input'`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE TRIGGER fail_sweep_checkpoint
		BEFORE UPDATE ON rff_integrity_sweep_progress FOR EACH ROW
		SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'checkpoint persistence failed'`); err != nil {
		t.Fatal(err)
	}
	maintenance := openSweepMaintenance(t, dsn)
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
}

func TestIntegritySweepTreatsUnknownTimerBehaviorAsIncompleteNotCorrupt(t *testing.T) {
	ctx := context.Background()
	store, dsn, _ := openSweepStore(t)
	now := time.Date(2026, 12, 1, 12, 0, 0, 0, time.UTC)
	_, _, err := store.ScheduleTimer(ctx, "timer", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
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
	maintenance := openSweepMaintenance(t, dsn)
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
	store, dsn, _ := openSweepStore(t)
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
	first := openSweepMaintenance(t, dsn)
	defer first.Close()
	second := openSweepMaintenance(t, dsn)
	defer second.Close()
	type result struct {
		progress transition.IntegritySweepProgress
		err      error
	}
	results := make(chan result, 2)
	for _, maintenance := range []*mysql.IntegrityMaintenance{first, second} {
		go func(maintenance *mysql.IntegrityMaintenance) {
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

func TestIntegritySweepSchemaUpgradesExistingMySQLStore(t *testing.T) {
	admin, config := newMySQLDatabase(t)
	dsn := config.FormatDSN()
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"rff_integrity_sweep_progress", "rff_integrity_sweep_state"} {
		if _, err := admin.ExecContext(t.Context(), `DROP TABLE `+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(t.Context(), `UPDATE rff_schema_version SET version = 9`); err != nil {
		t.Fatal(err)
	}
	store, err = mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatalf("upgrade existing store for integrity sweeps: %v", err)
	}
	defer store.Close()
	var version int
	if err := admin.QueryRowContext(t.Context(), `SELECT version FROM rff_schema_version`).Scan(&version); err != nil || version != 10 {
		t.Fatalf("integrity sweep schema version = %d, %v", version, err)
	}
}

func openSweepStore(t *testing.T) (*mysql.Store[string, string], string, *sql.DB) {
	t.Helper()
	admin, config := newMySQLDatabase(t)
	dsn := config.FormatDSN()
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, dsn, admin
}

func openSweepMaintenance(t *testing.T, dsn string) *mysql.IntegrityMaintenance {
	t.Helper()
	maintenance, err := mysql.OpenIntegrityMaintenance(dsn, func(_ context.Context, action, actor, reason string, _, _ int64) error {
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

func inputSweepCoverage(progress transition.IntegritySweepProgress) transition.IntegritySweepCoverage {
	for _, coverage := range progress.Coverage {
		if coverage.Kind == "input" {
			return coverage
		}
	}
	return transition.IntegritySweepCoverage{}
}
