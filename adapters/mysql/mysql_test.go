package mysql_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestOpenRequiresDSNDatabaseAndCodecs(t *testing.T) {
	if _, err := mysql.Open[string, string]("", mysql.StringCodec{}, mysql.StringCodec{}); err == nil {
		t.Fatal("empty DSN was accepted")
	}
	if _, err := mysql.Open[string, string]("user:pass@tcp(localhost:3306)/", mysql.StringCodec{}, mysql.StringCodec{}); err == nil {
		t.Fatal("DSN without a selected database was accepted")
	}
	if _, err := mysql.Open[string, string]("user:pass@tcp(localhost:3306)/rff_test", nil, mysql.StringCodec{}); err == nil {
		t.Fatal("nil entity codec was accepted")
	}
}

type failedOperationResultStore struct {
	*mysql.Store[string, string]
}

func (s failedOperationResultStore) CompleteTrackedOperation(context.Context, transition.HookLease, func() time.Time, transition.OperationIdentity, transition.OperationResult) error {
	return errors.New("injected result persistence failure")
}

func TestOperationResultPersistenceFailureStaysUnknown(t *testing.T) {
	store := openMySQL(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "operation-result-failure", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "result-failure", Entity: "entity", Key: "operation-result-failure",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "result-failure", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	binding := transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "result-failure", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "1"},
	}
	equal := func(_ context.Context, left, right []byte) (bool, error) { return string(left) == string(right), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("operation dispatch marker: started=%v err=%v", started, err)
	}
	wrapped := failedOperationResultStore{Store: store}
	if err := wrapped.CompleteTrackedOperation(ctx, lease, clock, binding.Identity, transition.OperationResult{
		Codec: binding.ResultCodec, Data: []byte("receipt"),
	}); err == nil {
		t.Fatal("injected result persistence failure was ignored")
	}
	if result, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); !errors.Is(err, transition.ErrOperationUnresolved) || started || result != nil {
		t.Fatalf("failed persistence was treated as safe to repeat: result=%+v started=%v err=%v", result, started, err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || inspection.Result != nil || !inspection.MayHaveDispatched {
		t.Fatalf("uncertain operation state was not preserved: %+v %v", inspection, err)
	}
	if err := store.ReportHook(ctx, lease, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkWorkSuccess(ctx, fence, clock); !errors.Is(err, transition.ErrOperationUnresolved) {
		t.Fatalf("uncertain result persistence allowed work success: %v", err)
	}
}

func TestOpenRejectsPartialSchemaWithoutCreatingTables(t *testing.T) {
	adminDB, config := newMySQLDatabase(t)
	if _, err := adminDB.ExecContext(t.Context(), `CREATE TABLE rff_executions (id INT NOT NULL) ENGINE=MyISAM`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("Open error = %v, want unsupported schema", err)
	}
	var tableCount int
	if err := adminDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name LIKE 'rff\\_%'`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 1 {
		t.Fatalf("adapter table count = %d, want the one pre-existing table only", tableCount)
	}
}

func TestOpenRejectsFutureSchemaWithoutDowngrade(t *testing.T) {
	adminDB, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.ExecContext(t.Context(), `UPDATE rff_schema_version SET version = 99`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("Open error = %v, want unsupported future schema", err)
	}
	var version int
	if err := adminDB.QueryRowContext(t.Context(), `SELECT version FROM rff_schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 99 {
		t.Fatalf("schema version = %d, want unchanged version 99", version)
	}
}

func TestOpenRejectsMissingVersionedTableWithoutRecreatingIt(t *testing.T) {
	adminDB, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.ExecContext(t.Context(), `DROP TABLE rff_inputs`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("Open error = %v, want unsupported incomplete schema", err)
	}
	var exists int
	if err := adminDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'rff_inputs'`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatal("Open recreated a missing versioned table")
	}
}

func TestOpenRejectsPreOperationSchemaWithoutWriting(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `DROP TABLE rff_operations`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE rff_schema_version SET version = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("pre-operation schema was not rejected: %v", err)
	}
	var version, count int
	if err := db.QueryRowContext(t.Context(), `SELECT version FROM rff_schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'rff_operations'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if version != 1 || count != 0 {
		t.Fatalf("unsupported store was rewritten: version=%d operation tables=%d", version, count)
	}
}

func TestOpenRejectsNonInnoDBTable(t *testing.T) {
	adminDB, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.ExecContext(t.Context(), `ALTER TABLE rff_timers ENGINE=MyISAM`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("Open error = %v, want unsupported engine", err)
	}
}

func TestOpenRejectsMissingIdentityIndex(t *testing.T) {
	adminDB, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.ExecContext(t.Context(), `ALTER TABLE rff_executions DROP INDEX rff_execution_identity_lookup`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("Open error = %v, want incompatible schema", err)
	}
}

func TestConcurrentOpenInitializesSchemaOnce(t *testing.T) {
	_, config := newMySQLDatabase(t)
	type result struct {
		store *mysql.Store[string, string]
		err   error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			results <- result{store: store, err: err}
		}()
	}
	wait.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Error(result.err)
		}
		if result.store != nil {
			if err := result.store.Close(); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestCrossStoreTransitionLeaseIsSerialized(t *testing.T) {
	_, config := newMySQLDatabase(t)
	first, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Error(err)
		}
		if err := second.Close(); err != nil {
			t.Error(err)
		}
	})
	_, created, err := first.Accept(context.Background(), "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion:  transition.CurrentSchemaVersion,
			ID:             "execution",
			Entity:         "entity",
			Key:            "request",
			Operation:      transition.Initialize,
			MachineID:      "machine",
			MachineVersion: "1",
			Target:         "ready",
			Input: transition.EncodedValue{
				Codec: transition.CodecVersion{Name: "input", Version: "1"},
				Data:  []byte("input"),
			},
			NotificationOverride: transition.NotifyInherit,
			Notify:               true,
			Policy:               fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
			AcceptedAt:           time.Unix(100, 0),
			Deadline:             time.Unix(160, 0),
			Status:               transition.Waiting,
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("accept: created=%v error=%v", created, err)
	}

	type leaseResult struct {
		owner string
		err   error
	}
	results := make(chan leaseResult, 2)
	now := func() time.Time { return time.Unix(101, 0) }
	var wait sync.WaitGroup
	for _, candidate := range []struct {
		store *mysql.Store[string, string]
		owner string
	}{{first, "owner-a"}, {second, "owner-b"}} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := candidate.store.AcquireTransitionLease(context.Background(), "entity", "execution", candidate.owner, now, time.Minute)
			results <- leaseResult{owner: candidate.owner, err: err}
		}()
	}
	wait.Wait()
	close(results)
	var acquired, held int
	for result := range results {
		switch {
		case result.err == nil:
			acquired++
		case errors.Is(result.err, transition.ErrLeaseHeld):
			held++
		default:
			t.Errorf("%s lease error = %v", result.owner, result.err)
		}
	}

	if acquired != 1 || held != 1 {
		t.Fatalf("acquired=%d held=%d, want one acquired and one held", acquired, held)
	}
}

func TestLongIdentifiersRemainExact(t *testing.T) {
	store := openMySQL(t)
	entity := strings.Repeat("e", 700) + "\x00entity"
	key := strings.Repeat("k", 700) + " \x00key"
	executionID := strings.Repeat("i", 700) + "\x00execution"
	decide := func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing != nil || view.Active {
			return nil, nil
		}
		return &transition.Execution[string, string]{
			SchemaVersion:  transition.CurrentSchemaVersion,
			ID:             executionID,
			Entity:         entity,
			Key:            key,
			Operation:      transition.Initialize,
			MachineID:      "machine",
			MachineVersion: "1",
			Target:         "ready",
			Input: transition.EncodedValue{
				Codec: transition.CodecVersion{Name: "input", Version: "1"},
				Data:  []byte("input"),
			},
			NotificationOverride: transition.NotifyInherit,
			Notify:               true,
			Policy:               fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
			AcceptedAt:           time.Unix(100, 0),
			Deadline:             time.Unix(160, 0),
			Status:               transition.Waiting,
		}, nil
	}
	first, created, err := store.Accept(context.Background(), entity, key, decide)
	if err != nil || !created || first.ID != executionID || first.Entity != entity || first.Key != key {
		t.Fatalf("long identifier acceptance: execution=%+v created=%v error=%v", first, created, err)
	}
	replayed, created, err := store.Accept(context.Background(), entity, key, decide)
	if err != nil || created || replayed.ID != executionID || replayed.Entity != entity || replayed.Key != key {
		t.Fatalf("long identifier replay: execution=%+v created=%v error=%v", replayed, created, err)
	}
}
