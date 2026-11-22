package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestAtomicAcceptanceCommitsHostAuthorizationAuditWithExecution(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "acceptance.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE host_audit (request TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	rejected := errors.New("authorization rejected")
	decide := func(reject bool) func(context.Context, *sql.Tx, transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return func(ctx context.Context, tx *sql.Tx, view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if view.Existing != nil {
				return nil, nil
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO host_audit (request) VALUES ('request')`); err != nil {
				return nil, err
			}
			if reject {
				return nil, rejected
			}
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
				Entity: "entity", Key: "request", Status: transition.Waiting,
			}, nil
		}
	}
	if _, _, err := store.AcceptAtomic(ctx, "entity", "request", decide(true)); !errors.Is(err, rejected) {
		t.Fatalf("rejected acceptance did not return authorization error: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM host_audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected acceptance left host writes: count=%d error=%v", count, err)
	}
	first, created, err := store.AcceptAtomic(ctx, "entity", "request", decide(false))
	if err != nil || !created || first.ID != "execution" {
		t.Fatalf("atomic acceptance failed: execution=%+v created=%v error=%v", first, created, err)
	}
	replayed, created, err := store.AcceptAtomic(ctx, "entity", "request", decide(false))
	if err != nil || created || replayed.ID != first.ID {
		t.Fatalf("atomic acceptance did not replay: execution=%+v created=%v error=%v", replayed, created, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM host_audit`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("acceptance audit was not committed once: count=%d error=%v", count, err)
	}
}

func TestInputRejectionIsDurableAtAcceptanceBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "input-rejection.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE domain_state (value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	_, _, err = store.AcceptInput(ctx, "input", func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1, ID: "input", Channel: "decisions", WorkflowID: "entity", Kind: transition.ExternalEvent,
			Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("request")},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	conflict := errors.New("domain conflict")
	classify := func(err error) *fsm.Failure {
		if errors.Is(err, conflict) {
			return &fsm.Failure{Code: "conflict"}
		}
		return nil
	}
	_, created, outcome, err := store.AcceptInputAtomic(ctx, "entity", "input",
		func(ctx context.Context, tx *sql.Tx, _ transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO domain_state (value) VALUES ('must-roll-back')`); err != nil {
				return nil, err
			}
			return nil, conflict
		}, classify)
	if err != nil || created || outcome == nil || outcome.Failure == nil || outcome.Failure.Code != "conflict" {
		t.Fatalf("rejection was not persisted at the decision boundary: outcome=%+v created=%v error=%v", outcome, created, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM domain_state`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected input retained domain writes: count=%d error=%v", count, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, created, replay, err := store.AcceptInputAtomic(ctx, "entity", "input",
		func(context.Context, *sql.Tx, transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			t.Fatal("replayed rejection was re-evaluated")
			return nil, nil
		}, classify)
	if err != nil || created || replay == nil || replay.Failure == nil || replay.Failure.Code != "conflict" {
		t.Fatalf("reopened rejection changed: outcome=%+v created=%v error=%v", replay, created, err)
	}
}

func TestInputAcceptanceAndOutcomeRollBackTogether(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "input-acceptance.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	_, _, err = store.AcceptInput(ctx, "input", func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{
			SchemaVersion: 1, ID: "input", Channel: "commands", WorkflowID: "entity", Kind: transition.Command,
			Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("request")},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.Exec(`CREATE TRIGGER fail_input_outcome BEFORE UPDATE ON rff_inputs BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	decide := func(context.Context, *sql.Tx, transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "input", Status: transition.Waiting,
		}, nil
	}
	classify := func(error) *fsm.Failure { return nil }
	if _, _, _, err := store.AcceptInputAtomic(ctx, "entity", "input", decide, classify); err == nil {
		t.Fatal("injected outcome failure was not reported")
	}
	ids, err := store.ListExecutionIDs(ctx)
	if err != nil || len(ids) != 0 {
		t.Fatalf("failed input outcome left an execution: %v, %v", ids, err)
	}
	record, err := store.ReadInput(ctx, "input")
	if err != nil || record.Outcome != nil {
		t.Fatalf("failed acceptance left an outcome: %+v, %v", record, err)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_input_outcome`); err != nil {
		t.Fatal(err)
	}
	execution, created, outcome, err := store.AcceptInputAtomic(ctx, "entity", "input", decide, classify)
	if err != nil || !created || execution.ID != "execution" || outcome == nil || outcome.Receipt != "execution" {
		t.Fatalf("atomic input acceptance failed: execution=%+v outcome=%+v error=%v", execution, outcome, err)
	}
	replayed, created, outcome, err := store.AcceptInputAtomic(ctx, "entity", "input",
		func(context.Context, *sql.Tx, transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			t.Fatal("successful input replay re-evaluated acceptance")
			return nil, nil
		}, classify)
	if err != nil || created || replayed.ID != execution.ID || outcome == nil || outcome.Receipt != execution.ID {
		t.Fatalf("successful input replay changed: execution=%+v outcome=%+v error=%v", replayed, outcome, err)
	}
}
