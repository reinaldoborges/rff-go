package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
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
