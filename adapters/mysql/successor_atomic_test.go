package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestSuccessorSharedAcceptanceIsAtomic(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "origin-host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	if _, err := db.Exec(`CREATE TABLE host_fence (entity VARCHAR(100) PRIMARY KEY, revision VARCHAR(100) NOT NULL, owner VARCHAR(100) NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO host_fence VALUES ('shared', 'revision-1', '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER host_fence_writer BEFORE UPDATE ON host_fence FOR EACH ROW
		BEGIN IF OLD.owner <> '' AND NEW.revision <> OLD.revision THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'fenced'; END IF; END`); err != nil {
		t.Fatal(err)
	}
	binding := successorcontract.Setup(t, store, host, "shared")
	decide := func(existing *transition.SuccessorBinding[string, string]) (*transition.SuccessorBinding[string, string], error) {
		if existing != nil {
			return nil, nil
		}
		return &binding, nil
	}
	denied := errors.New("authorization denied")
	hostAccept := func(deny bool) func(context.Context, *sql.Tx, transition.SuccessorBinding[string, string]) (transition.SuccessorReceipt[string, string], error) {
		return func(ctx context.Context, tx *sql.Tx, prepared transition.SuccessorBinding[string, string]) (transition.SuccessorReceipt[string, string], error) {
			result, err := tx.ExecContext(ctx, `UPDATE host_fence SET owner = ? WHERE entity = ? AND revision = ? AND owner = ''`, prepared.Execution.ID, prepared.Origin.Entity, string(prepared.OriginRevision))
			if err != nil {
				return transition.SuccessorReceipt[string, string]{}, err
			}
			n, err := result.RowsAffected()
			if err != nil || n != 1 {
				return transition.SuccessorReceipt[string, string]{}, transition.ErrConflict
			}
			if deny {
				return transition.SuccessorReceipt[string, string]{}, denied
			}
			return transition.SuccessorReceipt[string, string]{
				Binding: prepared, Principal: "worker", Decision: "allowed", AcceptedAt: time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
			}, nil
		}
	}
	if _, _, err := store.AcceptSuccessorAtomic(ctx, binding.Origin, decide, hostAccept(true)); !errors.Is(err, denied) {
		t.Fatalf("authorization failure: %v", err)
	}
	var owner string
	if err := db.QueryRow(`SELECT owner FROM host_fence WHERE entity = 'shared'`).Scan(&owner); err != nil || owner != "" {
		t.Fatalf("rejected acceptance retained fence: owner=%q error=%v", owner, err)
	}
	accepted, created, err := store.AcceptSuccessorAtomic(ctx, binding.Origin, decide, hostAccept(false))
	if err != nil || !created || accepted.ID != binding.Execution.ID {
		t.Fatalf("atomic acceptance: %+v created=%v error=%v", accepted, created, err)
	}
	if _, err := db.Exec(`UPDATE host_fence SET revision = 'revision-2' WHERE entity = 'shared'`); err == nil {
		t.Fatal("direct writer bypassed host fence")
	}
	replayed, created, err := store.AcceptSuccessorAtomic(ctx, binding.Origin, decide, func(context.Context, *sql.Tx, transition.SuccessorBinding[string, string]) (transition.SuccessorReceipt[string, string], error) {
		t.Fatal("accepted replay reran host acceptance")
		return transition.SuccessorReceipt[string, string]{}, nil
	})
	if err != nil || created || replayed.ID != accepted.ID {
		t.Fatalf("atomic replay: %+v created=%v error=%v", replayed, created, err)
	}
	successorcontract.RunAtomicDecisions(t, store, host)
}
