package successorcontract

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type AtomicStore interface {
	RestartStore
	AcceptSuccessorAtomic(context.Context, transition.ContinuationMarker[string, string],
		func(*Binding) (*Binding, error),
		func(context.Context, *sql.Tx, Binding) (transition.SuccessorReceipt[string, string], error),
	) (transition.Execution[string, string], bool, error)
}

func atomicDecision(binding Binding) func(*Binding) (*Binding, error) {
	return func(existing *Binding) (*Binding, error) {
		if existing != nil {
			return nil, nil
		}
		return &binding, nil
	}
}

func atomicHost(boundary string) func(context.Context, *sql.Tx, Binding) (transition.SuccessorReceipt[string, string], error) {
	return func(ctx context.Context, tx *sql.Tx, binding Binding) (transition.SuccessorReceipt[string, string], error) {
		var receipt transition.SuccessorReceipt[string, string]
		result, err := tx.ExecContext(ctx, `UPDATE host_fence SET owner = ? WHERE entity = ? AND revision = ? AND owner = ''`,
			binding.Execution.ID, binding.Origin.Entity, string(binding.OriginRevision))
		if err != nil {
			return receipt, err
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			return receipt, transition.ErrConflict
		}
		if boundary == "host-write" {
			os.Exit(87)
		}
		return transition.SuccessorReceipt[string, string]{
			Binding: binding, Principal: "test-worker", Decision: "allowed",
			AcceptedAt: time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
		}, nil
	}
}

func CrashAtomic(t *testing.T, store AtomicStore, db *sql.DB, host *Host, boundary string) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE host_fence (entity VARCHAR(100) PRIMARY KEY, revision VARCHAR(100) NOT NULL, owner VARCHAR(100) NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO host_fence VALUES ('restart', 'revision-1', '')`); err != nil {
		t.Fatal(err)
	}
	binding := Setup(t, store, host, "restart")
	if boundary == "before-accept" {
		os.Exit(87)
	}
	if _, _, err := store.AcceptSuccessorAtomic(context.Background(), binding.Origin, atomicDecision(binding), atomicHost(boundary)); err != nil {
		t.Fatal(err)
	}
	if boundary == "after-accept" {
		os.Exit(87)
	}
	t.Fatalf("unknown crash boundary %q", boundary)
}

func CheckAtomicRestart(t *testing.T, store AtomicStore, db *sql.DB, boundary string) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("origin markers: %+v error=%v", markers, err)
	}
	binding := BindingFor(markers[0])
	var owner string
	if err := db.QueryRow(`SELECT owner FROM host_fence WHERE entity = 'restart'`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if boundary == "after-accept" {
		if owner != binding.Execution.ID {
			t.Fatalf("committed fence missing: %q", owner)
		}
	} else {
		if owner != "" {
			t.Fatalf("uncommitted fence survived crash: %q", owner)
		}
		if _, err := store.InspectSuccessor(ctx, binding.Origin); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("uncommitted successor survived crash: %v", err)
		}
	}
	execution, created, err := store.AcceptSuccessorAtomic(ctx, binding.Origin, atomicDecision(binding), atomicHost(""))
	if err != nil || execution.ID != binding.Execution.ID || created != (boundary != "after-accept") {
		t.Fatalf("atomic recovery: %+v created=%v error=%v", execution, created, err)
	}
	if err := db.QueryRow(`SELECT owner FROM host_fence WHERE entity = 'restart'`).Scan(&owner); err != nil || owner != binding.Execution.ID {
		t.Fatalf("recovered fence: %q error=%v", owner, err)
	}
	ids, err := store.ListExecutionIDs(ctx)
	if err != nil || len(ids) != 2 {
		t.Fatalf("expected origin and one successor, ids=%v error=%v", ids, err)
	}
}
