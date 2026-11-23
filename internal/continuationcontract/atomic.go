package continuationcontract

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type AtomicStore interface {
	Store
	transition.OperationStore
	CommitAtomic(context.Context, transition.TransitionLease[string], func() time.Time,
		func(context.Context, *sql.Tx, transition.Execution[string, string]) (transition.CommitReceipt, error)) (transition.CommitReceipt, error)
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
}

func RunAtomic(t *testing.T, fixture func(*testing.T) (AtomicStore, *sql.DB)) {
	t.Helper()
	tests := []struct {
		name                string
		notify, opted, deny bool
		fail, ordinary      bool
	}{
		{name: "initial notification", notify: true, opted: true},
		{name: "ordinary without notification", opted: true, ordinary: true},
		{name: "rejection", opted: true, deny: true},
		{name: "rollback", opted: true, fail: true},
		{name: "not opted", notify: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db := fixture(t)
			ctx := context.Background()
			if _, err := db.ExecContext(ctx, `CREATE TABLE continuation_test_domain (id VARCHAR(64) NOT NULL PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
			_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				execution := &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: "origin", Entity: "entity", Key: "key",
					Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
					MachineID: "plan", MachineVersion: "1", Notify: tt.notify, Policy: policy,
					AcceptedAt: now, Deadline: now.Add(time.Hour),
				}
				if tt.opted {
					execution.Continuation = &transition.ContinuationSnapshot{MachineID: "plan", MachineVersion: "1", Policy: policy}
				}
				if tt.ordinary {
					execution.Operation, execution.HasPrevious, execution.Previous = transition.Transition, true, "previous"
				}
				return execution, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := store.AcquireTransitionLease(ctx, "entity", "origin", "owner", clock, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			kind := transition.HookEnter
			if tt.ordinary {
				kind = transition.HookTransition
			}
			if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: kind, DueAt: now}); err != nil {
				t.Fatal(err)
			}
			claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StartHook(ctx, claim, clock); err != nil {
				t.Fatal(err)
			}
			if tt.opted && !tt.deny && !tt.fail {
				codec := transition.CodecVersion{Name: "result", Version: "1"}
				binding := transition.OperationBinding{
					Identity: transition.OperationIdentity{ExecutionID: "origin", Key: string([]byte{'k', 0xff})},
					Kind:     "read", BehaviorVersion: "1", ComparatorVersion: "1", ResultCodec: codec,
				}
				equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
				if _, started, err := store.BeginTrackedOperation(ctx, claim, clock, binding, equal); err != nil || !started {
					t.Fatalf("start tracked operation: started=%v err=%v", started, err)
				}
				if err := store.CompleteTrackedOperation(ctx, claim, clock, binding.Identity, transition.OperationResult{Codec: codec, NoResult: true}); err != nil {
					t.Fatal(err)
				}
				if result, started, err := store.BeginTrackedOperation(ctx, claim, clock, binding, equal); err != nil || started || result == nil || !result.NoResult {
					t.Fatalf("tracked operation replay: result=%+v started=%v err=%v", result, started, err)
				}
			}
			if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
				t.Fatal(err)
			}
			operation, err := store.MarkWorkSuccess(ctx, lease, clock)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			callback := func(ctx context.Context, tx *sql.Tx, _ transition.Execution[string, string]) (transition.CommitReceipt, error) {
				calls++
				if _, err := tx.ExecContext(ctx, `INSERT INTO continuation_test_domain (id) VALUES (?)`, "origin"); err != nil {
					return transition.CommitReceipt{}, err
				}
				if os.Getenv("RFF_CONTINUATION_ATOMIC_CRASH") == "before-commit" {
					os.Exit(87)
				}
				if tt.fail {
					return transition.CommitReceipt{}, errors.New("injected rollback")
				}
				receipt := transition.CommitReceipt{ExecutionID: "origin", OperationID: operation, Outcome: transition.CommitSucceeded}
				if tt.deny {
					receipt.Outcome, receipt.Failure = transition.CommitDenied, &fsm.Failure{Code: "denied"}
				}
				return receipt, nil
			}
			_, err = store.CommitAtomic(ctx, lease, clock, callback)
			if (err != nil) != tt.fail {
				t.Fatalf("commit error: %v", err)
			}
			if os.Getenv("RFF_CONTINUATION_ATOMIC_CRASH") == "after-commit" {
				os.Exit(87)
			}
			var count int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM continuation_test_domain`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if (count == 1) != (!tt.deny && !tt.fail) {
				t.Fatalf("domain mutation count: %d", count)
			}
			markers, err := store.ContinuationMarkers(ctx)
			if err != nil || (len(markers) == 1) != (tt.opted && !tt.deny && !tt.fail) {
				t.Fatalf("unexpected markers: %+v, %v", markers, err)
			}
			if !tt.fail {
				if _, err := store.CommitAtomic(ctx, lease, clock, callback); err != nil || calls != 1 {
					t.Fatalf("commit replay reran mutation: calls=%d err=%v", calls, err)
				}
			}
		})
	}
}

func CheckAtomicRollbackAfterRestart(t *testing.T, store AtomicStore, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM continuation_test_domain`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("uncommitted domain mutation survived process loss: count=%d err=%v", count, err)
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 0 {
		t.Fatalf("uncommitted continuation survived process loss: %+v err=%v", markers, err)
	}
	execution, err := store.ReadActiveExecution(ctx, "origin")
	if err != nil || execution.CommitReceipt != nil || execution.CommitOperationID == "" {
		t.Fatalf("pre-commit execution state was not preserved: %+v err=%v", execution, err)
	}
	if ready, err := store.ContinuationReady(ctx, "origin"); ready || !errors.Is(err, transition.ErrContinuationPending) {
		t.Fatalf("uncommitted continuation became ready: ready=%v err=%v", ready, err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	lease, err := store.AcquireTransitionLease(ctx, "entity", "origin", "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	callback := func(ctx context.Context, tx *sql.Tx, execution transition.Execution[string, string]) (transition.CommitReceipt, error) {
		calls++
		if _, err := tx.ExecContext(ctx, `INSERT INTO continuation_test_domain (id) VALUES (?)`, "origin"); err != nil {
			return transition.CommitReceipt{}, err
		}
		return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded}, nil
	}
	if _, err := store.CommitAtomic(ctx, lease, clock, callback); err != nil {
		t.Fatalf("retry after uncommitted process loss: %v", err)
	}
	if _, err := store.CommitAtomic(ctx, lease, clock, callback); err != nil || calls != 1 {
		t.Fatalf("retry replay duplicated commit: calls=%d err=%v", calls, err)
	}
	CheckAtomicAfterRestart(t, store, db)
}

func CheckAtomicAfterRestart(t *testing.T, store AtomicStore, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM continuation_test_domain`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("domain commit missing after process loss: count=%d err=%v", count, err)
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 || markers[0].ExecutionID != "origin" {
		t.Fatalf("continuation missing after process loss: %+v err=%v", markers, err)
	}
	if ready, err := store.ContinuationReady(ctx, "origin"); err != nil || !ready {
		t.Fatalf("reconciled origin not ready after process loss: ready=%v err=%v", ready, err)
	}
}
