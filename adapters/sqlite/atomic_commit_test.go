package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type localCommitHost struct {
	store      *sqlite.Store[string, string]
	clock      func() time.Time
	fail       bool
	deny       bool
	calls      int
	afterWrite func()
	loseReply  bool
}

func (h *localCommitHost) Commit(ctx context.Context, lease transition.TransitionLease[string], _ transition.Execution[string, string]) (transition.CommitReceipt, error) {
	receipt, err := h.store.CommitAtomic(ctx, lease, h.clock, func(ctx context.Context, tx *sql.Tx, execution transition.Execution[string, string]) (transition.CommitReceipt, error) {
		h.calls++
		if _, err := tx.ExecContext(ctx, `INSERT INTO domain_state (entity, state) VALUES (?, ?)`, execution.Entity, execution.Target); err != nil {
			return transition.CommitReceipt{}, err
		}
		if h.afterWrite != nil {
			h.afterWrite()
		}
		if h.fail {
			return transition.CommitReceipt{}, errors.New("injected commit failure")
		}
		if h.deny {
			return transition.CommitReceipt{
				ExecutionID: execution.ID, OperationID: execution.CommitOperationID,
				Outcome: transition.CommitDenied, Failure: &fsm.Failure{Code: "invariant"},
			}, nil
		}
		return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded}, nil
	})
	if err == nil && h.loseReply {
		h.loseReply = false
		return transition.CommitReceipt{}, errors.New("lost commit response")
	}
	return receipt, err
}

func TestAtomicCommitRollsBackAndEnqueuesNotification(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "atomic.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("rejected commit discards domain writes", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "rejection.db")
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE domain_state (entity TEXT PRIMARY KEY, state TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
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
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := fixedClock(now)
		_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
				Entity: "entity", Key: "key", Operation: transition.Initialize, Target: "target", Status: transition.Waiting,
				Notify: true, AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
					MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartHook(ctx, claim, clock); err != nil {
			t.Fatal(err)
		}
		if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkWorkSuccess(ctx, fence, clock); err != nil {
			t.Fatal(err)
		}
		host := &localCommitHost{store: store, clock: clock, deny: true}
		coordinator, err := transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{
			ID: "coordinator", Clock: clock, LeaseTTL: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		execution, err := store.ReadActiveExecution(ctx, "execution")
		if err != nil || execution.Status != transition.Aborting || execution.CommitReceipt == nil ||
			execution.CommitReceipt.Outcome != transition.CommitDenied {
			t.Fatalf("rejection did not begin recovery: execution=%+v error=%v", execution, err)
		}
		outbox, err := sqlite.NewNotificationOutbox(store, clock)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
			t.Fatalf("rejected commit enqueued notification: %v", err)
		}
		db, err = sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM domain_state`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rejected commit retained domain writes: count=%d error=%v", count, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := host.Commit(ctx, fence, execution); err != nil || host.calls != 1 {
			t.Fatalf("rejection replay reran mutation: calls=%d error=%v", host.calls, err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE domain_state (entity TEXT PRIMARY KEY, state TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
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
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Target: "target", Status: transition.Waiting,
			Notify: true, AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
				MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkWorkSuccess(ctx, fence, clock); err != nil {
		t.Fatal(err)
	}
	host := &localCommitHost{store: store, clock: clock, fail: true}
	coordinator, err := transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "coordinator", Clock: clock, LeaseTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err == nil {
		t.Fatal("injected failure was not reported")
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM domain_state`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed commit did not roll back domain state: count=%d error=%v", count, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("failed commit created a notification: %v", err)
	}
	stale := fence
	stale.Generation++
	if _, err := host.Commit(ctx, stale, transition.Execution[string, string]{}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale fence committed: %v", err)
	}
	commitTime := now
	expiringHost := &localCommitHost{
		store: store, clock: func() time.Time { return commitTime },
		afterWrite: func() { commitTime = now.Add(time.Hour) },
	}
	if _, err := expiringHost.Commit(ctx, fence, transition.Execution[string, string]{}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("expired fence committed: %v", err)
	}
	host.fail = false
	host.loseReply = true
	if err := coordinator.Reconcile(ctx); err == nil {
		t.Fatal("lost commit response was not reported")
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM domain_state WHERE entity = 'entity'`).Scan(&state); err != nil || state != "target" {
		t.Fatalf("committed domain state missing: state=%q error=%v", state, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host.store = store
	outbox, err = sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || delivery.Intent.Target != "target" {
		t.Fatalf("atomic commit did not enqueue notification: delivery=%+v error=%v", delivery, err)
	}
	replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, nil
	})
	if err != nil || created || replayed.Status != transition.Complete || replayed.CommitReceipt == nil {
		t.Fatalf("atomic commit did not complete execution: execution=%+v error=%v", replayed, err)
	}
	if _, err := host.Commit(ctx, stale, replayed); err != nil || host.calls != 2 {
		t.Fatalf("commit replay reran mutation: calls=%d error=%v", host.calls, err)
	}
}
