package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type handoffHost struct {
	intent *transition.NotificationIntent[string, string]
	writes int
}

func (h *handoffHost) Handoff(
	_ context.Context, _ transition.TransitionLease[string], intent transition.NotificationIntent[string, string],
) error {
	if h.intent != nil {
		if *h.intent != intent {
			return errors.New("notification identity conflict")
		}
		return nil
	}
	h.intent = &intent
	h.writes++
	return errors.New("lost handoff response")
}

func TestNotificationHandoffReconcilesLostResponseBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "notification.db")
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
				MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
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
	operationID, err := store.MarkWorkSuccess(ctx, fence, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
		ExecutionID: "execution", OperationID: operationID, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	host := &handoffHost{}
	coordinator, err := transition.NewNotificationCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err == nil {
		t.Fatal("lost handoff response was not reported")
	}
	pending, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || pending.Status != transition.Notifying || pending.NotificationIntentID == "" {
		t.Fatalf("unknown handoff did not remain recoverable: execution=%+v error=%v", pending, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err = transition.NewNotificationCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, nil
	})
	if err != nil || created || replayed.Status != transition.Complete || replayed.NotificationIntentID != host.intent.ID ||
		host.writes != 1 || host.intent.Target != "target" {
		t.Fatalf("handoff was duplicated or not completed: execution=%+v host=%+v error=%v", replayed, host, err)
	}
}

type durableSplitHost struct {
	db          *sql.DB
	loseCommit  bool
	loseHandoff bool
}

func (h *durableSplitHost) Commit(ctx context.Context, _ transition.TransitionLease[string],
	execution transition.Execution[string, string],
) (transition.CommitReceipt, error) {
	result, err := h.db.ExecContext(ctx, `INSERT OR IGNORE INTO host_commits (execution_id, operation_id, target) VALUES (?, ?, ?)`,
		execution.ID, execution.CommitOperationID, execution.Target)
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	var operation, target string
	if err := h.db.QueryRowContext(ctx, `SELECT operation_id, target FROM host_commits WHERE execution_id = ?`,
		execution.ID).Scan(&operation, &target); err != nil {
		return transition.CommitReceipt{}, err
	}
	if operation != execution.CommitOperationID || target != execution.Target {
		return transition.CommitReceipt{}, transition.ErrConflict
	}
	count, err := result.RowsAffected()
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	if count == 1 && h.loseCommit {
		return transition.CommitReceipt{}, errors.New("lost host commit response")
	}
	return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: operation, Outcome: transition.CommitSucceeded}, nil
}

func (h *durableSplitHost) Handoff(ctx context.Context, _ transition.TransitionLease[string],
	intent transition.NotificationIntent[string, string],
) error {
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	result, err := h.db.ExecContext(ctx, `INSERT OR IGNORE INTO host_intents (id, intent) VALUES (?, ?)`, intent.ID, data)
	if err != nil {
		return err
	}
	var stored []byte
	if err := h.db.QueryRowContext(ctx, `SELECT intent FROM host_intents WHERE id = ?`, intent.ID).Scan(&stored); err != nil {
		return err
	}
	var prior transition.NotificationIntent[string, string]
	if err := json.Unmarshal(stored, &prior); err != nil {
		return err
	}
	if prior != intent {
		return transition.ErrConflict
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 1 && h.loseHandoff {
		return errors.New("lost host handoff response")
	}
	return nil
}

func TestSplitStoreCommitAndNotificationRecoverAfterBothStoresReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path, hostPath := filepath.Join(dir, "transition.db"), filepath.Join(dir, "host.db")
	var store *sqlite.Store[string, string]
	var host *durableSplitHost
	open := func() {
		t.Helper()
		var err error
		store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite3", hostPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, ddl := range []string{
			`CREATE TABLE IF NOT EXISTS host_commits (execution_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, target TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS host_intents (id TEXT PRIMARY KEY, intent BLOB NOT NULL)`,
		} {
			if _, err := db.Exec(ddl); err != nil {
				t.Fatal(err)
			}
		}
		host = &durableSplitHost{db: db}
	}
	closeStores := func() {
		t.Helper()
		if err := errors.Join(store.Close(), host.db.Close()); err != nil {
			t.Fatal(err)
		}
		store, host = nil, nil
	}
	open()
	t.Cleanup(func() {
		if store != nil {
			if err := errors.Join(store.Close(), host.db.Close()); err != nil {
				t.Error(err)
			}
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "target", Status: transition.Waiting, Notify: true,
			AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
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
	host.loseCommit = true
	commits, err := transition.NewCommitCoordinator[string, string](store, host, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err == nil {
		t.Fatal("lost commit response was not reported")
	}
	closeStores()
	open()
	commits, err = transition.NewCommitCoordinator[string, string](store, host, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	host.loseHandoff = true
	notifications, err := transition.NewNotificationCoordinator[string, string](store, host, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := notifications.Reconcile(ctx); err == nil {
		t.Fatal("lost handoff response was not reported")
	}
	pending, err := store.InspectExecution(ctx, "execution")
	if err != nil || pending.Status != transition.Notifying || pending.NotificationIntentID == "" {
		t.Fatalf("unknown handoff prematurely completed: %+v, %v", pending, err)
	}
	closeStores()
	open()
	notifications, err = transition.NewNotificationCoordinator[string, string](store, host, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := notifications.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	completed, err := store.InspectExecution(ctx, "execution")
	if err != nil || completed.Status != transition.Complete || completed.NotificationIntentID != pending.NotificationIntentID ||
		completed.CommitReceipt == nil || completed.CommitReceipt.Outcome != transition.CommitSucceeded ||
		completed.Hook == nil || len(completed.Hook.Attempts) != 1 {
		t.Fatalf("recovered transition changed: %+v, %v", completed, err)
	}
	var commitsCount, intentsCount int
	var target string
	if err := host.db.QueryRow(`SELECT COUNT(*), MAX(target) FROM host_commits`).Scan(&commitsCount, &target); err != nil {
		t.Fatal(err)
	}
	if err := host.db.QueryRow(`SELECT COUNT(*) FROM host_intents`).Scan(&intentsCount); err != nil {
		t.Fatal(err)
	}
	if commitsCount != 1 || intentsCount != 1 || target != "target" {
		t.Fatalf("split host effects duplicated or changed: commits=%d intents=%d target=%q", commitsCount, intentsCount, target)
	}
	var data []byte
	if err := host.db.QueryRow(`SELECT intent FROM host_intents WHERE id = ?`, pending.NotificationIntentID).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var intent transition.NotificationIntent[string, string]
	if err := json.Unmarshal(data, &intent); err != nil {
		t.Fatal(err)
	}
	if intent.ExecutionID != "execution" || intent.CommitOperationID != completed.CommitReceipt.OperationID ||
		intent.Entity != "entity" || intent.Target != "target" {
		t.Fatalf("durable handoff lost transition identity: %+v", intent)
	}
}
