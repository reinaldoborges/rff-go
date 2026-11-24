package continuationcontract

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// CrossStoreHost is a test host with its own durable database.
type CrossStoreHost struct {
	db    *sql.DB
	crash string
}

func OpenCrossStoreHost(path, crash string) (*CrossStoreHost, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS host_commits
		(id TEXT PRIMARY KEY, marker BLOB NOT NULL, receipt BLOB NOT NULL, acknowledged INTEGER NOT NULL);
		CREATE TABLE IF NOT EXISTS host_domain (id TEXT PRIMARY KEY)`); err != nil {
		db.Close()
		return nil, err
	}
	return &CrossStoreHost{db: db, crash: crash}, nil
}

func (h *CrossStoreHost) Close() error                    { return h.db.Close() }
func (*CrossStoreHost) ContinuationCommitSupported() bool { return true }

func (h *CrossStoreHost) Commit(ctx context.Context, lease transition.TransitionLease[string], execution transition.Execution[string, string]) (transition.CommitReceipt, error) {
	var receipt transition.CommitReceipt
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	var data []byte
	err = tx.QueryRowContext(ctx, `SELECT receipt FROM host_commits WHERE id = ?`, execution.ID).Scan(&data)
	if err == nil {
		if err := json.Unmarshal(data, &receipt); err != nil {
			return receipt, err
		}
		return receipt, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) || execution.Continuation == nil || lease.ExecutionID != execution.ID {
		return receipt, fmt.Errorf("invalid host commit: %v", err)
	}
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: execution.ID, OperationID: execution.CommitOperationID,
		Entity: execution.Entity, Target: execution.Target, MachineID: execution.Continuation.MachineID,
		MachineVersion: execution.Continuation.MachineVersion, Policy: execution.Continuation.Policy,
	}
	receipt = transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded}
	markerData, err := json.Marshal(marker)
	if err != nil {
		return receipt, err
	}
	receiptData, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO host_domain (id) VALUES (?)`, execution.ID); err != nil {
		return receipt, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO host_commits VALUES (?, ?, ?, 0)`, execution.ID, markerData, receiptData); err != nil {
		return receipt, err
	}
	if h.crash == "before-host" {
		os.Exit(87)
	}
	if err := tx.Commit(); err != nil {
		return receipt, err
	}
	if h.crash == "host" {
		os.Exit(87)
	}
	return receipt, nil
}

func (h *CrossStoreHost) ListContinuationMarkers(ctx context.Context, cursor string) ([]transition.ContinuationMarker[string, string], string, error) {
	if cursor != "" {
		return nil, "", errors.New("invalid cursor")
	}
	rows, err := h.db.QueryContext(ctx, `SELECT marker FROM host_commits WHERE acknowledged = 0 ORDER BY id`)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var markers []transition.ContinuationMarker[string, string]
	for rows.Next() {
		var data []byte
		var marker transition.ContinuationMarker[string, string]
		if err := rows.Scan(&data); err != nil {
			return nil, "", err
		}
		if err := json.Unmarshal(data, &marker); err != nil {
			return nil, "", err
		}
		markers = append(markers, marker)
	}
	return markers, "", rows.Err()
}

func (h *CrossStoreHost) AcknowledgeContinuationMarker(ctx context.Context, marker transition.ContinuationMarker[string, string]) error {
	if h.crash == "before-ack" {
		os.Exit(87)
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var data []byte
	if err := tx.QueryRowContext(ctx, `SELECT marker FROM host_commits WHERE id = ?`, marker.ExecutionID).Scan(&data); err != nil {
		return err
	}
	var prior transition.ContinuationMarker[string, string]
	if err := json.Unmarshal(data, &prior); err != nil {
		return err
	}
	if err := transition.MatchContinuationMarkers(prior, marker); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE host_commits SET acknowledged = 1 WHERE id = ?`, marker.ExecutionID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if h.crash == "after-ack" {
		os.Exit(87)
	}
	return nil
}

type CrossStore interface {
	Store
	transition.CommitStore[string, string]
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
	ReadContinuation(context.Context, string) (transition.ContinuationProgress, error)
	AcquireContinuation(context.Context, transition.ContinuationMarker[string, string], string, func() time.Time, time.Duration) (transition.ContinuationLease, error)
	StartContinuation(context.Context, transition.ContinuationLease, func() time.Time) error
}

func RunCrossStoreCrash(t *testing.T, store CrossStore, host *CrossStoreHost) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "origin", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
			MachineID: "plan", MachineVersion: "1", Policy: policy,
			Continuation: &transition.ContinuationSnapshot{MachineID: "plan", MachineVersion: "1", Policy: policy},
			AcceptedAt:   now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "origin", "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
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
	if _, err := store.MarkWorkSuccess(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{ID: "owner", Clock: clock, LeaseTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); !errors.Is(err, transition.ErrContinuationPending) {
		t.Fatalf("receipt should wait for marker: %v", err)
	}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	t.Fatal("expected process loss")
}

func CheckCrossStoreRestart(t *testing.T, store CrossStore, host *CrossStoreHost) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "owner", Clock: clock, LeaseTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		if !errors.Is(err, transition.ErrContinuationPending) {
			t.Fatal(err)
		}
		if err := discovery.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("marker lost or duplicated: %+v, %v", markers, err)
	}
	readinessObservedAt := now
	progress, err := store.ReadContinuation(ctx, markers[0].ExecutionID)
	if err != nil || progress.CycleStarted != readinessObservedAt ||
		!progress.Deadline.Equal(readinessObservedAt.Add(markers[0].Policy.OverallTimeout)) {
		t.Fatalf("durable cross-store readiness did not start its cycle: progress=%+v err=%v", progress, err)
	}
	now = now.Add(markers[0].Policy.OverallTimeout)
	if ready, err := store.ContinuationReady(ctx, "origin"); err != nil || !ready {
		t.Fatalf("delayed discovery must still observe the released origin: ready=%v err=%v", ready, err)
	}
	lease, err := store.AcquireContinuation(ctx, markers[0], "worker-after-restart", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartContinuation(ctx, lease, clock); !errors.Is(err, transition.ErrContinuationIntervention) {
		t.Fatalf("delayed cross-store discovery must consume the cycle deadline: %v", err)
	}
	progress, err = store.ReadContinuation(ctx, markers[0].ExecutionID)
	if err != nil || progress.CycleStarted != readinessObservedAt ||
		progress.Phase != transition.ContinuationIntervention {
		t.Fatalf("acquisition reset the cross-store readiness budget: progress=%+v err=%v", progress, err)
	}
	var count int
	if err := host.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM host_domain`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("host commit lost or duplicated: count=%d err=%v", count, err)
	}
}
