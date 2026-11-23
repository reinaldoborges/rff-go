package continuationcontract

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationrecord"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func RunMissingOrigin(t *testing.T, store CrossStore, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "valid", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
			MachineID: "plan", MachineVersion: "1", Policy: policy,
			Continuation: &transition.ContinuationSnapshot{MachineID: "plan", MachineVersion: "1", Policy: policy},
			AcceptedAt:   now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "valid", "owner", clock, time.Hour)
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
	operation, err := store.MarkWorkSuccess(ctx, lease, clock)
	if err != nil {
		t.Fatal(err)
	}
	valid := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "valid", OperationID: operation, Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1", Policy: policy,
	}
	missing := valid
	missing.ExecutionID, missing.Entity = "missing", "missing-entity"
	missing.OperationID = "rff-commit:ec1ab7cbb696e16602b72a3d35d975f7c9546e07da140d9426c5c9341e3cc626"
	host := &pagedHost{markers: []transition.ContinuationMarker[string, string]{missing, valid}, acknowledged: make(map[string]bool)}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := discovery.Reconcile(ctx); !errors.Is(err, transition.ErrContinuationOriginMissing) {
			t.Fatalf("missing origin was not diagnosed: %v", err)
		}
		if host.acknowledged["missing"] || !host.acknowledged["valid"] {
			t.Fatalf("incorrect acknowledgement: %v", host.acknowledged)
		}
		markers, err := store.ContinuationMarkers(ctx)
		if err != nil || len(markers) != 1 || markers[0].ExecutionID != "valid" {
			t.Fatalf("missing marker was persisted or valid marker lost: %+v %v", markers, err)
		}
	}
	prepared := PrepareOrigin(t, store, missing)
	if prepared.OperationID != missing.OperationID {
		t.Fatal("origin commit identity differs from retained host marker")
	}
	if err := discovery.Reconcile(ctx); err != nil || !host.acknowledged["missing"] {
		t.Fatalf("reconciliation after origin recovery: acknowledged=%v err=%v", host.acknowledged, err)
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 2 {
		t.Fatalf("recovered marker lost or duplicated: %+v err=%v", markers, err)
	}

	orphan := missing
	orphan.ExecutionID = "previously-stored-orphan"
	data, err := continuationrecord.Encode(orphan, []byte(orphan.Entity), []byte(orphan.Target))
	if err != nil {
		t.Fatal(err)
	}
	digest := continuationrecord.Digest(orphan.ExecutionID)
	if _, err := db.ExecContext(ctx, `INSERT INTO rff_continuations (digest, record) VALUES (?, ?)`, digest[:], data); err != nil {
		t.Fatal(err)
	}
	if ready, err := store.ContinuationReady(ctx, orphan.ExecutionID); !errors.Is(err, transition.ErrContinuationOriginMissing) || ready {
		t.Fatalf("stored missing origin must be diagnosed: ready=%v err=%v", ready, err)
	}
}
