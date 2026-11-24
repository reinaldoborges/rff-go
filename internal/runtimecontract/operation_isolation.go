package runtimecontract

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunOperationIsolation checks that one unreadable record blocks only its identity.
func RunOperationIsolation(t *testing.T, address string, db *sql.DB, open func(string) (RestartOperationStore, error), unsupported error) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store, err := open(address)
	if err != nil {
		t.Fatal(err)
	}
	accept := func(id, entity, key string) {
		t.Helper()
		if _, _, err := store.Accept(ctx, entity, key, func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: entity, Key: key,
				Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	accept("execution", "entity", "request")
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
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
	binding := restartBinding()
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("operation start: started=%v err=%v", started, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = ?`, []byte{0xff}, "execution"); err != nil {
		t.Fatal(err)
	}
	store, err = open(address)
	if err != nil {
		t.Fatalf("unreadable individual operation blocked store initialization: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.InspectTrackedOperation(ctx, binding.Identity); !errors.Is(err, unsupported) {
		t.Fatalf("unreadable operation did not block inspection: %v", err)
	}
	accept("other-execution", "other-entity", "other-request")
}
