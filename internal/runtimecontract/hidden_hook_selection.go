package runtimecontract

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type HiddenHookStore interface {
	Accept(context.Context, string, string, transition.AcceptanceDecisionFunc[string, string]) (transition.Execution[string, string], bool, error)
	AcquireTransitionLease(context.Context, string, string, string, func() time.Time, time.Duration) (transition.TransitionLease[string], error)
	DispatchHook(context.Context, transition.TransitionLease[string], func() time.Time, transition.HookDispatch) (string, error)
	ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error)
}

func CheckHiddenHookProjection(t *testing.T, store HiddenHookStore, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	for _, id := range []string{"a", "b"} {
		entity := "entity-" + id
		_, _, err := store.Accept(ctx, entity, "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: entity, Key: "request",
				Operation: transition.Initialize, Target: "ready", Status: transition.Waiting,
				Deadline: now.Add(time.Hour),
				Policy:   fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, entity, id, "coordinator", clock, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{
			Kind: transition.HookEnter, DueAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE rff_hook_work SET completed = 1 WHERE execution_id = ?`, "a"); err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || lease.ExecutionID != "b" {
		t.Fatalf("hidden first hook: got %q, error %v", lease.ExecutionID, err)
	}
}
