package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestNextExecutionFencesCompletedExecutionWithoutWaitingForLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "next.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	accept := func(id string) {
		t.Helper()
		if _, _, err := store.Accept(ctx, "entity", id, func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: "entity", Key: id,
				Operation: transition.Initialize, Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	accept("first")
	first, err := store.AcquireTransitionLease(ctx, "entity", "first", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, first, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
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
	operation, err := store.MarkWorkSuccess(ctx, first, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, first, clock, transition.CommitReceipt{
		ExecutionID: "first", OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	accept("second")
	second, err := store.AcquireTransitionLease(ctx, "entity", "second", "coordinator", clock, time.Hour)
	if err != nil || second.Generation <= first.Generation {
		t.Fatalf("new execution could not immediately fence the old one: first=%+v second=%+v error=%v", first, second, err)
	}
	if _, err := store.RenewTransitionLease(ctx, first, clock, time.Hour); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("completed execution retained authority: %v", err)
	}
}
