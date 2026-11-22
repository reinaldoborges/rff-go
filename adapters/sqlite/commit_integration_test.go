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

type lostCommitResponseHost struct {
	receipt *transition.CommitReceipt
	current string
	writes  int
}

func (h *lostCommitResponseHost) Commit(
	_ context.Context, _ transition.TransitionLease[string], execution transition.Execution[string, string],
) (transition.CommitReceipt, error) {
	if h.receipt != nil {
		return *h.receipt, nil
	}
	h.current = execution.Target
	h.writes++
	receipt := transition.CommitReceipt{
		ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded,
	}
	h.receipt = &receipt
	return transition.CommitReceipt{}, errors.New("lost host response")
}

func TestCommitReconciliationUsesDurableHostReceiptAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "commit.db")
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
			AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
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
	stale := fence
	stale.Generation++
	if err := store.RecordCommitReceipt(ctx, stale, clock, transition.CommitReceipt{
		ExecutionID: "execution", OperationID: operationID, Outcome: transition.CommitSucceeded,
	}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale coordinator recorded a commit receipt: %v", err)
	}
	host := &lostCommitResponseHost{}
	coordinator, err := transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err == nil {
		t.Fatal("lost commit response was not reported")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err = transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	host.current = "independently-changed"
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, nil
	})
	if err != nil || created || replayed.Status != transition.Complete || replayed.CommitReceipt == nil ||
		replayed.CommitReceipt.Outcome != transition.CommitSucceeded || host.writes != 1 || host.current != "independently-changed" {
		t.Fatalf("commit was repeated or not settled: execution=%+v created=%v host=%+v error=%v", replayed, created, host, err)
	}
}
