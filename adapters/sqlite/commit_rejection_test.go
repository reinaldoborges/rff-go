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

type lostRejectionHost struct {
	receipt *transition.CommitReceipt
	current string
}

func (h *lostRejectionHost) Commit(
	_ context.Context, _ transition.TransitionLease[string], execution transition.Execution[string, string],
) (transition.CommitReceipt, error) {
	if h.receipt != nil {
		return *h.receipt, nil
	}
	if h.current == execution.Previous {
		h.current = execution.Target
		receipt := transition.CommitReceipt{
			ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded,
		}
		h.receipt = &receipt
		return receipt, nil
	}
	failure := fsm.Failure{Code: "source-changed"}
	receipt := transition.CommitReceipt{
		ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitDenied, Failure: &failure,
	}
	h.receipt = &receipt
	return transition.CommitReceipt{}, errors.New("lost rejection response")
}

func TestDefinitiveRejectionAutomaticallyStartsRecoveryAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "rejection.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
			Entity: "entity", Key: "key", Operation: transition.Transition, Previous: "original-source", HasPrevious: true,
			Target: "target", Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
				MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookTransition, DueAt: now}); err != nil {
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
	host := &lostRejectionHost{current: "changed-source"}
	commits, err := transition.NewCommitCoordinator[string, string](store, host, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: 2 * time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err == nil {
		t.Fatal("lost rejection response was not reported")
	}
	pending, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || pending.Status != transition.Running || pending.CommitReceipt != nil || pending.RecoveryStarted {
		t.Fatalf("unknown commit result was treated as rejection: execution=%+v error=%v", pending, err)
	}
	host.current = "later-source"
	if err := commits.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.Aborting || !execution.RecoveryStarted ||
		execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitDenied ||
		!execution.CycleDeadline.Equal(now.Add(time.Hour)) {
		t.Fatalf("rejection did not start a fresh recovery cycle: execution=%+v error=%v", execution, err)
	}
	recovery, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, call, err := store.InspectHook(ctx, recovery, clock)
	if err != nil || call.Kind != transition.HookRecover || call.Failure == nil || call.Failure.Code != "source-changed" {
		t.Fatalf("recovery did not receive safe rejection data: call=%+v error=%v", call, err)
	}
	attempt, err := store.StartHook(ctx, recovery, clock)
	if err != nil || attempt.Attempt.Number != 2 {
		t.Fatalf("rejection recovery did not get a fresh budget: attempt=%+v error=%v", attempt, err)
	}
	if err := store.ReportHook(ctx, recovery, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	hooks, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := hooks.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, nil
	})
	if err != nil || created || replayed.Status != transition.Aborted || replayed.CommitReceipt == nil ||
		replayed.CommitReceipt.Outcome != transition.CommitDenied || host.current != "later-source" {
		t.Fatalf("rejection or authoritative state changed: execution=%+v created=%v host=%+v error=%v", replayed, created, host, err)
	}
}
