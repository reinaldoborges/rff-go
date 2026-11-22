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
