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

type notificationClock struct{ now time.Time }

func (c notificationClock) Now() time.Time { return c.now }
func (c notificationClock) NewTicker(d time.Duration) transition.WorkerTicker {
	return (transition.SystemWorkerClock{}).NewTicker(d)
}
func (c notificationClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

func TestNotificationDeliverySurvivesRestartAndRetriesIndependently(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "delivery.db")
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
	claim, err := store.ClaimHook(ctx, "hook-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	operation, err := store.MarkWorkSuccess(ctx, fence, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
		ExecutionID: "execution", OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil {
		t.Fatal(err)
	}
	intent := transition.NotificationIntent[string, string]{
		ID: pending.NotificationIntentID, ExecutionID: pending.ID, CommitOperationID: pending.CommitOperationID,
		Entity: pending.Entity, Operation: pending.Operation, Target: pending.Target,
	}
	stale := fence
	stale.Generation++
	if err := outbox.Handoff(ctx, stale, intent); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("stale fence created a handoff: %v", err)
	}
	coordinator, err := transition.NewNotificationCoordinator[string, string](store, outbox, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := outbox.ClaimNotification(ctx, "delivery-worker", clock, time.Minute)
	if err != nil || first.Intent.ExecutionID != "execution" || first.Intent.Target != "target" {
		t.Fatalf("claim: delivery=%+v error=%v", first, err)
	}
	if _, err := outbox.RenewNotification(ctx, first.Lease, fixedClock(now.Add(30*time.Second)), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err = sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "replacement", fixedClock(now.Add(time.Minute)), time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("renewed claim was stolen: %v", err)
	}
	later := fixedClock(now.Add(90 * time.Second))
	second, err := outbox.ClaimNotification(ctx, "replacement", later, time.Minute)
	if err != nil || second.Intent.ID != first.Intent.ID || second.Attempt != first.Attempt+1 {
		t.Fatalf("redelivery did not preserve identity: first=%+v second=%+v error=%v", first, second, err)
	}
	if err := outbox.CompleteNotification(ctx, first.Lease, later); !errors.Is(err, transition.ErrStaleNotificationLease) {
		t.Fatalf("stale worker completed notification: %v", err)
	}
	if err := outbox.RetryNotificationDelivery(ctx, second.Lease, later, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "replacement", later, time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("notification retried before due: %v", err)
	}
	due := fixedClock(now.Add(2 * time.Minute))
	var third transition.NotificationDelivery[string, string]
	worker, err := transition.NewNotificationWorker[string, string](outbox, func(_ context.Context, delivery transition.NotificationDelivery[string, string]) error {
		third = delivery
		return nil
	}, transition.NotificationWorkerConfig{
		ID: "replacement", LeaseTTL: time.Minute, RenewalInterval: time.Second,
		AttemptTimeout: time.Minute, RetryDelay: time.Second, Clock: notificationClock{now.Add(2 * time.Minute)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); err != nil {
		t.Fatal(err)
	}
	if third.Intent.ID != first.Intent.ID || third.Attempt != second.Attempt+1 {
		t.Fatalf("retry: delivery=%+v", third)
	}
	if _, err := outbox.ClaimNotification(ctx, "replacement", due, time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("completed notification was redelivered: %v", err)
	}
	if err := outbox.Handoff(ctx, stale, intent); err != nil {
		t.Fatalf("durable handoff replay failed: %v", err)
	}
	changed := intent
	changed.Target = "other"
	if err := outbox.Handoff(ctx, stale, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed intent replay did not conflict: %v", err)
	}
	replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return nil, nil
	})
	if err != nil || created || replayed.Status != transition.Complete {
		t.Fatalf("delivery changed transition outcome: execution=%+v error=%v", replayed, err)
	}
}
