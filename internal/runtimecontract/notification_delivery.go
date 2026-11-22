package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// NotificationDeliveryFixture supplies independent transition and delivery ports.
type NotificationDeliveryFixture struct {
	Store   Store
	Handoff transition.NotificationHandoff[string, string]
	Outbox  transition.NotificationActivityStore[string, string]
}

// RunNotificationDelivery checks stable delivery identity, retry and fencing.
func RunNotificationDelivery(t *testing.T, newFixture func(*testing.T, func() time.Time) NotificationDeliveryFixture) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	fixture := newFixture(t, clock)
	store, outbox := fixture.Store, fixture.Outbox
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "open", Status: transition.Waiting, Notify: true,
			AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
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
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	domain := &host{}
	commits, err := transition.NewCommitCoordinator[string, string](store, domain, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := commits.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	notifications, err := transition.NewNotificationCoordinator[string, string](store, fixture.Handoff, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := notifications.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := outbox.ClaimNotification(ctx, "worker", clock, time.Minute)
	if err != nil || first.Intent.ExecutionID != "execution" {
		t.Fatalf("durable notification was not claimable: %+v, %v", first, err)
	}
	now = now.Add(time.Minute)
	second, err := outbox.ClaimNotification(ctx, "replacement", clock, time.Minute)
	if err != nil || second.Intent.ID != first.Intent.ID || second.Attempt != first.Attempt+1 {
		t.Fatalf("redelivery changed notification identity: first=%+v second=%+v error=%v", first, second, err)
	}
	if err := outbox.CompleteNotification(ctx, first.Lease, clock); !errors.Is(err, transition.ErrStaleNotificationLease) {
		t.Fatalf("stale notification claim completed: %v", err)
	}
	due := now.Add(time.Minute)
	if err := outbox.RetryNotificationDelivery(ctx, second.Lease, clock, due); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "replacement", clock, time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("notification retried before its due time: %v", err)
	}
	now = due
	third, err := outbox.ClaimNotification(ctx, "replacement", clock, time.Minute)
	if err != nil || third.Intent.ID != first.Intent.ID || third.Attempt != second.Attempt+1 {
		t.Fatalf("notification retry changed identity: %+v, %v", third, err)
	}
	if err := outbox.CompleteNotification(ctx, third.Lease, clock); err != nil {
		t.Fatal(err)
	}
	record, err := store.InspectExecution(ctx, "execution")
	if err != nil || record.Status != transition.Complete || domain.writes != 1 || domain.current != "open" {
		t.Fatalf("delivery retries changed the transition: %+v, %v", record, err)
	}
}
