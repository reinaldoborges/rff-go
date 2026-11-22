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

func TestIncompatibleNotificationDoesNotBlockOtherDeliveries(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "notifications.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Now().UTC()
	clock := fixedClock(now)
	outbox, err := sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ id, target string }{{"a", "incompatible"}, {"b", "compatible"}} {
		_, _, err := store.Accept(ctx, item.id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: item.id, Entity: item.id, Key: "key",
				Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
				Target: item.target, Status: transition.Waiting, Notify: true, AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		fence, err := store.AcquireTransitionLease(ctx, item.id, item.id, "coordinator", clock, time.Hour)
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
		operation, err := store.MarkWorkSuccess(ctx, fence, clock)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
			ExecutionID: item.id, OperationID: operation, Outcome: transition.CommitSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
		coordinator, err := transition.NewNotificationCoordinator[string, string](store, outbox, transition.CoordinatorConfig{
			ID: "coordinator", Clock: clock, LeaseTTL: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, selectiveStateCodec{})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err = sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "b" {
		t.Fatalf("incompatible notification blocked compatible work: delivery=%+v error=%v", delivery, err)
	}
	diagnostic, err := outbox.ReadNotificationCompatibility(ctx, "a")
	if err != nil || diagnostic == nil || diagnostic.Component != transition.CompatibilityState {
		t.Fatalf("incompatible notification has no safe diagnostic: diagnostic=%+v error=%v", diagnostic, err)
	}
	if err := outbox.CompleteNotification(ctx, delivery.Lease, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute); !errors.Is(err, transition.ErrCompatibility) ||
		errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("unavailable notification was not distinguishable from an empty queue: %v", err)
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
	delivery, err = outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "a" {
		t.Fatalf("restored notification did not become available: delivery=%+v error=%v", delivery, err)
	}
	diagnostic, err = outbox.ReadNotificationCompatibility(ctx, "a")
	if err != nil || diagnostic != nil {
		t.Fatalf("restored notification retained diagnostic: diagnostic=%+v error=%v", diagnostic, err)
	}
}
