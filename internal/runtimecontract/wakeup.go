package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type wakeupTicker struct{ ticks chan time.Time }

func (t wakeupTicker) Ticks() <-chan time.Time { return t.ticks }
func (wakeupTicker) Stop()                     {}

// RunWakeup checks that transient notifications and polling discover durable work.
func RunWakeup(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	queue := transition.NewWakeupQueue()
	ticks := make(chan time.Time, 1)
	scans := make(chan error, 3)
	done := make(chan error, 1)
	go func() {
		done <- transition.RunReconciliation(ctx, queue, wakeupTicker{ticks}, func(ctx context.Context) error {
			err := coordinator.Reconcile(ctx)
			select {
			case scans <- err:
			case <-ctx.Done():
			}
			return err
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("reconciliation loop did not stop")
		}
	})
	awaitScan := func() {
		t.Helper()
		select {
		case err := <-scans:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("coordinator scan did not run")
		}
	}
	awaitScan()
	accept := func(id string) {
		t.Helper()
		_, _, err := store.Accept(ctx, id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: id, Key: "key",
				Operation: transition.Initialize, Target: "open", Status: transition.Waiting,
				AcceptedAt: now, Deadline: now.Add(time.Hour),
				Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	accept("woken")
	queue.Notify()
	awaitScan()
	first, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || first.ExecutionID != "woken" {
		t.Fatalf("wakeup did not dispatch accepted work: %+v, %v", first, err)
	}
	accept("polled")
	ticks <- now
	awaitScan()
	second, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || second.ExecutionID != "polled" {
		t.Fatalf("polling did not recover unwoken work: %+v, %v", second, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciliation loop did not stop on cancellation: %v", err)
		}
		done <- err
	case <-time.After(5 * time.Second):
		t.Fatal("reconciliation loop did not stop on cancellation")
	}
}
