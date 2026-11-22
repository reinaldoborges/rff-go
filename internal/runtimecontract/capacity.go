package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunCapacity checks persistence health independently of worker availability.
func RunCapacity(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	workers, err := transition.NewWorkerMonitor(clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Minute, Clock: clock, Workers: workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if status := coordinator.Status(); !status.Healthy || !status.Capacity.Degraded || status.Capacity.Available != 0 {
		t.Fatalf("zero-worker capacity was not degraded independently of persistence: %+v", status)
	}
	_, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("zero worker capacity prevented durable acceptance: created=%v error=%v", created, err)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Ready: true, Capacity: 2, Active: 1, Heartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if status := coordinator.Status(); status.Capacity.Degraded || status.Capacity.Available != 1 {
		t.Fatalf("ready worker capacity was not visible: %+v", status)
	}
	now = now.Add(time.Minute)
	if status := coordinator.Status(); !status.Healthy || !status.Capacity.Degraded || status.Capacity.Available != 0 {
		t.Fatalf("stale worker remained available: %+v", status)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Ready: true, Capacity: 2, Heartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Ready: true, Capacity: 2, Heartbeat: now.Add(-time.Second)}); !errors.Is(err, transition.ErrStaleWorkerHeartbeat) {
		t.Fatalf("stale heartbeat replaced a fresh report: %v", err)
	}
	if status := coordinator.Status(); status.Capacity.Degraded || status.Capacity.Available != 2 {
		t.Fatalf("fresh capacity was not preserved: %+v", status)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Draining: true, Capacity: 2, Active: 1, Heartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if status := coordinator.Status(); !status.Capacity.Degraded || status.Capacity.Available != 0 {
		t.Fatalf("draining worker remained available: %+v", status)
	}
}
