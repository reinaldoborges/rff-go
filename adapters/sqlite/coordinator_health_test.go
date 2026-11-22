package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestCoordinatorReportsPersistenceHealthSeparatelyFromWorkers(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "health.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Minute, Clock: fixedClock(now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if status := coordinator.Status(); status.Healthy || !status.LastScan.IsZero() {
		t.Fatalf("unscanned coordinator reported healthy: %+v", status)
	}
	if err := coordinator.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := coordinator.Status(); !status.Healthy || !status.LastScan.Equal(now) {
		t.Fatalf("successful persistence scan was not healthy: %+v", status)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(context.Background()); err == nil {
		t.Fatal("closed persistence store was not reported")
	}
	if status := coordinator.Status(); status.Healthy {
		t.Fatalf("failed persistence scan reported healthy: %+v", status)
	}
}

func TestCoordinatorReportsStaleAndUnavailableWorkerCapacitySeparately(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "capacity.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
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
	status := coordinator.Status()
	if !status.Healthy || !status.Capacity.Degraded || status.Capacity.Available != 0 {
		t.Fatalf("zero-worker capacity was not degraded independently of persistence: %+v", status)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Ready: true, Capacity: 2, Active: 1, Heartbeat: now}); err != nil {
		t.Fatal(err)
	}
	status = coordinator.Status()
	if status.Capacity.Degraded || status.Capacity.Available != 1 {
		t.Fatalf("available worker capacity was not reported: %+v", status)
	}
	now = now.Add(time.Minute)
	status = coordinator.Status()
	if !status.Healthy || !status.Capacity.Degraded || status.Capacity.Available != 0 {
		t.Fatalf("stale worker was treated as available: %+v", status)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Ready: true, Capacity: 2, Heartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Ready: true, Capacity: 2, Heartbeat: now.Add(-time.Second)}); !errors.Is(err, transition.ErrStaleWorkerHeartbeat) {
		t.Fatalf("older report replaced fresh heartbeat: %v", err)
	}
	if status := coordinator.Status(); status.Capacity.Available != 2 || status.Capacity.Degraded {
		t.Fatalf("fresh heartbeat was not preserved: %+v", status)
	}
	if err := workers.Report("worker", transition.WorkerStatus{Draining: true, Capacity: 2, Active: 1, Heartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if status := coordinator.Status(); !status.Capacity.Degraded || status.Capacity.Available != 0 {
		t.Fatalf("draining worker was treated as available: %+v", status)
	}
	_, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("degraded worker capacity prevented durable acceptance: created=%v error=%v", created, err)
	}
}
