package sqlite_test

import (
	"context"
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
