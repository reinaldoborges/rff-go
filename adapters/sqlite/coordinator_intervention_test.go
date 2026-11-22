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

func TestCoordinatorEntersInterventionWhenRetryConsumesLastAttempt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "intervention.db")
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
			Entity: "entity", Key: "key", Operation: transition.Initialize, Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
				MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
				RetryDelays: []time.Duration{0},
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	failure := fsm.Failure{Code: "work-failed"}
	if err := store.ReportHook(ctx, claim, transition.HookResult{Failure: failure, Disposition: fsm.DispositionRetry}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	retry, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, retry, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, retry, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.NeedsIntervention || execution.Failure == nil || *execution.Failure != failure {
		t.Fatalf("exhausted execution did not retain its failure for intervention: record=%+v error=%v", execution, err)
	}
	if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("work was dispatched after budget exhaustion: %v", err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyHookControl(ctx, fence, clock, transition.HookControl{ID: "retry", Action: transition.RetryIntervention}); err != nil {
		t.Fatal(err)
	}
	coordinator, err = transition.NewCoordinator[string, string](store, unknownClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartHook(ctx, resumed, clock)
	if err != nil || started.Attempt.Number != 3 {
		t.Fatalf("authorized retry did not reset budget and preserve numbering: attempt=%+v error=%v", started, err)
	}
}
