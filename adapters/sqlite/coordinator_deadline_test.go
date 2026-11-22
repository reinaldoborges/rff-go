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

type deadlineClassifier struct{}

func (deadlineClassifier) ClassifyHookFailure(_ transition.Execution[string, string], err error) (transition.HookResult, error) {
	if !errors.Is(err, transition.ErrTransitionDeadline) {
		return transition.HookResult{}, err
	}
	return transition.HookResult{Failure: fsm.Failure{Code: "deadline"}, Disposition: fsm.DispositionIntervention}, nil
}

func TestCoordinatorSettlesExpiredUnstartedWorkWithoutWorker(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "deadline.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Second), Policy: fsm.Policy{
				MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Second,
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, deadlineClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || execution.Status != transition.NeedsIntervention || execution.Failure == nil ||
		execution.Failure.Code != "deadline" || execution.FailureDisposition != fsm.DispositionIntervention {
		t.Fatalf("expired work did not enter intervention: execution=%+v error=%v", execution, err)
	}
	if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
		t.Fatalf("expired work was claimable after intervention: %v", err)
	}
}
