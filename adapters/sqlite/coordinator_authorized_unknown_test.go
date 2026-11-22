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

type unknownInterventionClassifier struct{}

func (unknownInterventionClassifier) ClassifyHookFailure(_ transition.Execution[string, string], err error) (transition.HookResult, error) {
	if !errors.Is(err, transition.ErrAttemptUnknown) {
		return transition.HookResult{}, err
	}
	return transition.HookResult{Failure: fsm.Failure{Code: "unknown"}, Disposition: fsm.DispositionIntervention}, nil
}

func TestAuthorizedRetryOfUnknownOutcomeKeepsOriginalIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "unknown.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
			AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
				MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := transition.NewCoordinator[string, string](store, unknownInterventionClassifier{}, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	lost, err := store.ClaimHook(ctx, "lost-worker", clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lost, clock); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestHookControl(store, ctx, fence, clock, transition.HookControl{ID: "retry", Action: transition.RetryIntervention}); err != nil {
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
	resumed, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil || resumed.ActivityID != lost.ActivityID {
		t.Fatalf("authorized retry did not retain the unknown hook identity: lease=%+v error=%v", resumed, err)
	}
}
