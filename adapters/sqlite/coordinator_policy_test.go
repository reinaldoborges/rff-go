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

func TestCoordinatorStopsWhenPolicyCannotRetry(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disposition fsm.Disposition
		delays      []time.Duration
	}{
		{name: "empty schedule", disposition: fsm.DispositionRetry},
		{name: "intervention disposition", disposition: fsm.DispositionIntervention, delays: []time.Duration{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "policy.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
						MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: tc.delays,
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
			lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StartHook(ctx, lease, clock); err != nil {
				t.Fatal(err)
			}
			failure := fsm.Failure{Code: "classified"}
			if err := store.ReportHook(ctx, lease, transition.HookResult{Failure: failure, Disposition: tc.disposition}, clock); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			execution, err := store.ReadActiveExecution(ctx, "execution")
			if err != nil || execution.Status != transition.NeedsIntervention || execution.Failure == nil ||
				*execution.Failure != failure || execution.FailureDisposition != tc.disposition {
				t.Fatalf("policy did not retain the failure: execution=%+v error=%v", execution, err)
			}
			if _, err := store.ClaimHook(ctx, "worker", clock, time.Minute); !errors.Is(err, transition.ErrNoHookActivity) {
				t.Fatalf("policy-blocked work was claimable: %v", err)
			}
		})
	}
}
