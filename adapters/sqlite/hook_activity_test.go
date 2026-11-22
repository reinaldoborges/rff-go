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

func TestHookActivityFencesReclaimedClaimsAndPersistsOutcome(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "activity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
			Entity: "entity", Key: "key", Operation: transition.Initialize, Target: "open",
			Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", fixedClock(now), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	activityID, err := store.DispatchHook(ctx, fence, fixedClock(now), transition.HookDispatch{
		Kind: transition.HookEnter, DueAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimHook(ctx, "worker-a", fixedClock(now), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimHook(ctx, "worker-b", fixedClock(now.Add(time.Minute)), time.Minute)
	if err != nil || second.ActivityID != activityID || second.Generation <= first.Generation {
		t.Fatalf("claim was not reclaimed with a new fence: first=%+v second=%+v error=%v", first, second, err)
	}
	if _, err := store.RenewHook(ctx, first, fixedClock(now.Add(time.Minute)), time.Minute); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("stale worker renewed a claim: %v", err)
	}
	execution, call, err := store.InspectHook(ctx, second, fixedClock(now.Add(time.Minute)))
	if err != nil || execution.ID != "execution" || call.Kind != transition.HookEnter {
		t.Fatalf("claimed work could not be inspected: execution=%+v call=%+v error=%v", execution, call, err)
	}
	started, err := store.StartHook(ctx, second, fixedClock(now.Add(time.Minute)))
	if err != nil || started.Attempt.Number != 1 || started.Attempt.LeaseGeneration != second.Generation || started.Attempt.ActivityID != activityID {
		t.Fatalf("unstarted lost claim consumed an attempt: attempt=%+v error=%v", started, err)
	}
	result := transition.HookResult{Success: true}
	if err := store.ReportHook(ctx, second, result, fixedClock(now.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, second, result, fixedClock(now.Add(time.Minute))); err != nil {
		t.Fatalf("identical report did not replay: %v", err)
	}
	changed := transition.HookResult{Failure: fsm.Failure{Code: "changed"}, Disposition: fsm.DispositionIntervention}
	if err := store.ReportHook(ctx, second, changed, fixedClock(now.Add(time.Minute))); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed report did not conflict: %v", err)
	}
	if err := store.ReportHook(ctx, first, result, fixedClock(now.Add(time.Minute))); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("stale worker reported an outcome: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || progress.ActivityID != activityID || !progress.Completed || !progress.Result.Success || len(progress.Attempts) != 1 {
		t.Fatalf("durable activity result missing: progress=%+v error=%v", progress, err)
	}
}
