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

func TestAuthorizedHookControlDenialPhaseGateAndReplay(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "control.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
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
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, 3*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	denied := errors.New("not authorized")
	allowed := false
	var authorized []transition.ControlAuthorization
	controls, err := transition.NewAuthorizedControls[string](store, func(_ context.Context, request transition.ControlAuthorization) error {
		authorized = append(authorized, request)
		if !allowed {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	control := transition.HookControl{ID: "retry", Action: transition.RetryIntervention}
	if _, err := controls.Hook(ctx, lease, clock, control); !errors.Is(err, denied) {
		t.Fatalf("unauthorized control was not denied: %v", err)
	}
	allowed = true
	if _, err := controls.Hook(ctx, lease, clock, control); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("invalid phase was not rejected: %v", err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, claim, transition.HookResult{
		Failure: fsm.Failure{Code: "failed"}, Disposition: fsm.DispositionIntervention,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterHookIntervention(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	allowed = false
	if _, err := controls.Hook(ctx, lease, clock, control); !errors.Is(err, denied) {
		t.Fatalf("intervention denial was not enforced: %v", err)
	}
	before, err := store.InspectExecution(ctx, "execution")
	if err != nil || before.Status != transition.NeedsIntervention || len(before.Controls) != 0 {
		t.Fatalf("denied control mutated execution: inspection=%+v error=%v", before, err)
	}
	allowed = true
	receipt, err := controls.Hook(ctx, lease, clock, control)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Minute)
	allowed = false
	if _, err := controls.Hook(ctx, lease, clock, control); !errors.Is(err, denied) {
		t.Fatalf("control replay bypassed authorization: %v", err)
	}
	allowed = true
	replayed, err := controls.Hook(ctx, lease, clock, control)
	if err != nil || !replayed.Deadline.Equal(receipt.Deadline) || !replayed.AcceptedAt.Equal(receipt.AcceptedAt) {
		t.Fatalf("authorized replay renewed the control cycle: receipt=%+v error=%v", replayed, err)
	}
	if len(authorized) != 6 || authorized[0].ExecutionID != "execution" ||
		authorized[0].RequestID != "retry" || authorized[0].Action != transition.ControlRetry {
		t.Fatalf("host did not receive authorization details: %+v", authorized)
	}
}
