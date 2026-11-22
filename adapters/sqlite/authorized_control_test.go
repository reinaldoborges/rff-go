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
			Operation: transition.Initialize, MachineID: "task", MachineVersion: "1", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("data")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
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
	available := true
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "task", Version: "1", InitialStates: []string{"open"},
		Handlers:         map[string]fsm.Handler[string, string, string, string]{"open": fsm.NoopHandler[string, string, string, string]{}},
		SelectInitial:    func(string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		SelectTransition: func(string, string, string) (fsm.Selection[string], error) { return fsm.Selection[string]{}, nil },
		ClassifyFailure: func(error) (fsm.Failure, fsm.Disposition) {
			return fsm.Failure{Code: "failed"}, fsm.DispositionIntervention
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry fsm.Registry[string, string, string, string]
	if err := registry.Register(machine); err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) {
		if !available {
			return nil, errors.New("codec unavailable")
		}
		return stringValueCodec{}, nil
	}
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	var authorized []transition.ControlAuthorization
	controls, err := transition.NewAuthorizedHookControls[string, string, string, string](store, runner, func(_ context.Context, request transition.ControlAuthorization) error {
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
	if _, err := store.ApplyCheckedHookControl(ctx, lease, clock, control, func(transition.Execution[string, string], transition.HookCall) error {
		now = now.Add(4 * time.Hour)
		return transition.NewCompatibilityError(transition.CompatibilityDiagnostic{
			Component: transition.CompatibilityInputCodec, Name: "input", Version: "1",
		}, nil)
	}); !errors.Is(err, transition.ErrStaleLease) {
		t.Fatalf("compatibility check persisted under an expired fence: %v", err)
	}
	now = now.Add(-4 * time.Hour)
	before, err = store.InspectExecution(ctx, "execution")
	if err != nil || before.Compatibility != nil || before.Status != transition.NeedsIntervention {
		t.Fatalf("expired control check changed the execution: inspection=%+v error=%v", before, err)
	}
	allowed = true
	available = false
	if _, err := controls.Hook(ctx, lease, clock, control); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable pinned codec did not block control: %v", err)
	}
	blocked, err := store.InspectExecution(ctx, "execution")
	if err != nil || blocked.Status != transition.NeedsIntervention || len(blocked.Controls) != 0 ||
		blocked.Compatibility == nil || blocked.Compatibility.Component != transition.CompatibilityInputCodec {
		t.Fatalf("incompatible control changed execution or lost its diagnostic: inspection=%+v error=%v", blocked, err)
	}
	available = true
	manager, err := transition.NewCompatibilityCoordinator[string, string, string, string](store, runner, transition.CoordinatorConfig{
		ID: "coordinator", LeaseTTL: 3 * time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
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
	if len(authorized) != 7 || authorized[0].ExecutionID != "execution" ||
		authorized[0].RequestID != "retry" || authorized[0].Action != transition.ControlRetry {
		t.Fatalf("host did not receive authorization details: %+v", authorized)
	}
}
