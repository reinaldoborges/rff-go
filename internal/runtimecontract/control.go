package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ControlStore combines the ports used by the authorized-control contract.
type ControlStore interface {
	Store
	transition.CheckedHookControlStore[string, string]
}

// RunAuthorizedControl checks phase gates, authorization and durable replay.
func RunAuthorizedControl(t *testing.T, newStore func(*testing.T) ControlStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			Operation: transition.Initialize, MachineID: "machine", MachineVersion: "1", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := fsm.NewMachine(fsm.Config[string, string, string, string]{
		ID: "machine", Version: "1", InitialStates: []string{"open"},
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
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	runner, err := transition.NewHookRunner[string, string, string, string](&registry, resolve, resolve)
	if err != nil {
		t.Fatal(err)
	}
	allowed := false
	denied := errors.New("control denied")
	controls, err := transition.NewAuthorizedHookControls[string, string, string, string](
		store, runner, func(_ context.Context, authorization transition.ControlAuthorization) error {
			if authorization.ExecutionID != "execution" || authorization.RequestID != "retry" ||
				authorization.Action != transition.ControlRetry {
				t.Fatalf("incorrect control authorization: %+v", authorization)
			}
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
		t.Fatalf("unauthorized control was accepted: %v", err)
	}
	allowed = true
	if _, err := controls.Hook(ctx, lease, clock, control); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("control in the wrong phase was accepted: %v", err)
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
		t.Fatalf("intervention control bypassed authorization: %v", err)
	}
	record, err := store.InspectExecution(ctx, "execution")
	if err != nil || record.Status != transition.NeedsIntervention || len(record.Controls) != 0 {
		t.Fatalf("denied control changed intervention: %+v, %v", record, err)
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
	replay, err := controls.Hook(ctx, lease, clock, control)
	if err != nil || !replay.AcceptedAt.Equal(receipt.AcceptedAt) || !replay.Deadline.Equal(receipt.Deadline) {
		t.Fatalf("control replay renewed its cycle: original=%+v replay=%+v error=%v", receipt, replay, err)
	}
}
