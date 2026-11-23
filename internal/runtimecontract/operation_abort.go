package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type OperationControlStore interface {
	OperationStore
	transition.CheckedHookControlStore[string, string]
}

// RunOperationAbort checks that recovery may run but cannot abort unresolved effects.
func RunOperationAbort(t *testing.T, newStore func(*testing.T) OperationControlStore) {
	runOperationAbort(t, newStore, false)
}

// RunOperationAbortRace checks the atomic recovery-operation and abort boundary.
func RunOperationAbortRace(t *testing.T, newStore func(*testing.T) OperationControlStore) {
	runOperationAbort(t, newStore, true)
}

func runOperationAbort(t *testing.T, newStore func(*testing.T) OperationControlStore, race bool) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	binding := restartBinding()
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	settled := binding
	settled.Identity.Key = "known-effect"
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, settled, equal); err != nil || !started {
		t.Fatalf("known operation start: started=%v err=%v", started, err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, settled.Identity, transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt")}); err != nil {
		t.Fatal(err)
	}
	if !race {
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
			t.Fatalf("operation start: started=%v err=%v", started, err)
		}
	}
	failure := fsm.Failure{Code: "uncertain"}
	if err := store.ReportHook(ctx, lease, transition.HookResult{Failure: failure, Disposition: fsm.DispositionIntervention}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterHookIntervention(ctx, fence, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyCheckedHookControl(ctx, fence, clock, transition.HookControl{ID: "abort", Action: transition.AbortIntervention},
		func(transition.Execution[string, string], transition.HookCall) error { return nil }); err != nil {
		t.Fatal(err)
	}
	recovery, err := store.ClaimHook(ctx, "recovery-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, recovery, clock); err != nil {
		t.Fatal(err)
	}
	if result, started, err := store.BeginTrackedOperation(ctx, recovery, clock, settled, equal); err != nil || started || result == nil || string(result.Data) != "receipt" {
		t.Fatalf("recovery did not replay earlier operation: result=%+v started=%v err=%v", result, started, err)
	}
	compensation := binding
	compensation.Identity.Key = "independent-refund"
	compensation.Compensation = true
	compensation.Prerequisites = []string{}
	if !race {
		dependentCompensation := binding
		dependentCompensation.Identity.Key = "dependent-refund"
		dependentCompensation.Compensation = true
		dependentCompensation.Prerequisites = []string{binding.Identity.Key}
		if _, started, err := store.BeginTrackedOperation(ctx, recovery, clock, dependentCompensation, equal); !errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) || started {
			t.Fatalf("dependent recovery operation did not wait for the unresolved effect: started=%v err=%v", started, err)
		}
	}
	if race {
		start := make(chan struct{})
		type operationOutcome struct {
			started bool
			err     error
		}
		operation := make(chan operationOutcome, 1)
		abort := make(chan error, 1)
		go func() {
			<-start
			_, started, err := store.BeginTrackedOperation(ctx, recovery, clock, compensation, equal)
			operation <- operationOutcome{started, err}
		}()
		go func() {
			<-start
			err := store.ReportHook(ctx, recovery, transition.HookResult{Success: true}, clock)
			if err == nil {
				err = store.FinishAbort(ctx, fence, clock)
			}
			abort <- err
		}()
		close(start)
		op, abortErr := <-operation, <-abort
		if op.started && op.err == nil {
			if !errors.Is(abortErr, transition.ErrOperationUnresolved) {
				t.Fatalf("abort raced past a started operation: %v", abortErr)
			}
		} else if !errors.Is(op.err, transition.ErrStaleActivityLease) || abortErr != nil {
			t.Fatalf("unexpected abort race: operation=%+v abort=%v", op, abortErr)
		}
		return
	}
	if _, started, err := store.BeginTrackedOperation(ctx, recovery, clock, compensation, equal); err != nil || !started {
		t.Fatalf("independent recovery operation: started=%v err=%v", started, err)
	}
	if err := store.CompleteTrackedOperation(ctx, recovery, clock, compensation.Identity, transition.OperationResult{Codec: binding.ResultCodec, NoResult: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, recovery, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishAbort(ctx, fence, clock); !errors.Is(err, transition.ErrOperationUnresolved) {
		t.Fatalf("unresolved operation allowed abort: %v", err)
	}
}
