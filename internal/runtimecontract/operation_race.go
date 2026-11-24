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

// RunOperationSuccessRace checks the boundary between operation start and work success.
func RunOperationSuccessRace(t *testing.T, newStore func(*testing.T) OperationStore) {
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
	start := make(chan struct{})
	type operationOutcome struct {
		started bool
		err     error
	}
	operation := make(chan operationOutcome, 1)
	success := make(chan error, 1)
	go func() {
		<-start
		_, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal)
		operation <- operationOutcome{started, err}
	}()
	go func() {
		<-start
		err := store.ReportHook(ctx, lease, transition.HookResult{Success: true}, clock)
		if err == nil {
			_, err = store.MarkWorkSuccess(ctx, fence, clock)
		}
		success <- err
	}()
	close(start)
	op, successErr := <-operation, <-success
	if op.started && op.err == nil {
		if !errors.Is(successErr, transition.ErrOperationUnresolved) {
			t.Fatalf("work success raced past an operation: %v", successErr)
		}
	} else if !errors.Is(op.err, transition.ErrStaleActivityLease) || successErr != nil {
		t.Fatalf("unexpected race results: operation=%+v success=%v", op, successErr)
	}
}
