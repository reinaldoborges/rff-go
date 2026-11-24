package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type OperationStore interface {
	Store
	transition.OperationStore
	transition.OperationInspector
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
}

// RunOperation checks durable matching, replay, and terminal fencing.
func RunOperation(t *testing.T, newStore func(*testing.T) OperationStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			MachineID: "machine", MachineVersion: "1",
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			Continuation: &transition.ContinuationSnapshot{
				MachineID: "machine", MachineVersion: "1",
				Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
			},
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
	codec := transition.CodecVersion{Name: "result", Version: "1"}
	binding := transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "execution", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "1", ComparatorVersion: "bytes-v1", Input: []byte("amount=100"), ResultCodec: codec,
	}
	equal := func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil }
	invalid := binding
	invalid.Identity.Key = "invalid"
	invalid.Kind = ""
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, invalid, equal); !errors.Is(err, transition.ErrInvalidRequest) {
		t.Fatalf("invalid binding was accepted: %v", err)
	}
	result, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal)
	if err != nil || !started || result != nil {
		t.Fatalf("start: result=%+v started=%v err=%v", result, started, err)
	}
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); !errors.Is(err, transition.ErrOperationUnresolved) {
		t.Fatalf("in-flight operation did not block: %v", err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, binding.Identity, transition.OperationResult{Codec: codec, Data: []byte("receipt")}); err != nil {
		t.Fatal(err)
	}
	unavailable := binding
	unavailable.Identity.Key = "known-success-unavailable"
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, unavailable, equal); err != nil || !started {
		t.Fatalf("known-success case did not start: started=%v err=%v", started, err)
	}
	unavailableResult := transition.OperationResult{Codec: codec, Unavailable: true}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, unavailable.Identity, unavailableResult); err != nil {
		t.Fatalf("known success with unavailable result was not retained: %v", err)
	}
	replayedUnavailable, started, err := store.BeginTrackedOperation(ctx, lease, clock, unavailable, equal)
	if err != nil || started || replayedUnavailable == nil || !replayedUnavailable.Unavailable {
		t.Fatalf("known success with unavailable result permitted another dispatch: result=%+v started=%v err=%v",
			replayedUnavailable, started, err)
	}
	binaryKey := binding
	binaryKey.Identity.Key = string([]byte{'k', 0xff})
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binaryKey, equal); err != nil || !started {
		t.Fatalf("binary key did not start: %v", err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, binaryKey.Identity, transition.OperationResult{Codec: codec, NoResult: true}); err != nil {
		t.Fatalf("binary key was not preserved: %v", err)
	}
	if result, started, err := store.BeginTrackedOperation(ctx, lease, clock, binaryKey, equal); err != nil || started || result == nil || !result.NoResult {
		t.Fatalf("binary key no-result replay failed: result=%+v started=%v err=%v", result, started, err)
	}
	for _, key := range []string{strings.Repeat("x", 300) + "A", strings.Repeat("x", 300) + "a"} {
		distinct := binding
		distinct.Identity.Key = key
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, distinct, equal); err != nil || !started {
			t.Fatalf("distinct long identity did not start: %v", err)
		}
		if err := store.CompleteTrackedOperation(ctx, lease, clock, distinct.Identity, transition.OperationResult{Codec: codec, NoResult: true}); err != nil {
			t.Fatalf("distinct long identity did not complete: %v", err)
		}
	}
	concurrent := binding
	concurrent.Identity.Key = "concurrent"
	type startResult struct {
		started bool
		err     error
	}
	outcomes := make(chan startResult, 8)
	var callers sync.WaitGroup
	for i := 0; i < cap(outcomes); i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			_, started, err := store.BeginTrackedOperation(ctx, lease, clock, concurrent, equal)
			outcomes <- startResult{started, err}
		}()
	}
	callers.Wait()
	close(outcomes)
	startedCount := 0
	for outcome := range outcomes {
		if outcome.started && outcome.err == nil {
			startedCount++
		} else if !errors.Is(outcome.err, transition.ErrOperationUnresolved) {
			t.Fatalf("concurrent caller was not blocked: %+v", outcome)
		}
	}
	if startedCount != 1 {
		t.Fatalf("matching concurrent callers started %d dispatches", startedCount)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, concurrent.Identity, transition.OperationResult{Codec: codec, NoResult: true}); err != nil {
		t.Fatal(err)
	}
	result, started, err = store.BeginTrackedOperation(ctx, lease, clock, binding, equal)
	if err != nil || started || result == nil || string(result.Data) != "receipt" {
		t.Fatalf("replay: result=%+v started=%v err=%v", result, started, err)
	}
	expiredEqual := func(_ context.Context, a, b []byte) (bool, error) {
		now = now.Add(2 * time.Minute)
		return bytes.Equal(a, b), nil
	}
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, binding, expiredEqual); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("expired replay claim was accepted: %v", err)
	}
	now = now.Add(-2 * time.Minute)
	changed := binding
	changed.Input = []byte("amount=200")
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, changed, equal); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed semantics did not conflict: %v", err)
	}
	unverifiable := func(context.Context, []byte, []byte) (bool, error) {
		return false, errors.New("comparator unavailable")
	}
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, binding, unverifiable); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unverifiable input did not block: %v", err)
	}
	changed = binding
	changed.BehaviorVersion = "2"
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, changed, equal); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed operation behavior did not conflict: %v", err)
	}
	changed = binding
	changed.ResultCodec.Version = "2"
	if _, _, err := store.BeginTrackedOperation(ctx, lease, clock, changed, equal); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed result codec did not conflict: %v", err)
	}
	unknown := binding
	unknown.Identity.Key = "second"
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, unknown, equal); err != nil || !started {
		t.Fatalf("second identity did not start: %v", err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, unknown.Identity)
	if err != nil || inspection.Binding.Identity != unknown.Identity || !inspection.MayHaveDispatched || inspection.Result != nil {
		t.Fatalf("unresolved operation was not inspectable: %+v, %v", inspection, err)
	}
	if err := store.ReportHook(ctx, lease, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkWorkSuccess(ctx, fence, clock); !errors.Is(err, transition.ErrOperationUnresolved) {
		t.Fatalf("unresolved dispatch allowed work success: %v", err)
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 0 {
		t.Fatalf("unresolved operation created a continuation: %+v, %v", markers, err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, unknown.Identity, transition.OperationResult{Codec: codec}); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("completed scope accepted late result: %v", err)
	}
}
