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

type OperationPrerequisiteStore interface {
	OperationStore
	transition.OperationReconciliationStore
	transition.AcceptanceStore[string, string]
	transition.CoordinatorStore[string, string]
	transition.HookActivityStore[string, string]
	transition.OperationInspector
}

// RunOperationPrerequisites verifies durable compensation dependencies,
// fail-closed handling, stable identities, and cycle rejection.
func RunOperationPrerequisites(t *testing.T, newStore func(*testing.T) OperationPrerequisiteStore) {
	t.Helper()
	t.Run("settled prerequisite and distinct compensation identity", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		equal := operationInputEqual
		charge := prerequisiteBinding("charge", false, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, charge, equal); err != nil || !started {
			t.Fatalf("start charge: started=%v err=%v", started, err)
		}
		if err := store.CompleteTrackedOperation(ctx, lease, clock, charge.Identity, prerequisiteResult()); err != nil {
			t.Fatal(err)
		}

		refund := prerequisiteBinding("refund", true, []string{"charge"})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, refund, equal); err != nil || !started {
			t.Fatalf("start refund after settled charge: started=%v err=%v", started, err)
		}
		if err := store.CompleteTrackedOperation(ctx, lease, clock, refund.Identity, prerequisiteResult()); err != nil {
			t.Fatal(err)
		}
		replayed, started, err := store.BeginTrackedOperation(ctx, lease, clock, refund, equal)
		if err != nil || started || replayed == nil || string(replayed.Data) != "receipt" {
			t.Fatalf("refund replay: result=%+v started=%v err=%v", replayed, started, err)
		}
		chargeInspection, err := store.InspectTrackedOperation(ctx, charge.Identity)
		if err != nil || chargeInspection.Binding.Identity.Key != "charge" {
			t.Fatalf("charge identity was not retained: %+v err=%v", chargeInspection, err)
		}
		refundInspection, err := store.InspectTrackedOperation(ctx, refund.Identity)
		if err != nil || !refundInspection.Binding.Compensation ||
			!equalStrings(refundInspection.Binding.Prerequisites, []string{"charge"}) {
			t.Fatalf("compensation dependencies were not retained: %+v err=%v", refundInspection, err)
		}
	})

	t.Run("missing and unresolved prerequisites block dispatch", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		equal := operationInputEqual
		refund := prerequisiteBinding("refund", true, []string{"charge"})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, refund, equal); !errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) || started {
			t.Fatalf("missing prerequisite did not block refund: started=%v err=%v", started, err)
		}
		declaration, err := store.InspectTrackedOperation(ctx, refund.Identity)
		if err != nil || declaration.Binding.Identity != refund.Identity || declaration.MayHaveDispatched || !declaration.PrerequisiteBlocked {
			t.Fatalf("blocked dependency declaration was not retained without dispatch: %+v err=%v", declaration, err)
		}
		changedDeclaration := refund
		changedDeclaration.Prerequisites = []string{}
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, changedDeclaration, equal); !errors.Is(err, transition.ErrConflict) || started {
			t.Fatalf("changed prerequisite binding did not conflict: started=%v err=%v", started, err)
		}

		charge := prerequisiteBinding("charge", false, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, charge, equal); err != nil || !started {
			t.Fatalf("start prerequisite: started=%v err=%v", started, err)
		}
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, refund, equal); !errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) || started {
			t.Fatalf("in-flight prerequisite did not block refund: started=%v err=%v", started, err)
		}
		if err := store.CompleteTrackedOperation(ctx, lease, clock, charge.Identity, prerequisiteResult()); err != nil {
			t.Fatal(err)
		}
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, refund, equal); err != nil || !started {
			t.Fatalf("refund did not proceed after prerequisite settled: started=%v err=%v", started, err)
		}
	})

	t.Run("incomplete compensation declaration blocks unresolved effects", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		equal := operationInputEqual
		charge := prerequisiteBinding("charge", false, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, charge, equal); err != nil || !started {
			t.Fatalf("start unknown charge: started=%v err=%v", started, err)
		}
		undeclared := prerequisiteBinding("refund", true, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, undeclared, equal); !errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) || started {
			t.Fatalf("missing dependency declaration did not fail closed: started=%v err=%v", started, err)
		}
		independent := prerequisiteBinding("independent", true, []string{})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, independent, equal); err != nil || !started {
			t.Fatalf("explicitly independent compensation was blocked: started=%v err=%v", started, err)
		}
	})

	t.Run("nil prerequisite declaration cannot be retried as independent", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		equal := operationInputEqual
		charge := prerequisiteBinding("charge", false, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, charge, equal); err != nil || !started {
			t.Fatalf("start unresolved charge: started=%v err=%v", started, err)
		}

		incomplete := prerequisiteBinding("refund", true, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, incomplete, equal); !errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) || started {
			t.Fatalf("incomplete declaration did not fail closed: started=%v err=%v", started, err)
		}
		independent := incomplete
		independent.Prerequisites = []string{}
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, independent, equal); !errors.Is(err, transition.ErrConflict) || started {
			t.Fatalf("nil declaration was retried as independent: started=%v err=%v", started, err)
		}
	})

	t.Run("independent prerequisite declaration cannot be retried as incomplete", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		equal := operationInputEqual
		independent := prerequisiteBinding("refund", true, []string{})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, independent, equal); err != nil || !started {
			t.Fatalf("start independent compensation: started=%v err=%v", started, err)
		}
		incomplete := independent
		incomplete.Prerequisites = nil
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, incomplete, equal); !errors.Is(err, transition.ErrConflict) || started {
			t.Fatalf("independent declaration was retried as incomplete: started=%v err=%v", started, err)
		}
		now := clock()
		assessment := transition.OperationAssessment{
			Outcome: transition.OperationOutcomeUnknown,
			Evidence: transition.OperationEvidence{
				Identity: independent.Identity, Outcome: transition.OperationOutcomeUnknown,
				Provider: "payment-sandbox", Scope: "merchant-7", Reference: "refund-1",
				InputFingerprint: "refund-input-v1", Version: "lookup-v1", Provenance: "authoritative-query",
				BehaviorVersion: independent.BehaviorVersion, ComparatorVersion: independent.ComparatorVersion,
				ObservedAt: now, ValidUntil: now.Add(time.Minute),
			},
		}
		if _, err := store.ReconcileTrackedOperation(ctx, lease, clock, incomplete, assessment, equal); !errors.Is(err, transition.ErrConflict) {
			t.Fatalf("reconciliation accepted changed prerequisite nilness: %v", err)
		}
	})

	t.Run("cycles are retained and rejected", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		equal := operationInputEqual
		first := prerequisiteBinding("first", true, []string{"second"})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, first, equal); !errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) || started {
			t.Fatalf("missing forward prerequisite did not block: started=%v err=%v", started, err)
		}
		second := prerequisiteBinding("second", true, []string{"first"})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, second, equal); !errors.Is(err, transition.ErrOperationPrerequisiteCycle) || started {
			t.Fatalf("cycle was not rejected: started=%v err=%v", started, err)
		}
		if _, err := store.InspectTrackedOperation(ctx, second.Identity); err != nil {
			t.Fatalf("cyclic declaration was not retained for inspection: %v", err)
		}
		self := prerequisiteBinding("self", true, []string{"self"})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, self, equal); !errors.Is(err, transition.ErrOperationPrerequisiteCycle) || started {
			t.Fatalf("self dependency was not rejected: started=%v err=%v", started, err)
		}
	})

	t.Run("conflicting prerequisite evidence blocks compensation", func(t *testing.T) {
		store, lease, clock := startOperationAttempt(t, newStore)
		ctx := context.Background()
		charge := prerequisiteBinding("charge", false, nil)
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, charge, operationInputEqual); err != nil || !started {
			t.Fatalf("start charge: started=%v err=%v", started, err)
		}
		if err := store.CompleteTrackedOperation(ctx, lease, clock, charge.Identity, prerequisiteResult()); err != nil {
			t.Fatal(err)
		}
		now := clock()
		conflict := transition.OperationAssessment{
			Outcome: transition.OperationOutcomeFailed,
			Evidence: transition.OperationEvidence{
				Identity: charge.Identity, Outcome: transition.OperationOutcomeFailed,
				Provider: "payment-sandbox", Scope: "merchant-7", Reference: "charge-2",
				InputFingerprint: "charge-input-v1", Version: "lookup-v1", Provenance: "authoritative-query",
				BehaviorVersion: charge.BehaviorVersion, ComparatorVersion: charge.ComparatorVersion,
				ObservedAt: now, ValidUntil: now.Add(time.Minute),
			},
			Result: &transition.OperationResult{Codec: charge.ResultCodec, FailureCode: "declined"},
		}
		if _, err := store.ReconcileTrackedOperation(ctx, lease, clock, charge, conflict, operationInputEqual); !errors.Is(err, transition.ErrOperationEvidenceConflict) {
			t.Fatalf("contradictory prerequisite evidence was not retained: %v", err)
		}
		refund := prerequisiteBinding("refund", true, []string{"charge"})
		if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, refund, operationInputEqual); !errors.Is(err, transition.ErrOperationEvidenceConflict) || started {
			t.Fatalf("conflicting prerequisite allowed compensation dispatch: started=%v err=%v", started, err)
		}
	})
}

func startOperationAttempt(t *testing.T, newStore func(*testing.T) OperationPrerequisiteStore) (OperationPrerequisiteStore, transition.HookLease, func() time.Time) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "compensation", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "compensation", "coordinator", clock, time.Hour)
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
	return store, lease, clock
}

func prerequisiteBinding(key string, compensation bool, prerequisites []string) transition.OperationBinding {
	return transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "compensation", Key: key},
		Kind:     "payment", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "v1"},
		Compensation: compensation, Prerequisites: prerequisites,
	}
}

func prerequisiteResult() transition.OperationResult {
	return transition.OperationResult{Codec: transition.CodecVersion{Name: "receipt", Version: "v1"}, Data: []byte("receipt")}
}

func operationInputEqual(_ context.Context, left, right []byte) (bool, error) {
	return bytes.Equal(left, right), nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
