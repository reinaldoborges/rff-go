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

type OperationRepeatStore interface {
	OperationStore
	transition.OperationReconciliationStore
}

// RunOperationSafeRepeat verifies the durable adapter only opens a new claim
// for an exact, currently valid provider guarantee and fences the old report.
func RunOperationSafeRepeat(t *testing.T, newStore func(*testing.T) OperationRepeatStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	start := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "repeat", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: start.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 4, AttemptTimeout: time.Minute, OverallTimeout: time.Hour, RetryDelays: []time.Duration{0}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "repeat", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	firstActivity, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: start})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := store.ClaimHook(ctx, "first-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, oldLease, clock); err != nil {
		t.Fatal(err)
	}
	binding := transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "repeat", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "v1"},
	}
	equal := func(_ context.Context, left, right []byte) (bool, error) { return bytes.Equal(left, right), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, oldLease, clock, binding, equal); err != nil || !started {
		t.Fatalf("first dispatch marker: started=%v err=%v", started, err)
	}
	now = start.Add(2 * time.Minute)
	if err := store.ResolveUnknownHook(ctx, fence, clock, firstActivity, 1, transition.HookResult{
		Failure: fsm.Failure{Code: "provider-timeout"}, Disposition: fsm.DispositionRetry,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{
		Kind: transition.HookRetry, RetryCycle: 1, DueAt: now, Failure: &fsm.Failure{Code: "provider-timeout"},
	}); err != nil {
		t.Fatal(err)
	}
	newLease, err := store.ClaimHook(ctx, "replacement-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, newLease, clock); err != nil {
		t.Fatal(err)
	}
	evidence := transition.OperationEvidence{
		Identity: binding.Identity, Outcome: transition.OperationOutcomeUnknown,
		Provider: "payment-sandbox", Scope: "merchant-7", Reference: "charge-key-44",
		InputFingerprint: "semantic-input-fingerprint-v1",
		Version:          "lookup-v1", Provenance: "provider-idempotency-lookup",
		BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
		ObservedAt: now, ValidUntil: now.Add(time.Minute),
	}
	guarantee := transition.SafeRepeatGuarantee{
		Identity: binding.Identity, Provider: evidence.Provider, Scope: evidence.Scope, Reference: evidence.Reference,
		InputFingerprint: evidence.InputFingerprint, Version: "idempotency-retention-v1",
		BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
		ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(2 * time.Minute), OverlapUntil: now.Add(time.Minute),
	}
	assessment := transition.OperationAssessment{Outcome: transition.OperationOutcomeUnknown, Evidence: evidence, Repeat: &guarantee}
	result, err := store.ReconcileTrackedOperation(ctx, newLease, clock, binding, assessment, equal)
	if err != nil || !result.Dispatch || result.Resolved {
		t.Fatalf("verified repeat was not fenced and authorized: %+v %v", result, err)
	}
	if err := store.CompleteTrackedOperation(ctx, oldLease, clock, binding.Identity, transition.OperationResult{
		Codec: binding.ResultCodec, Data: []byte("late-stale-result"),
	}); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("old attempt was not fenced by reconciliation: %v", err)
	}
	if err := store.CompleteTrackedOperation(ctx, newLease, clock, binding.Identity, transition.OperationResult{
		Codec: binding.ResultCodec, Data: []byte("repeat-receipt"),
	}); err != nil {
		t.Fatal(err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || inspection.Result == nil || string(inspection.Result.Data) != "repeat-receipt" ||
		len(inspection.History) < 2 || inspection.History[0].Assessment.Evidence.Outcome != transition.OperationOutcomeUnknown {
		t.Fatalf("repeat evidence or result was not retained: %+v %v", inspection, err)
	}

	absenceBinding := binding
	absenceBinding.Identity.Key = "quiescent-absence"
	if _, started, err := store.BeginTrackedOperation(ctx, newLease, clock, absenceBinding, equal); err != nil || !started {
		t.Fatalf("quiescent-absence operation start: started=%v err=%v", started, err)
	}
	if err := store.ReportHook(ctx, newLease, transition.HookResult{
		Failure: fsm.Failure{Code: "second-operation-pending"}, Disposition: fsm.DispositionRetry,
	}, clock); err != nil {
		t.Fatalf("report retryable hook outcome: %v", err)
	}
	if err := store.RedeliverHook(ctx, fence, clock, newLease.ActivityID, newLease.Generation, now); err != nil {
		t.Fatalf("redeliver hook retry: %v", err)
	}
	finalLease, err := store.ClaimHook(ctx, "final-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, finalLease, clock); err != nil {
		t.Fatal(err)
	}
	absenceEvidence := transition.OperationEvidence{
		Identity: absenceBinding.Identity, Outcome: transition.OperationOutcomeAbsent,
		Provider: "payment-sandbox", Scope: "merchant-7", Reference: "absence-query-1",
		InputFingerprint: "semantic-input-fingerprint-v1",
		Version:          "lookup-v1", Provenance: "authoritative-absence",
		BehaviorVersion: absenceBinding.BehaviorVersion, ComparatorVersion: absenceBinding.ComparatorVersion,
		ObservedAt: now, ValidUntil: now.Add(time.Minute), Quiesced: true,
	}
	replayed, err := store.ReconcileTrackedOperation(ctx, finalLease, clock, absenceBinding, transition.OperationAssessment{
		Outcome: transition.OperationOutcomeAbsent, Evidence: absenceEvidence,
	}, equal)
	if err != nil || !replayed.Dispatch || replayed.Resolved {
		t.Fatalf("authoritative quiescent absence did not authorize a retry: %+v %v", replayed, err)
	}
	if err := store.CompleteTrackedOperation(ctx, finalLease, clock, absenceBinding.Identity, transition.OperationResult{
		Codec: absenceBinding.ResultCodec, Data: []byte("receipt-after-absence"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, finalLease, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	inspection, err = store.InspectTrackedOperation(ctx, absenceBinding.Identity)
	if err != nil || inspection.Result == nil || string(inspection.Result.Data) != "receipt-after-absence" {
		t.Fatalf("quiescent-absence retry result was not retained: %+v %v", inspection, err)
	}
}
