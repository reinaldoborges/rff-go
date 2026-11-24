package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type OperationReconciliationStore interface {
	OperationStore
	transition.OperationReconciliationStore
	transition.OperationResolutionStore
	transition.CheckedHookControlStore[string, string]
}

// RunOperationReconciliation exercises durable unknown resolution, operator
// authorization, replay, stale-report fencing, and conflicting evidence.
func RunOperationReconciliation(t *testing.T, newStore func(*testing.T) OperationReconciliationStore) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "request", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "reconciliation", Entity: "entity", Key: "request",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "reconciliation", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	activityID, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, lease, clock); err != nil {
		t.Fatal(err)
	}
	binding := transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "reconciliation", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "v1"},
	}
	equal := func(_ context.Context, a, b []byte) (bool, error) { return string(a) == string(b), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("operation did not mark dispatch before provider work: started=%v err=%v", started, err)
	}
	providerCalls := 1
	if providerCalls != 1 {
		t.Fatal("provider call count changed")
	}
	failure := fsm.Failure{Code: "provider-response-lost"}
	if err := store.ReportHook(ctx, lease, transition.HookResult{
		Failure: failure, Disposition: fsm.DispositionIntervention,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterHookIntervention(ctx, fence, clock); err != nil {
		t.Fatal(err)
	}

	evidence := transition.OperationEvidence{
		Identity: binding.Identity, Outcome: transition.OperationOutcomeSucceeded,
		Provider: "payment-sandbox", Scope: "merchant-7", Reference: "lookup-123",
		InputFingerprint: "input-fingerprint-v1",
		Version:          "lookup-v1", Provenance: "authoritative-query",
		BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
		ObservedAt: now, ValidUntil: now.Add(time.Minute),
	}
	assessment := transition.OperationAssessment{
		Outcome: transition.OperationOutcomeSucceeded, Evidence: evidence,
		Result: &transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt-100")},
	}
	authorizationCalls := 0
	resolver, err := transition.NewAuthorizedOperationResolver(store, func(_ context.Context, request transition.OperationResolutionRequest) (transition.OperationResolutionAuthorization, error) {
		authorizationCalls++
		if request.Identity != binding.Identity {
			t.Fatalf("authorization received the wrong identity: %+v", request.Identity)
		}
		return transition.OperationResolutionAuthorization{Principal: "operator-1", Reason: "verified provider receipt"}, nil
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.OperationResolutionRequest{
		ID: "resolution-1", Identity: binding.Identity, Assessment: assessment,
		VerifyEvidence: func(_ context.Context, actual transition.OperationBinding, evidence transition.OperationEvidence) error {
			if actual.Identity != evidence.Identity || evidence.Reference == "" {
				return errors.New("evidence does not match the operation")
			}
			return nil
		},
		ValidateResult: func(_ context.Context, actual transition.OperationBinding, result transition.OperationResult) error {
			if actual.ResultCodec != result.Codec || string(result.Data) != "receipt-100" {
				return errors.New("missing or incompatible recovered result")
			}
			return nil
		},
	}
	denied, err := transition.NewAuthorizedOperationResolver(store, func(context.Context, transition.OperationResolutionRequest) (transition.OperationResolutionAuthorization, error) {
		return transition.OperationResolutionAuthorization{}, errors.New("denied")
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Resolve(ctx, request); !errors.Is(err, transition.ErrOperationResolutionDenied) {
		t.Fatalf("unauthorized resolution succeeded: %v", err)
	}
	before, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || before.Result != nil {
		t.Fatalf("denied resolution changed the operation: %+v %v", before, err)
	}

	inspection, err := resolver.Resolve(ctx, request)
	if err != nil || inspection.Result == nil || string(inspection.Result.Data) != "receipt-100" ||
		inspection.ActivityID != "operator:resolution-1" || len(inspection.History) < 1 ||
		inspection.History[len(inspection.History)-1].Principal != "operator-1" {
		t.Fatalf("authorized resolution was not durably audited: %+v %v", inspection, err)
	}
	execution, err := store.ReadActiveExecution(ctx, "reconciliation")
	if err != nil || execution.Status != transition.NeedsIntervention {
		t.Fatalf("operator resolution resumed work without a separate retry: status=%s err=%v", execution.Status, err)
	}
	duplicate, err := resolver.Resolve(ctx, request)
	if err != nil || len(duplicate.History) != len(inspection.History) || authorizationCalls != 2 {
		t.Fatalf("identical resolution replay was not idempotent and reauthorized: %+v calls=%d err=%v", duplicate, authorizationCalls, err)
	}
	changed := request
	changed.ID = "resolution-1"
	changed.Assessment = assessment
	changed.Assessment.Result = &transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("different-receipt")}
	changed.ValidateResult = func(_ context.Context, actual transition.OperationBinding, result transition.OperationResult) error {
		if actual.ResultCodec != result.Codec || len(result.Data) == 0 {
			return errors.New("invalid result")
		}
		return nil
	}
	if _, err := resolver.Resolve(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed resolution replay did not conflict: %v", err)
	}
	if err := store.CompleteTrackedOperation(ctx, lease, clock, binding.Identity, *assessment.Result); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("operator resolution did not fence the stale hook report: %v", err)
	}
	execution, err = store.ReadActiveExecution(ctx, "reconciliation")
	if err != nil || execution.Status != transition.NeedsIntervention || providerCalls != 1 {
		t.Fatalf("operator resolution resumed or redispatched work: status=%s calls=%d err=%v", execution.Status, providerCalls, err)
	}

	conflict := transition.OperationAssessment{
		Outcome: transition.OperationOutcomeAbsent,
		Evidence: transition.OperationEvidence{
			Identity: binding.Identity, Outcome: transition.OperationOutcomeAbsent,
			Provider: "payment-sandbox", Scope: "merchant-7", Reference: "lookup-124",
			InputFingerprint: "input-fingerprint-v1",
			Version:          "lookup-v1", Provenance: "authoritative-query",
			BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
			ObservedAt: now, ValidUntil: now.Add(time.Minute), Quiesced: true,
		},
	}
	conflictingRequest := request
	conflictingRequest.ID = "resolution-2"
	conflictingRequest.Assessment = conflict
	if _, err := resolver.Resolve(ctx, conflictingRequest); !errors.Is(err, transition.ErrOperationEvidenceConflict) {
		t.Fatalf("conflicting later evidence was not surfaced: %v", err)
	}
	inspection, err = store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || !inspection.EvidenceConflict {
		t.Fatalf("conflicting evidence was not retained for intervention: %+v %v", inspection, err)
	}
	historyCount := len(inspection.History)
	replayedConflict, replayErr := resolver.Resolve(ctx, conflictingRequest)
	if !errors.Is(replayErr, transition.ErrOperationEvidenceConflict) ||
		!replayedConflict.EvidenceConflict || len(replayedConflict.History) != historyCount {
		t.Fatalf("conflicting resolution replay hid conflict or duplicated audit history: inspection=%+v err=%v", replayedConflict, replayErr)
	}
	if activityID == "" {
		t.Fatal("hook activity identity was not retained")
	}
	RunOperationResolutionReplayAfterRetry(t, newStore)
	RunReconciliationSuccessUnavailable(t, newStore)
}

func RunReconciliationSuccessUnavailable(
	t *testing.T,
	newStore func(*testing.T) OperationReconciliationStore,
) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "missing-result", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "missing-result", Entity: "entity", Key: "missing-result",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "missing-result", "coordinator", clock, time.Hour)
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
	binding := transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "missing-result", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "v1"},
	}
	equal := func(_ context.Context, a, b []byte) (bool, error) { return string(a) == string(b), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("operation did not retain its unknown dispatch: started=%v err=%v", started, err)
	}
	assessment := transition.OperationAssessment{
		Outcome: transition.OperationOutcomeSucceeded,
		Evidence: transition.OperationEvidence{
			Identity: binding.Identity, Outcome: transition.OperationOutcomeSucceeded,
			Provider: "payment-sandbox", Scope: "merchant-7", Reference: "lookup-125",
			InputFingerprint: "input-fingerprint-v1",
			Version:          "lookup-v1", Provenance: "authoritative-query",
			BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
			ObservedAt: now, ValidUntil: now.Add(time.Minute),
		},
		Result: &transition.OperationResult{Codec: binding.ResultCodec, Unavailable: true},
	}
	reconciled, err := store.ReconcileTrackedOperation(ctx, lease, clock, binding, assessment, equal)
	if err != nil || !reconciled.Resolved || reconciled.Result == nil || !reconciled.Result.Unavailable {
		t.Fatalf("confirmed success without typed result was not durably settled: %+v %v", reconciled, err)
	}
	inspection, err := store.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil || inspection.Result == nil || !inspection.Result.Unavailable {
		t.Fatalf("durable inspection did not expose known success with unavailable result: %+v %v", inspection, err)
	}
	replayed, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal)
	if err != nil || started || replayed == nil || !replayed.Unavailable {
		t.Fatalf("settled success did not block a later dispatch: result=%+v started=%v err=%v", replayed, started, err)
	}
}

func RunOperationResolutionReplayAfterRetry(
	t *testing.T,
	newStore func(*testing.T) OperationReconciliationStore,
) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, "entity", "resolution-replay", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "resolution-replay", Entity: "entity", Key: "resolution-replay",
			Operation: transition.Initialize, Status: transition.Waiting, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, "entity", "resolution-replay", "coordinator", clock, time.Hour)
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
	binding := transition.OperationBinding{
		Identity: transition.OperationIdentity{ExecutionID: "resolution-replay", Key: "charge"},
		Kind:     "charge", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: transition.CodecVersion{Name: "receipt", Version: "v1"},
	}
	equal := func(_ context.Context, a, b []byte) (bool, error) { return string(a) == string(b), nil }
	if _, started, err := store.BeginTrackedOperation(ctx, lease, clock, binding, equal); err != nil || !started {
		t.Fatalf("operation did not mark dispatch before provider work: started=%v err=%v", started, err)
	}
	if err := store.ReportHook(ctx, lease, transition.HookResult{
		Failure: fsm.Failure{Code: "provider-response-lost"}, Disposition: fsm.DispositionIntervention,
	}, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterHookIntervention(ctx, fence, clock); err != nil {
		t.Fatal(err)
	}
	evidence := transition.OperationEvidence{
		Identity: binding.Identity, Outcome: transition.OperationOutcomeSucceeded,
		Provider: "payment-sandbox", Scope: "merchant-7", Reference: "lookup-replay",
		InputFingerprint: "input-fingerprint-v1", Version: "lookup-v1", Provenance: "authoritative-query",
		BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
		ObservedAt: now, ValidUntil: now.Add(time.Minute),
	}
	assessment := transition.OperationAssessment{
		Outcome: transition.OperationOutcomeSucceeded, Evidence: evidence,
		Result: &transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("receipt-replay")},
	}
	authorizationCalls := 0
	resolver, err := transition.NewAuthorizedOperationResolver(store, func(context.Context, transition.OperationResolutionRequest) (transition.OperationResolutionAuthorization, error) {
		authorizationCalls++
		return transition.OperationResolutionAuthorization{Principal: "operator-1", Reason: "verified provider receipt"}, nil
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.OperationResolutionRequest{
		ID: "resolution-replay-1", Identity: binding.Identity, Assessment: assessment,
		VerifyEvidence: func(context.Context, transition.OperationBinding, transition.OperationEvidence) error { return nil },
		ValidateResult: func(context.Context, transition.OperationBinding, transition.OperationResult) error { return nil },
	}
	initial, err := resolver.Resolve(ctx, request)
	if err != nil || initial.Result == nil {
		t.Fatalf("initial resolution failed: inspection=%+v err=%v", initial, err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := store.ApplyCheckedHookControl(ctx, fence, clock, transition.HookControl{
		ID: "retry-after-resolution", Action: transition.RetryIntervention,
	}, func(transition.Execution[string, string], transition.HookCall) error { return nil }); err != nil {
		t.Fatalf("separate retry did not leave intervention: %v", err)
	}
	execution, err := store.ReadActiveExecution(ctx, "resolution-replay")
	if err != nil || execution.Status != transition.Waiting {
		t.Fatalf("separate retry did not leave intervention: status=%s err=%v", execution.Status, err)
	}
	controlCount := len(execution.Controls)
	replayRequest := request
	replayRequest.VerifyEvidence = nil
	replayRequest.ValidateResult = nil
	replayed, err := resolver.Resolve(ctx, replayRequest)
	if err != nil || len(replayed.History) != len(initial.History) || authorizationCalls != 2 {
		t.Fatalf("expired resolution replay after retry was not authorized and idempotent: %+v calls=%d err=%v", replayed, authorizationCalls, err)
	}
	execution, err = store.ReadActiveExecution(ctx, "resolution-replay")
	if err != nil || execution.Status != transition.Waiting || len(execution.Controls) != controlCount {
		t.Fatalf("resolution replay created a control cycle: status=%s controls=%d err=%v", execution.Status, len(execution.Controls), err)
	}
	changed := replayRequest
	changed.Assessment.Result = &transition.OperationResult{Codec: binding.ResultCodec, Data: []byte("different-receipt")}
	if _, err := resolver.Resolve(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed resolution replay did not conflict after retry: %v", err)
	}
}
