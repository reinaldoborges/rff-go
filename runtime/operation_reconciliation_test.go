package runtime

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type reconciliationTestStore struct {
	binding    OperationBinding
	result     *OperationResult
	started    bool
	evidence   []OperationEvidence
	repeat     []SafeRepeatGuarantee
	activityID string
	generation uint64
	onRepeat   func()
}

func (s *reconciliationTestStore) BeginTrackedOperation(_ context.Context, lease HookLease, _ func() time.Time, binding OperationBinding, equal func(context.Context, []byte, []byte) (bool, error)) (*OperationResult, bool, error) {
	if s.binding.Identity.ExecutionID == "" {
		s.binding = binding
		s.activityID, s.generation = lease.ActivityID, lease.Generation
		s.started = true
		return nil, true, nil
	}
	matches, err := equal(context.Background(), s.binding.Input, binding.Input)
	if err != nil || !matches {
		return nil, false, ErrConflict
	}
	if s.result != nil {
		return s.result, false, nil
	}
	return nil, false, ErrOperationUnresolved
}

func (s *reconciliationTestStore) CompleteTrackedOperation(_ context.Context, lease HookLease, _ func() time.Time, identity OperationIdentity, result OperationResult) error {
	if lease.ActivityID != s.activityID || lease.Generation != s.generation || identity != s.binding.Identity {
		return ErrStaleActivityLease
	}
	s.result = &result
	return nil
}

func (s *reconciliationTestStore) InspectTrackedOperation(_ context.Context, identity OperationIdentity) (OperationInspection, error) {
	if identity != s.binding.Identity {
		return OperationInspection{}, ErrOperationNotFound
	}
	return OperationInspection{Binding: s.binding, ActivityID: s.activityID, Generation: s.generation, MayHaveDispatched: true, Result: s.result}, nil
}

func (s *reconciliationTestStore) ReconcileTrackedOperation(_ context.Context, lease HookLease, clock func() time.Time, binding OperationBinding, assessment OperationAssessment, equal func(context.Context, []byte, []byte) (bool, error)) (OperationReconciliation, error) {
	matches, err := equal(context.Background(), s.binding.Input, binding.Input)
	if err != nil || !matches || binding.Identity != s.binding.Identity {
		return OperationReconciliation{}, ErrConflict
	}
	s.evidence = append(s.evidence, assessment.Evidence)
	if assessment.Outcome == OperationOutcomeSucceeded || assessment.Outcome == OperationOutcomeFailed {
		s.result = assessment.Result
		s.activityID, s.generation = lease.ActivityID, lease.Generation
		return OperationReconciliation{Result: s.result, Resolved: true}, nil
	}
	if OperationMayRepeat(binding, assessment, clock()) && s.activityID != lease.ActivityID {
		s.activityID, s.generation = lease.ActivityID, lease.Generation
		s.started = true
		if s.onRepeat != nil {
			s.onRepeat()
		}
		return OperationReconciliation{Dispatch: true}, nil
	}
	s.activityID, s.generation = lease.ActivityID, lease.Generation
	return OperationReconciliation{}, nil
}

func reconciliationRequest() OperationRequest {
	return OperationRequest{
		Key: "charge", Kind: "charge", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		Input: []byte("amount=100"), ResultCodec: CodecVersion{Name: "receipt", Version: "v1"},
		Preflight:      func(context.Context) error { return nil },
		ValidateInput:  func(context.Context, []byte) error { return nil },
		EqualInput:     func(_ context.Context, a, b []byte) (bool, error) { return bytes.Equal(a, b), nil },
		ValidateResult: func(context.Context, OperationResult) error { return nil },
	}
}

func reconciliationEvidence(identity OperationIdentity, now time.Time) OperationEvidence {
	return OperationEvidence{
		Identity: identity, Outcome: OperationOutcomeSucceeded, Provider: "test-provider", Scope: "merchant-7",
		Reference: "lookup-123", InputFingerprint: "request-fingerprint-v1",
		Version: "lookup-v1", Provenance: "authoritative-lookup",
		BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		ObservedAt: now, ValidUntil: now.Add(time.Minute),
	}
}

func TestUnknownMutationRepeatsOnlyWithCurrentExactGuarantee(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		if dispatches == 1 {
			return OperationResult{}, errors.New("response lost")
		}
		return OperationResult{Codec: req.ResultCodec, Data: []byte("receipt")}, nil
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) {
		t.Fatalf("first outcome was not unknown: %v", err)
	}
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	repeat := SafeRepeatGuarantee{
		Identity: OperationIdentity{ExecutionID: "execution", Key: "charge"},
		Provider: "test-provider", Scope: "merchant-7", Reference: "lookup-123",
		InputFingerprint: "request-fingerprint-v1", Version: "idempotency-v1",
		BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(30 * time.Second), OverlapUntil: now.Add(time.Minute),
	}
	req.Reconcile = func(_ context.Context, identity OperationIdentity, _ OperationBinding, _ OperationInspection) (OperationAssessment, error) {
		evidence := reconciliationEvidence(identity, now)
		evidence.Outcome = OperationOutcomeUnknown
		return OperationAssessment{Outcome: OperationOutcomeUnknown, Evidence: evidence, Repeat: &repeat}, nil
	}
	req.VerifySafeRepeat = func(context.Context, OperationBinding, OperationAssessment, time.Time) error { return nil }
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("too-short guarantee permitted a repeat: dispatches=%d err=%v", dispatches, err)
	}
	repeat.ValidUntil = now.Add(-time.Second)
	lease.ActivityID, lease.Generation = "activity-3", 3
	scope.lease = lease
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("expired provider guarantee permitted a repeat: dispatches=%d err=%v", dispatches, err)
	}
	repeat.ValidUntil = now.Add(2 * time.Minute)
	repeat.InputFingerprint = "different-operation-input"
	lease.ActivityID, lease.Generation = "activity-4", 4
	scope.lease = lease
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("mismatched semantic input guarantee permitted a repeat: dispatches=%d err=%v", dispatches, err)
	}
	repeat.InputFingerprint = "request-fingerprint-v1"
	lease.ActivityID, lease.Generation = "activity-5", 5
	scope.lease = lease
	result, err := TrackOperation(ctx, req, dispatch)
	if err != nil || string(result.Data) != "receipt" || dispatches != 2 {
		t.Fatalf("valid exact-operation guarantee did not permit a bounded repeat: result=%+v dispatches=%d err=%v", result, dispatches, err)
	}
}

func TestRepeatedDispatchFailureDoesNotLoopWithinAttempt(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{}, errors.New("response lost")
	}
	assessmentFor := func(identity OperationIdentity) OperationAssessment {
		evidence := reconciliationEvidence(identity, now)
		evidence.Outcome = OperationOutcomeUnknown
		repeat := SafeRepeatGuarantee{
			Identity: identity, Provider: evidence.Provider, Scope: evidence.Scope, Reference: evidence.Reference,
			InputFingerprint: evidence.InputFingerprint, Version: "idempotency-v1",
			BehaviorVersion: evidence.BehaviorVersion, ComparatorVersion: evidence.ComparatorVersion,
			ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(2 * time.Minute), OverlapUntil: now.Add(time.Minute),
		}
		return OperationAssessment{Outcome: OperationOutcomeUnknown, Evidence: evidence, Repeat: &repeat}
	}
	req.ClassifyDispatch = func(_ context.Context, identity OperationIdentity, _ error) (OperationAssessment, error) {
		return assessmentFor(identity), nil
	}
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	req.VerifySafeRepeat = func(context.Context, OperationBinding, OperationAssessment, time.Time) error { return nil }
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("initial unknown dispatch was not retained: calls=%d err=%v", dispatches, err)
	}

	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	req.Reconcile = func(_ context.Context, identity OperationIdentity, _ OperationBinding, _ OperationInspection) (OperationAssessment, error) {
		return assessmentFor(identity), nil
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 2 {
		t.Fatalf("repeated unknown dispatch looped inside one attempt: calls=%d err=%v", dispatches, err)
	}
}

func TestAuthoritativeAbsenceRequiresQuiescence(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{}, errors.New("response lost")
	}
	_, _ = TrackOperation(ctx, req, dispatch)
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	req.Reconcile = func(_ context.Context, identity OperationIdentity, _ OperationBinding, _ OperationInspection) (OperationAssessment, error) {
		evidence := reconciliationEvidence(identity, now)
		evidence.Outcome = OperationOutcomeAbsent
		evidence.Quiesced = false
		return OperationAssessment{Outcome: OperationOutcomeAbsent, Evidence: evidence}, nil
	}
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("non-quiescent absence permitted dispatch: dispatches=%d err=%v", dispatches, err)
	}
}

func TestTrackedReadRepeatMustPreserveOriginalResult(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	req.Kind = "read"
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{}, errors.New("read response lost")
	}
	_, _ = TrackOperation(ctx, req, dispatch)
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	evidence := reconciliationEvidence(OperationIdentity{ExecutionID: "execution", Key: "charge"}, now)
	evidence.Outcome = OperationOutcomeUnknown
	repeat := SafeRepeatGuarantee{
		Identity: evidence.Identity, Provider: evidence.Provider, Scope: evidence.Scope, Reference: evidence.Reference,
		InputFingerprint: evidence.InputFingerprint,
		Version:          "read-repeat-v1", BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(time.Minute), OverlapUntil: now.Add(30 * time.Second),
	}
	req.Reconcile = func(context.Context, OperationIdentity, OperationBinding, OperationInspection) (OperationAssessment, error) {
		return OperationAssessment{Outcome: OperationOutcomeUnknown, Evidence: evidence, Repeat: &repeat}, nil
	}
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	req.VerifySafeRepeat = func(context.Context, OperationBinding, OperationAssessment, time.Time) error {
		return errors.New("new read could change the original result")
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("changed read result semantics permitted a new lookup: dispatches=%d err=%v", dispatches, err)
	}
}

func TestExpiredEvidenceCannotAuthorizeRepeat(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{}, errors.New("response lost")
	}
	_, _ = TrackOperation(ctx, req, dispatch)
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	evidence := reconciliationEvidence(OperationIdentity{ExecutionID: "execution", Key: "charge"}, now.Add(-2*time.Minute))
	evidence.Outcome = OperationOutcomeAbsent
	evidence.Quiesced = true
	req.Reconcile = func(context.Context, OperationIdentity, OperationBinding, OperationInspection) (OperationAssessment, error) {
		return OperationAssessment{Outcome: OperationOutcomeAbsent, Evidence: evidence}, nil
	}
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrCompatibility) || dispatches != 1 {
		t.Fatalf("expired absence evidence authorized repeat: dispatches=%d err=%v", dispatches, err)
	}
}

func TestRepeatEvidenceIsRecheckedAfterStoreClaim(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		if dispatches == 1 {
			return OperationResult{}, errors.New("response lost")
		}
		return OperationResult{Codec: req.ResultCodec, Data: []byte("receipt")}, nil
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) {
		t.Fatalf("first outcome was not unknown: %v", err)
	}
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	evidence := reconciliationEvidence(OperationIdentity{ExecutionID: "execution", Key: "charge"}, now)
	evidence.Outcome = OperationOutcomeUnknown
	repeat := SafeRepeatGuarantee{
		Identity: evidence.Identity, Provider: evidence.Provider, Scope: evidence.Scope, Reference: evidence.Reference,
		InputFingerprint: evidence.InputFingerprint, Version: "idempotency-v1",
		BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(2 * time.Minute), OverlapUntil: now.Add(time.Minute),
	}
	req.Reconcile = func(context.Context, OperationIdentity, OperationBinding, OperationInspection) (OperationAssessment, error) {
		return OperationAssessment{Outcome: OperationOutcomeUnknown, Evidence: evidence, Repeat: &repeat}, nil
	}
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	req.VerifySafeRepeat = func(context.Context, OperationBinding, OperationAssessment, time.Time) error { return nil }
	store.onRepeat = func() { now = now.Add(time.Minute + time.Second) }

	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) || dispatches != 1 {
		t.Fatalf("repeat proceeded after evidence and guarantee expired during claim: dispatches=%d err=%v", dispatches, err)
	}
}

func TestRepeatUsesProviderEffectHorizonNotHookDeadline(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		if dispatches == 1 {
			return OperationResult{}, errors.New("response lost")
		}
		return OperationResult{Codec: req.ResultCodec, Data: []byte("receipt")}, nil
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) {
		t.Fatalf("first outcome was not unknown: %v", err)
	}
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	evidence := reconciliationEvidence(OperationIdentity{ExecutionID: "execution", Key: "charge"}, now)
	evidence.Outcome = OperationOutcomeUnknown
	repeat := SafeRepeatGuarantee{
		Identity: evidence.Identity, Provider: evidence.Provider, Scope: evidence.Scope, Reference: evidence.Reference,
		InputFingerprint: evidence.InputFingerprint, Version: "idempotency-v1",
		BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(2 * time.Minute), OverlapUntil: now.Add(time.Minute),
	}
	req.Reconcile = func(context.Context, OperationIdentity, OperationBinding, OperationInspection) (OperationAssessment, error) {
		return OperationAssessment{Outcome: OperationOutcomeUnknown, Evidence: evidence, Repeat: &repeat}, nil
	}
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	latestEffect := now.Add(45 * time.Second)
	repeatChecks := 0
	req.VerifySafeRepeat = func(_ context.Context, _ OperationBinding, assessment OperationAssessment, _ time.Time) error {
		repeatChecks++
		if assessment.Repeat == nil || assessment.Repeat.OverlapUntil.Before(latestEffect) {
			return errors.New("provider cannot guarantee idempotency through the latest possible effect")
		}
		return nil
	}

	if _, err := TrackOperation(ctx, req, dispatch); err != nil || dispatches != 2 || repeatChecks != 2 {
		t.Fatalf("verified provider effect horizon was coupled to the hook deadline: dispatches=%d repeatChecks=%d err=%v", dispatches, repeatChecks, err)
	}
}

func TestRepeatEvidenceIsVerifiedImmediatelyBeforeDispatch(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		if dispatches == 1 {
			return OperationResult{}, errors.New("response lost")
		}
		return OperationResult{Codec: req.ResultCodec, Data: []byte("receipt")}, nil
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) {
		t.Fatalf("first outcome was not unknown: %v", err)
	}
	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	evidence := reconciliationEvidence(OperationIdentity{ExecutionID: "execution", Key: "charge"}, now)
	evidence.Outcome = OperationOutcomeUnknown
	repeat := SafeRepeatGuarantee{
		Identity: evidence.Identity, Provider: evidence.Provider, Scope: evidence.Scope, Reference: evidence.Reference,
		InputFingerprint: evidence.InputFingerprint, Version: "idempotency-v1",
		BehaviorVersion: "v1", ComparatorVersion: "bytes-v1",
		ValidFrom: now.Add(-time.Second), ValidUntil: now.Add(2 * time.Minute), OverlapUntil: now.Add(time.Minute),
	}
	req.Reconcile = func(context.Context, OperationIdentity, OperationBinding, OperationInspection) (OperationAssessment, error) {
		return OperationAssessment{Outcome: OperationOutcomeUnknown, Evidence: evidence, Repeat: &repeat}, nil
	}
	evidenceVerifications := 0
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error {
		evidenceVerifications++
		now = now.Add(10 * time.Second)
		return nil
	}
	req.VerifySafeRepeat = func(_ context.Context, _ OperationBinding, _ OperationAssessment, verifiedAt time.Time) error {
		if !verifiedAt.Equal(now) {
			return errors.New("repeat check used a stale verification time")
		}
		return nil
	}

	result, err := TrackOperation(ctx, req, dispatch)
	if err != nil || string(result.Data) != "receipt" || dispatches != 2 || evidenceVerifications != 2 {
		t.Fatalf("repeat did not use freshly verified evidence: result=%+v dispatches=%d verifications=%d err=%v",
			result, dispatches, evidenceVerifications, err)
	}
}

func TestKnownSuccessWithUnavailableResultIsDurablySettled(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	req.ValidateResult = func(context.Context, OperationResult) error { return errors.New("required receipt unavailable") }
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{Codec: req.ResultCodec, Data: []byte("receipt")}, nil
	}

	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrCompatibility) {
		t.Fatalf("unavailable required result was not rejected: %v", err)
	}
	if store.result == nil || !store.result.Unavailable || dispatches != 1 {
		t.Fatalf("confirmed success was not durably marked result-unavailable: result=%+v dispatches=%d", store.result, dispatches)
	}

	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrCompatibility) || dispatches != 1 {
		t.Fatalf("known success with unavailable result permitted a later dispatch: dispatches=%d err=%v", dispatches, err)
	}
}

func TestReconciledSuccessWithoutRequiredResultIsDurablySettled(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{}, errors.New("provider response lost")
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) {
		t.Fatalf("lost response was not retained as unknown: %v", err)
	}

	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	req.VerifyEvidence = func(context.Context, OperationBinding, OperationEvidence) error { return nil }
	req.Reconcile = func(_ context.Context, identity OperationIdentity, _ OperationBinding, _ OperationInspection) (OperationAssessment, error) {
		return OperationAssessment{Outcome: OperationOutcomeSucceeded, Evidence: reconciliationEvidence(identity, now)}, nil
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrCompatibility) {
		t.Fatalf("missing required result was not surfaced: %v", err)
	}
	if store.result == nil || !store.result.Unavailable || dispatches != 1 {
		t.Fatalf("reconciled success was not durably marked result-unavailable: result=%+v dispatches=%d", store.result, dispatches)
	}

	lease.ActivityID, lease.Generation = "activity-3", 3
	scope.lease = lease
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrCompatibility) || dispatches != 1 {
		t.Fatalf("known success with unavailable result permitted redispatch: dispatches=%d err=%v", dispatches, err)
	}
}

func TestTrackedUnknownOperationReconcilesConfirmedSuccess(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := &reconciliationTestStore{}
	identity := OperationIdentity{ExecutionID: "execution", Key: "charge"}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: clock}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	req := reconciliationRequest()
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{}, errors.New("response lost")
	}
	if _, err := TrackOperation(ctx, req, dispatch); !errors.Is(err, ErrOperationUnresolved) {
		t.Fatalf("lost response was not reported as unknown: %v", err)
	}

	lease.ActivityID, lease.Generation = "activity-2", 2
	scope.lease = lease
	req.Reconcile = func(_ context.Context, id OperationIdentity, binding OperationBinding, prior OperationInspection) (OperationAssessment, error) {
		if id != identity || binding.Identity != identity || prior.Result != nil || !prior.MayHaveDispatched {
			t.Fatalf("reconciler received the wrong operation: id=%+v binding=%+v prior=%+v", id, binding, prior)
		}
		return OperationAssessment{
			Outcome: OperationOutcomeSucceeded, Evidence: reconciliationEvidence(id, now),
			Result: &OperationResult{Codec: req.ResultCodec, Data: []byte("provider-receipt")},
		}, nil
	}
	req.VerifyEvidence = func(_ context.Context, binding OperationBinding, evidence OperationEvidence) error {
		if evidence.Identity != binding.Identity || evidence.Scope != "merchant-7" {
			return errors.New("evidence does not bind to the operation")
		}
		return nil
	}
	result, err := TrackOperation(ctx, req, dispatch)
	if err != nil || string(result.Data) != "provider-receipt" || dispatches != 1 {
		t.Fatalf("confirmed success was not replayed without redispatch: result=%+v dispatches=%d err=%v", result, dispatches, err)
	}
	if len(store.evidence) != 1 {
		t.Fatalf("reconciliation evidence was not durably submitted: %+v", store.evidence)
	}
}

func TestCompensationRequiresRecoverHook(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	store := &reconciliationTestStore{}
	lease := HookLease{ExecutionID: "execution", ActivityID: "activity-1", Generation: 1}
	scope := &operationScope{ctx: context.Background(), store: store, lease: lease, clock: func() time.Time { return now }, kind: HookRetry}
	ctx := context.WithValue(context.Background(), operationTrackerKey{}, scope)
	request := reconciliationRequest()
	request.Key = "refund"
	request.Compensation = true
	request.Prerequisites = []string{}
	dispatches := 0
	dispatch := func(context.Context, OperationIdentity) (OperationResult, error) {
		dispatches++
		return OperationResult{Codec: request.ResultCodec, Data: []byte("receipt")}, nil
	}
	if _, err := TrackOperation(ctx, request, dispatch); !errors.Is(err, ErrInvalidRequest) || dispatches != 0 {
		t.Fatalf("ordinary retry dispatched a compensation: calls=%d err=%v", dispatches, err)
	}
	scope.kind = HookRecover
	if _, err := TrackOperation(ctx, request, dispatch); err != nil {
		t.Fatalf("Recover could not dispatch tracked compensation: %v", err)
	}
	if dispatches != 1 || !store.binding.Compensation || store.binding.Prerequisites == nil ||
		store.binding.Identity.Key != "refund" {
		t.Fatalf("Recover did not durably bind distinct compensation metadata: calls=%d binding=%+v", dispatches, store.binding)
	}
}
