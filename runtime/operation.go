package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"
)

var (
	// ErrOperationTrackingUnavailable means no active runtime hook supplied tracking.
	ErrOperationTrackingUnavailable = errors.New("tracked operation capability is unavailable")
	// ErrOperationUnresolved means a dispatch may have occurred without a confirmed result.
	ErrOperationUnresolved = errors.New("tracked operation outcome is unresolved")
	// ErrOperationFailed reports a durably recorded definitive failure.
	ErrOperationFailed = errors.New("tracked operation failed definitively")
	// ErrOperationNotFound means no operation has been recorded for the identity.
	ErrOperationNotFound = errors.New("tracked operation not found")
	// ErrOperationEvidenceConflict means provider evidence conflicts with a prior resolution.
	ErrOperationEvidenceConflict = errors.New("tracked operation evidence conflicts with prior resolution")
	// ErrOperationPrerequisiteUnresolved means a compensation prerequisite is absent or unsettled.
	ErrOperationPrerequisiteUnresolved = errors.New("tracked operation prerequisite is unresolved")
	// ErrOperationPrerequisiteCycle means compensation prerequisites contain a cycle.
	ErrOperationPrerequisiteCycle = errors.New("tracked operation prerequisites contain a cycle")
)

// OperationIdentity is scoped to one transition execution, not a business entity.
type OperationIdentity struct {
	ExecutionID string
	Key         string
}

// OperationRequest identifies and binds a host-selected downstream operation.
// Input and results must contain only host-approved safe data. Preflight must
// verify the exact behavior, comparator and result codec before dispatch.
// Provider is a stable non-secret provider identity. The host must also bind
// any account or routing scope in the input. Operations without a provider
// remain usable but cannot be migrated because their origin cannot be proved.
// EqualInput must be side-effect-free and must not re-enter the store.
// ValidateResult must verify typed data or an immutable reference on each replay.
// VerifySafeRepeat must authenticate the provider guarantee and prove that its
// OverlapUntil bounds the latest possible effect of both old and repeat calls;
// this horizon is independent of the hook deadline.
type OperationRequest struct {
	Key               string
	Kind              string
	Provider          string
	BehaviorVersion   string
	ComparatorVersion string
	// Compensation marks a host-defined Recover operation that has dependencies.
	Compensation bool
	// Prerequisites are same-execution operation keys. Nil is an incomplete
	// declaration; an explicit empty slice declares independence.
	Prerequisites    []string
	Input            []byte
	ResultCodec      CodecVersion
	Preflight        func(context.Context) error
	ValidateInput    func(context.Context, []byte) error
	EqualInput       func(context.Context, []byte, []byte) (bool, error)
	ValidateResult   func(context.Context, OperationResult) error
	ClassifyDispatch func(context.Context, OperationIdentity, error) (OperationAssessment, error)
	Reconcile        func(context.Context, OperationIdentity, OperationBinding, OperationInspection) (OperationAssessment, error)
	VerifyEvidence   func(context.Context, OperationBinding, OperationEvidence) error
	VerifySafeRepeat func(context.Context, OperationBinding, OperationAssessment, time.Time) error
}

// OperationResult is the safe result retained for replay. Typed success has
// non-nil Data, including for an empty encoding. NoResult is explicit and has
// nil Data. The runtime sets Unavailable when confirmed success lacks a valid
// required result; progression remains blocked until its result is recovered.
// The known effect cannot be redispatched. A
// nonempty safe FailureCode represents a definitive failure; raw errors must
// not be put in this field.
type OperationResult struct {
	NoResult    bool
	Unavailable bool
	Codec       CodecVersion
	Data        []byte
	FailureCode string
}

// OperationBinding contains only host-approved durable data.
type OperationBinding struct {
	Identity          OperationIdentity
	Kind              string
	Provider          string
	BehaviorVersion   string
	ComparatorVersion string
	Compensation      bool
	Prerequisites     []string
	Input             []byte
	ResultCodec       CodecVersion
}

type operationBindingWire struct {
	ExecutionID, Key, Kind, Provider, BehaviorVersion, ComparatorVersion []byte
	Compensation                                                         bool
	Prerequisites                                                        []string
	Input, CodecName, CodecVersion                                       []byte
}

func (b OperationBinding) MarshalJSON() ([]byte, error) {
	return json.Marshal(operationBindingWire{
		ExecutionID: []byte(b.Identity.ExecutionID), Key: []byte(b.Identity.Key),
		Kind: []byte(b.Kind), Provider: []byte(b.Provider), BehaviorVersion: []byte(b.BehaviorVersion), ComparatorVersion: []byte(b.ComparatorVersion),
		Compensation: b.Compensation, Prerequisites: b.Prerequisites,
		Input: b.Input, CodecName: []byte(b.ResultCodec.Name), CodecVersion: []byte(b.ResultCodec.Version),
	})
}

func (b *OperationBinding) UnmarshalJSON(data []byte) error {
	var wire operationBindingWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*b = OperationBinding{
		Identity: OperationIdentity{ExecutionID: string(wire.ExecutionID), Key: string(wire.Key)},
		Kind:     string(wire.Kind), Provider: string(wire.Provider), BehaviorVersion: string(wire.BehaviorVersion), ComparatorVersion: string(wire.ComparatorVersion),
		Compensation: wire.Compensation, Prerequisites: wire.Prerequisites,
		Input: wire.Input, ResultCodec: CodecVersion{Name: string(wire.CodecName), Version: string(wire.CodecVersion)},
	}
	return nil
}

type operationResultWire struct {
	NoResult                      bool
	Unavailable                   bool
	CodecName, CodecVersion, Data []byte
	FailureCode                   []byte
}

func (r OperationResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(operationResultWire{
		NoResult: r.NoResult, Unavailable: r.Unavailable,
		CodecName: []byte(r.Codec.Name), CodecVersion: []byte(r.Codec.Version), Data: r.Data, FailureCode: []byte(r.FailureCode),
	})
}

func (r *OperationResult) UnmarshalJSON(data []byte) error {
	var wire operationResultWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*r = OperationResult{
		NoResult: wire.NoResult, Unavailable: wire.Unavailable,
		Codec: CodecVersion{Name: string(wire.CodecName), Version: string(wire.CodecVersion)}, Data: wire.Data, FailureCode: string(wire.FailureCode),
	}
	return nil
}

// ValidOperationResult distinguishes explicit no-result, typed data, and failure.
func ValidOperationResult(result OperationResult, codec CodecVersion) bool {
	if result.Codec != codec {
		return false
	}
	if result.Unavailable {
		return !result.NoResult && result.Data == nil && result.FailureCode == ""
	}
	if result.FailureCode != "" {
		return !result.NoResult && result.Data == nil
	}
	if result.NoResult {
		return result.Data == nil
	}
	return result.Data != nil
}

func sameOperationResult(a, b *OperationResult) bool {
	return a != nil && b != nil && a.Codec == b.Codec && a.NoResult == b.NoResult &&
		a.Unavailable == b.Unavailable && a.FailureCode == b.FailureCode &&
		(a.Data == nil) == (b.Data == nil) && slices.Equal(a.Data, b.Data)
}

// OperationStore persists starts and outcomes under the current hook fence.
// Begin atomically records possible dispatch or returns an existing outcome,
// comparing full identities and semantic input. An uncertain prior call blocks.
// Complete must fence the started claim and durably retain the safe outcome.
type OperationStore interface {
	BeginTrackedOperation(context.Context, HookLease, func() time.Time, OperationBinding, func(context.Context, []byte, []byte) (bool, error)) (*OperationResult, bool, error)
	CompleteTrackedOperation(context.Context, HookLease, func() time.Time, OperationIdentity, OperationResult) error
}

// OperationOutcome is a host-classified provider outcome. Unknown and absence
// are distinct: absence is useful only with evidence that old calls are quiescent.
type OperationOutcome string

const (
	OperationOutcomeUnknown   OperationOutcome = "unknown"
	OperationOutcomeSucceeded OperationOutcome = "succeeded"
	OperationOutcomeFailed    OperationOutcome = "failed"
	OperationOutcomeAbsent    OperationOutcome = "absent"
)

// OperationEvidence contains safe, provider-specific evidence bound to one
// tracked operation. Do not place credentials, secrets, or raw provider errors
// in these fields.
type OperationEvidence struct {
	Identity          OperationIdentity
	Outcome           OperationOutcome
	Provider          string
	Scope             string
	Reference         string
	InputFingerprint  string
	Version           string
	Provenance        string
	BehaviorVersion   string
	ComparatorVersion string
	ObservedAt        time.Time
	ValidUntil        time.Time
	Quiesced          bool
}

// SafeRepeatGuarantee records the provider's idempotency window for this exact
// operation. OverlapUntil is the provider-verified latest instant that either
// the original or repeated dispatch could still take effect; it is not the
// hook deadline. ValidUntil must extend through this effect horizon.
type SafeRepeatGuarantee struct {
	Identity          OperationIdentity
	Provider          string
	Scope             string
	Reference         string
	InputFingerprint  string
	Version           string
	BehaviorVersion   string
	ComparatorVersion string
	ValidFrom         time.Time
	ValidUntil        time.Time
	OverlapUntil      time.Time
}

// OperationAssessment is the host's provider-specific adjudication of an
// unknown or definitive failure. A repeat is authorized only by quiescent
// absence or a verified, still-valid SafeRepeatGuarantee.
type OperationAssessment struct {
	Outcome  OperationOutcome
	Evidence OperationEvidence
	Result   *OperationResult
	Repeat   *SafeRepeatGuarantee
}

// OperationHistoryEntry retains safe classifications and intervention audit
// records without replacing prior evidence or outcomes.
type OperationHistoryEntry struct {
	Assessment OperationAssessment
	RequestID  string
	Principal  string
	Reason     string
	RecordedAt time.Time
	// Conflicting marks a resolution request whose outcome was rejected.
	Conflicting bool
}

// OperationInspection exposes only the host-approved persisted binding and outcome.
type OperationInspection struct {
	Binding             OperationBinding
	ActivityID          string
	Generation          uint64
	MayHaveDispatched   bool
	DispatchPending     bool
	PrerequisiteBlocked bool
	Result              *OperationResult
	History             []OperationHistoryEntry
	EvidenceConflict    bool
}

// OperationInspector is a host-authorized read-only inspection seam.
type OperationInspector interface {
	InspectTrackedOperation(context.Context, OperationIdentity) (OperationInspection, error)
}

// OperationReconciliation persists an assessment under the active hook fence.
type OperationReconciliation struct {
	Result   *OperationResult
	Dispatch bool
	Resolved bool
}

// OperationReconciliationStore atomically fences stale operation reports,
// persists evidence, and authorizes only safe repeats.
type OperationReconciliationStore interface {
	ReconcileTrackedOperation(context.Context, HookLease, func() time.Time, OperationBinding, OperationAssessment, func(context.Context, []byte, []byte) (bool, error)) (OperationReconciliation, error)
}

type operationScope struct {
	ctx   context.Context
	store OperationStore
	lease HookLease
	clock func() time.Time
	kind  HookKind
}

func (s *operationScope) track(ctx context.Context, req OperationRequest, dispatch func(context.Context, OperationIdentity) (OperationResult, error)) (OperationResult, error) {
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return OperationResult{}, ErrStaleActivityLease
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	if req.Key == "" || req.Kind == "" || req.BehaviorVersion == "" || req.ComparatorVersion == "" ||
		req.ResultCodec.Name == "" || req.ResultCodec.Version == "" || req.Preflight == nil || req.ValidateInput == nil ||
		req.EqualInput == nil || req.ValidateResult == nil || dispatch == nil {
		return OperationResult{}, ErrInvalidRequest
	}
	if req.Compensation && s.kind != HookRecover {
		return OperationResult{}, ErrInvalidRequest
	}
	input := slices.Clone(req.Input)
	if err := req.Preflight(ctx); err != nil {
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, ErrStaleActivityLease
		}
		return OperationResult{}, fmt.Errorf("%w: operation behavior or codec is unavailable", ErrCompatibility)
	}
	if err := req.ValidateInput(ctx, slices.Clone(input)); err != nil {
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, ErrStaleActivityLease
		}
		return OperationResult{}, fmt.Errorf("%w: operation input is unavailable", ErrCompatibility)
	}
	binding := OperationBinding{
		Identity: OperationIdentity{ExecutionID: s.lease.ExecutionID, Key: req.Key},
		Kind:     req.Kind, Provider: req.Provider, BehaviorVersion: req.BehaviorVersion, ComparatorVersion: req.ComparatorVersion,
		Compensation: req.Compensation, Prerequisites: slices.Clone(req.Prerequisites),
		Input: input, ResultCodec: req.ResultCodec,
	}
	result, started, err := s.store.BeginTrackedOperation(ctx, s.lease, s.clock, binding, req.EqualInput)
	if err != nil {
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, errors.Join(ErrStaleActivityLease, err)
		}
		if errors.Is(err, ErrOperationUnresolved) && req.Reconcile != nil {
			return s.reconcile(ctx, req, binding, dispatch)
		}
		return OperationResult{}, err
	}
	if !started && result != nil && result.FailureCode != "" && req.Reconcile != nil {
		return s.reconcile(ctx, req, binding, dispatch)
	}
	if !started && result != nil && result.Unavailable {
		if req.Reconcile != nil {
			return s.reconcile(ctx, req, binding, dispatch)
		}
		return OperationResult{}, fmt.Errorf("%w: confirmed operation result is unavailable", ErrCompatibility)
	}
	if started {
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
		}
		resultValue, dispatchErr := dispatch(ctx, binding.Identity)
		if dispatchErr != nil {
			if s.ctx.Err() != nil || ctx.Err() != nil {
				return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
			}
			if req.ClassifyDispatch != nil {
				assessment, classifyErr := req.ClassifyDispatch(ctx, binding.Identity, dispatchErr)
				if classifyErr == nil {
					return s.applyAssessment(ctx, req, binding, assessment, dispatch)
				}
			}
			return OperationResult{}, ErrOperationUnresolved
		}
		result = &resultValue
	}
	if result == nil || !ValidOperationResult(*result, req.ResultCodec) {
		if started {
			return OperationResult{}, s.recordUnavailableSuccess(ctx, binding)
		}
		return OperationResult{}, ErrCompatibility
	}
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return OperationResult{}, ErrStaleActivityLease
	}
	if !started {
		if inspector, ok := s.store.(OperationInspector); ok {
			inspection, inspectErr := inspector.InspectTrackedOperation(ctx, binding.Identity)
			if inspectErr != nil {
				return OperationResult{}, inspectErr
			}
			if inspection.EvidenceConflict {
				return OperationResult{}, ErrOperationEvidenceConflict
			}
		}
	}
	result.Data = slices.Clone(result.Data)
	if result.FailureCode == "" {
		toValidate := *result
		toValidate.Data = slices.Clone(result.Data)
		if err := req.ValidateResult(ctx, toValidate); err != nil {
			if started {
				return OperationResult{}, s.recordUnavailableSuccess(ctx, binding)
			}
			if s.ctx.Err() != nil || ctx.Err() != nil {
				return OperationResult{}, ErrStaleActivityLease
			}
			return OperationResult{}, fmt.Errorf("%w: operation result is unavailable", ErrCompatibility)
		}
	}
	if started {
		if err := s.store.CompleteTrackedOperation(ctx, s.lease, s.clock, binding.Identity, *result); err != nil {
			if s.ctx.Err() != nil || ctx.Err() != nil {
				return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease, err)
			}
			recorded, newDispatch, readErr := s.store.BeginTrackedOperation(ctx, s.lease, s.clock, binding, req.EqualInput)
			if readErr != nil || newDispatch || !sameOperationResult(recorded, result) {
				return OperationResult{}, errors.Join(ErrOperationUnresolved, err, readErr)
			}
			result = recorded
		}
	} else {
		recorded, newDispatch, err := s.store.BeginTrackedOperation(ctx, s.lease, s.clock, binding, req.EqualInput)
		if err != nil {
			return OperationResult{}, err
		}
		if newDispatch || !sameOperationResult(recorded, result) {
			return OperationResult{}, ErrOperationUnresolved
		}
		result = recorded
	}
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return OperationResult{}, ErrStaleActivityLease
	}
	if result.Unavailable {
		return OperationResult{}, fmt.Errorf("%w: confirmed operation result is unavailable", ErrCompatibility)
	}
	result.Data = slices.Clone(result.Data)
	if result.FailureCode != "" {
		return *result, ErrOperationFailed
	}
	return *result, nil
}

func (s *operationScope) recordUnavailableSuccess(ctx context.Context, binding OperationBinding) error {
	result := OperationResult{Codec: binding.ResultCodec, Unavailable: true}
	if err := s.store.CompleteTrackedOperation(
		context.WithoutCancel(ctx), s.lease, s.clock, binding.Identity, result,
	); err != nil {
		return errors.Join(ErrOperationUnresolved, err)
	}
	return fmt.Errorf("%w: confirmed operation result is unavailable", ErrCompatibility)
}

func (s *operationScope) reconcile(
	ctx context.Context,
	req OperationRequest,
	binding OperationBinding,
	dispatch func(context.Context, OperationIdentity) (OperationResult, error),
) (OperationResult, error) {
	inspector, inspectable := s.store.(OperationInspector)
	_, reconcilable := s.store.(OperationReconciliationStore)
	if !inspectable || !reconcilable || req.VerifyEvidence == nil {
		return OperationResult{}, ErrOperationUnresolved
	}
	prior, err := inspector.InspectTrackedOperation(ctx, binding.Identity)
	if err != nil {
		return OperationResult{}, err
	}
	if prior.EvidenceConflict {
		return OperationResult{}, ErrOperationEvidenceConflict
	}
	assessment, err := req.Reconcile(ctx, binding.Identity, binding, prior)
	if err != nil {
		return OperationResult{}, ErrOperationUnresolved
	}
	return s.applyAssessment(ctx, req, binding, assessment, dispatch)
}

func (s *operationScope) applyAssessment(
	ctx context.Context,
	req OperationRequest,
	binding OperationBinding,
	assessment OperationAssessment,
	dispatch func(context.Context, OperationIdentity) (OperationResult, error),
) (OperationResult, error) {
	return s.applyAssessmentWithDispatch(ctx, req, binding, assessment, dispatch, true)
}

func (s *operationScope) applyAssessmentWithDispatch(
	ctx context.Context,
	req OperationRequest,
	binding OperationBinding,
	assessment OperationAssessment,
	dispatch func(context.Context, OperationIdentity) (OperationResult, error),
	allowDispatch bool,
) (OperationResult, error) {
	now := s.clock()
	missingSuccessResult := assessment.Outcome == OperationOutcomeSucceeded &&
		(assessment.Result == nil || !ValidOperationResult(*assessment.Result, binding.ResultCodec) ||
			assessment.Result.Unavailable || assessment.Result.FailureCode != "")
	if missingSuccessResult && !validOperationEvidence(binding, assessment, now) {
		return OperationResult{}, ErrCompatibility
	}
	if !missingSuccessResult && !validOperationAssessment(binding, assessment, now) {
		return OperationResult{}, ErrCompatibility
	}
	if req.VerifyEvidence == nil {
		return OperationResult{}, ErrCompatibility
	}
	if err := req.VerifyEvidence(ctx, binding, assessment.Evidence); err != nil {
		return OperationResult{}, fmt.Errorf("%w: operation evidence is unverifiable", ErrCompatibility)
	}
	now = s.clock()
	if missingSuccessResult {
		if !validOperationEvidence(binding, assessment, now) {
			return OperationResult{}, ErrOperationUnresolved
		}
	} else if !validOperationAssessment(binding, assessment, now) {
		return OperationResult{}, ErrOperationUnresolved
	}
	if assessment.Outcome == OperationOutcomeSucceeded && !missingSuccessResult {
		validated := *assessment.Result
		validated.Data = slices.Clone(assessment.Result.Data)
		if err := req.ValidateResult(ctx, validated); err != nil {
			missingSuccessResult = true
		}
	}
	if missingSuccessResult {
		assessment.Result = &OperationResult{Codec: binding.ResultCodec, Unavailable: true}
	}
	if assessment.Repeat != nil {
		if req.VerifySafeRepeat == nil ||
			!validSafeRepeat(binding, assessment.Evidence, *assessment.Repeat, now) ||
			req.VerifySafeRepeat(ctx, binding, assessment, now) != nil {
			return OperationResult{}, ErrOperationUnresolved
		}
		now = s.clock()
		if !validOperationAssessment(binding, assessment, now) ||
			!validSafeRepeat(binding, assessment.Evidence, *assessment.Repeat, now) {
			return OperationResult{}, ErrOperationUnresolved
		}
	}
	store, ok := s.store.(OperationReconciliationStore)
	if !ok {
		return OperationResult{}, ErrOperationUnresolved
	}
	reconcileContext := ctx
	if missingSuccessResult {
		reconcileContext = context.WithoutCancel(ctx)
	}
	reconciliation, err := store.ReconcileTrackedOperation(reconcileContext, s.lease, s.clock, binding, assessment, req.EqualInput)
	if err != nil {
		return OperationResult{}, err
	}
	if reconciliation.Dispatch {
		if !allowDispatch {
			return OperationResult{}, ErrOperationUnresolved
		}
		now = s.clock()
		if !validOperationAssessment(binding, assessment, now) {
			return OperationResult{}, ErrOperationUnresolved
		}
		if err := req.VerifyEvidence(ctx, binding, assessment.Evidence); err != nil {
			return OperationResult{}, ErrOperationUnresolved
		}
		now = s.clock()
		if !validOperationAssessment(binding, assessment, now) {
			return OperationResult{}, ErrOperationUnresolved
		}
		if assessment.Repeat != nil {
			if req.VerifySafeRepeat == nil ||
				!validSafeRepeat(binding, assessment.Evidence, *assessment.Repeat, now) ||
				req.VerifySafeRepeat(ctx, binding, assessment, now) != nil {
				return OperationResult{}, ErrOperationUnresolved
			}
			now = s.clock()
			if !validOperationAssessment(binding, assessment, now) ||
				!validSafeRepeat(binding, assessment.Evidence, *assessment.Repeat, now) {
				return OperationResult{}, ErrOperationUnresolved
			}
		}
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
		}
		result, dispatchErr := dispatch(ctx, binding.Identity)
		if dispatchErr != nil {
			if s.ctx.Err() != nil || ctx.Err() != nil {
				return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
			}
			if req.ClassifyDispatch != nil {
				nextAssessment, classifyErr := req.ClassifyDispatch(ctx, binding.Identity, dispatchErr)
				if classifyErr == nil {
					return s.applyAssessmentWithDispatch(ctx, req, binding, nextAssessment, dispatch, false)
				}
			}
			return OperationResult{}, ErrOperationUnresolved
		}
		if !ValidOperationResult(result, req.ResultCodec) {
			return OperationResult{}, s.recordUnavailableSuccess(ctx, binding)
		}
		if result.FailureCode == "" {
			validated := result
			validated.Data = slices.Clone(result.Data)
			if err := req.ValidateResult(ctx, validated); err != nil {
				return OperationResult{}, s.recordUnavailableSuccess(ctx, binding)
			}
		}
		if err := s.store.CompleteTrackedOperation(ctx, s.lease, s.clock, binding.Identity, result); err != nil {
			return OperationResult{}, errors.Join(ErrOperationUnresolved, err)
		}
		result.Data = slices.Clone(result.Data)
		if result.FailureCode != "" {
			return result, ErrOperationFailed
		}
		return result, nil
	}

	if !reconciliation.Resolved || reconciliation.Result == nil {
		return OperationResult{}, ErrOperationUnresolved
	}
	result := *reconciliation.Result
	if !ValidOperationResult(result, req.ResultCodec) {
		return OperationResult{}, ErrCompatibility
	}
	if result.Unavailable {
		return OperationResult{}, fmt.Errorf("%w: confirmed operation result is unavailable", ErrCompatibility)
	}
	if result.FailureCode != "" {
		return result, ErrOperationFailed
	}
	validated := result
	validated.Data = slices.Clone(result.Data)
	if err := req.ValidateResult(ctx, validated); err != nil {
		return OperationResult{}, fmt.Errorf("%w: operation result is unavailable", ErrCompatibility)
	}
	result.Data = slices.Clone(result.Data)
	return result, nil
}

func validOperationEvidence(binding OperationBinding, assessment OperationAssessment, now time.Time) bool {
	evidence := assessment.Evidence
	if evidence.Identity != binding.Identity || evidence.Outcome != assessment.Outcome ||
		(binding.Provider != "" && evidence.Provider != binding.Provider) ||
		evidence.Provider == "" || evidence.Scope == "" || evidence.Reference == "" ||
		evidence.InputFingerprint == "" || evidence.Version == "" || evidence.Provenance == "" ||
		evidence.BehaviorVersion != binding.BehaviorVersion ||
		evidence.ComparatorVersion != binding.ComparatorVersion ||
		evidence.ObservedAt.IsZero() || evidence.ValidUntil.IsZero() || now.IsZero() ||
		now.Before(evidence.ObservedAt) || !now.Before(evidence.ValidUntil) {
		return false
	}
	if evidence.ValidUntil.Before(evidence.ObservedAt) {
		return false
	}
	return true
}

func validOperationAssessment(binding OperationBinding, assessment OperationAssessment, now time.Time) bool {
	if !validOperationEvidence(binding, assessment, now) {
		return false
	}
	switch assessment.Outcome {
	case OperationOutcomeSucceeded:
		return assessment.Repeat == nil && assessment.Result != nil && ValidOperationResult(*assessment.Result, binding.ResultCodec) &&
			!assessment.Result.Unavailable && assessment.Result.FailureCode == ""
	case OperationOutcomeFailed:
		return assessment.Result != nil && ValidOperationResult(*assessment.Result, binding.ResultCodec) &&
			!assessment.Result.Unavailable && assessment.Result.FailureCode != ""
	case OperationOutcomeAbsent, OperationOutcomeUnknown:
		return assessment.Result == nil
	default:
		return false
	}
}

// OperationReconciliationAssessmentValid additionally permits a confirmed
// success to carry the runtime's unavailable-result marker.
func OperationReconciliationAssessmentValid(binding OperationBinding, assessment OperationAssessment, now time.Time) bool {
	if !validOperationEvidence(binding, assessment, now) {
		return false
	}
	if assessment.Outcome == OperationOutcomeSucceeded &&
		assessment.Result != nil && assessment.Result.Unavailable {
		return assessment.Repeat == nil && ValidOperationResult(*assessment.Result, binding.ResultCodec)
	}
	return validOperationAssessment(binding, assessment, now)
}

// OperationAssessmentValid checks the structural binding and validity interval
// of host-supplied evidence. Provider authenticity remains host-verified.
func OperationAssessmentValid(binding OperationBinding, assessment OperationAssessment, now time.Time) bool {
	return validOperationAssessment(binding, assessment, now)
}

// OperationRepeatValid checks that a guarantee is for the exact operation and
// spans the declared overlap between prior and repeated dispatches.
func OperationRepeatValid(binding OperationBinding, evidence OperationEvidence, repeat SafeRepeatGuarantee, now time.Time) bool {
	return validSafeRepeat(binding, evidence, repeat, now)
}

// OperationMayRepeat reports whether the assessment permits another dispatch.
func OperationMayRepeat(binding OperationBinding, assessment OperationAssessment, now time.Time) bool {
	return assessment.Outcome == OperationOutcomeAbsent && assessment.Evidence.Quiesced ||
		(assessment.Outcome == OperationOutcomeUnknown || assessment.Outcome == OperationOutcomeFailed) &&
			assessment.Repeat != nil && OperationRepeatValid(binding, assessment.Evidence, *assessment.Repeat, now)
}

func validSafeRepeat(binding OperationBinding, evidence OperationEvidence, repeat SafeRepeatGuarantee, now time.Time) bool {
	return repeat.Identity == binding.Identity &&
		repeat.Provider == evidence.Provider && repeat.Scope == evidence.Scope && repeat.Reference == evidence.Reference &&
		repeat.InputFingerprint == evidence.InputFingerprint &&
		repeat.Version != "" && repeat.BehaviorVersion == binding.BehaviorVersion &&
		repeat.ComparatorVersion == binding.ComparatorVersion && !repeat.ValidFrom.IsZero() &&
		!repeat.ValidUntil.IsZero() && !repeat.OverlapUntil.IsZero() &&
		!now.Before(repeat.ValidFrom) && now.Before(repeat.ValidUntil) &&
		!now.After(repeat.OverlapUntil) && !repeat.ValidUntil.Before(repeat.OverlapUntil)
}

type operationTracker interface {
	track(context.Context, OperationRequest, func(context.Context, OperationIdentity) (OperationResult, error)) (OperationResult, error)
}

type operationTrackerKey struct{}

// TrackOperation runs within the hook attempt budget and never invokes dispatch
// when a runtime capability is absent. The host maps the supplied identity to
// a stable provider-compatible idempotency identity without collisions.
func TrackOperation(ctx context.Context, request OperationRequest, dispatch func(context.Context, OperationIdentity) (OperationResult, error)) (OperationResult, error) {
	tracker, ok := ctx.Value(operationTrackerKey{}).(operationTracker)
	if !ok {
		return OperationResult{}, ErrOperationTrackingUnavailable
	}
	return tracker.track(ctx, request, dispatch)
}

// NormalizeOperationBinding clones and sorts a binding's prerequisite keys.
func NormalizeOperationBinding(binding OperationBinding) (OperationBinding, error) {
	if err := ValidateDurableID(binding.Identity.ExecutionID); err != nil {
		return OperationBinding{}, err
	}
	binding.Input = slices.Clone(binding.Input)
	binding.Prerequisites = slices.Clone(binding.Prerequisites)
	for _, key := range binding.Prerequisites {
		if key == "" {
			return OperationBinding{}, ErrInvalidRequest
		}
	}
	sort.Strings(binding.Prerequisites)
	for i := 1; i < len(binding.Prerequisites); i++ {
		if binding.Prerequisites[i] == binding.Prerequisites[i-1] {
			return OperationBinding{}, ErrInvalidRequest
		}
	}
	return binding, nil
}
