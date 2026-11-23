package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
)

// OperationIdentity is scoped to one transition execution, not a business entity.
type OperationIdentity struct {
	ExecutionID string
	Key         string
}

// OperationRequest identifies and binds a host-selected downstream operation.
// Input and results must contain only host-approved safe data. Preflight must
// verify the exact behavior, comparator and result codec before dispatch.
// EqualInput must be side-effect-free and must not re-enter the store.
// ValidateResult must verify typed data or an immutable reference on each replay.
type OperationRequest struct {
	Key               string
	Kind              string
	BehaviorVersion   string
	ComparatorVersion string
	Input             []byte
	ResultCodec       CodecVersion
	Preflight         func(context.Context) error
	ValidateInput     func(context.Context, []byte) error
	EqualInput        func(context.Context, []byte, []byte) (bool, error)
	ValidateResult    func(context.Context, OperationResult) error
}

// OperationResult is the safe result retained for replay. Typed success has
// non-nil Data, including for an empty encoding. NoResult is explicit and has
// nil Data. A nonempty safe FailureCode represents a definitive failure; raw
// errors must not be put in this field. Unknown outcomes are returned as errors.
type OperationResult struct {
	NoResult    bool
	Codec       CodecVersion
	Data        []byte
	FailureCode string
}

// OperationBinding contains only host-approved durable data.
type OperationBinding struct {
	Identity          OperationIdentity
	Kind              string
	BehaviorVersion   string
	ComparatorVersion string
	Input             []byte
	ResultCodec       CodecVersion
}

type operationBindingWire struct {
	ExecutionID, Key, Kind, BehaviorVersion, ComparatorVersion []byte
	Input, CodecName, CodecVersion                             []byte
}

func (b OperationBinding) MarshalJSON() ([]byte, error) {
	return json.Marshal(operationBindingWire{
		ExecutionID: []byte(b.Identity.ExecutionID), Key: []byte(b.Identity.Key),
		Kind: []byte(b.Kind), BehaviorVersion: []byte(b.BehaviorVersion), ComparatorVersion: []byte(b.ComparatorVersion),
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
		Kind:     string(wire.Kind), BehaviorVersion: string(wire.BehaviorVersion), ComparatorVersion: string(wire.ComparatorVersion),
		Input: wire.Input, ResultCodec: CodecVersion{Name: string(wire.CodecName), Version: string(wire.CodecVersion)},
	}
	return nil
}

type operationResultWire struct {
	NoResult                      bool
	CodecName, CodecVersion, Data []byte
	FailureCode                   []byte
}

func (r OperationResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(operationResultWire{NoResult: r.NoResult, CodecName: []byte(r.Codec.Name), CodecVersion: []byte(r.Codec.Version), Data: r.Data, FailureCode: []byte(r.FailureCode)})
}

func (r *OperationResult) UnmarshalJSON(data []byte) error {
	var wire operationResultWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*r = OperationResult{NoResult: wire.NoResult, Codec: CodecVersion{Name: string(wire.CodecName), Version: string(wire.CodecVersion)}, Data: wire.Data, FailureCode: string(wire.FailureCode)}
	return nil
}

// ValidOperationResult distinguishes explicit no-result, typed data, and failure.
func ValidOperationResult(result OperationResult, codec CodecVersion) bool {
	if result.Codec != codec {
		return false
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
		a.FailureCode == b.FailureCode && (a.Data == nil) == (b.Data == nil) && slices.Equal(a.Data, b.Data)
}

// OperationStore persists starts and outcomes under the current hook fence.
// Begin atomically records possible dispatch or returns an existing outcome,
// comparing full identities and semantic input. An uncertain prior call blocks.
// Complete must fence the started claim and durably retain the safe outcome.
type OperationStore interface {
	BeginTrackedOperation(context.Context, HookLease, func() time.Time, OperationBinding, func(context.Context, []byte, []byte) (bool, error)) (*OperationResult, bool, error)
	CompleteTrackedOperation(context.Context, HookLease, func() time.Time, OperationIdentity, OperationResult) error
}

// OperationInspection exposes only the host-approved persisted binding and outcome.
type OperationInspection struct {
	Binding           OperationBinding
	ActivityID        string
	Generation        uint64
	MayHaveDispatched bool
	Result            *OperationResult
}

// OperationInspector is a host-authorized read-only inspection seam.
type OperationInspector interface {
	InspectTrackedOperation(context.Context, OperationIdentity) (OperationInspection, error)
}

type operationScope struct {
	ctx   context.Context
	store OperationStore
	lease HookLease
	clock func() time.Time
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
		Kind:     req.Kind, BehaviorVersion: req.BehaviorVersion, ComparatorVersion: req.ComparatorVersion,
		Input: input, ResultCodec: req.ResultCodec,
	}
	result, started, err := s.store.BeginTrackedOperation(ctx, s.lease, s.clock, binding, req.EqualInput)
	if err != nil {
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, errors.Join(ErrStaleActivityLease, err)
		}
		return OperationResult{}, err
	}
	if started {
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
		}
		resultValue, dispatchErr := dispatch(ctx, binding.Identity)
		if s.ctx.Err() != nil || ctx.Err() != nil {
			return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
		}
		if dispatchErr != nil {
			return OperationResult{}, ErrOperationUnresolved
		}
		result = &resultValue
	}
	if result == nil || !ValidOperationResult(*result, req.ResultCodec) {
		return OperationResult{}, ErrCompatibility
	}
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return OperationResult{}, ErrStaleActivityLease
	}
	result.Data = slices.Clone(result.Data)
	if result.FailureCode == "" {
		toValidate := *result
		toValidate.Data = slices.Clone(result.Data)
		if err := req.ValidateResult(ctx, toValidate); err != nil {
			if s.ctx.Err() != nil || ctx.Err() != nil {
				if started {
					return OperationResult{}, errors.Join(ErrOperationUnresolved, ErrStaleActivityLease)
				}
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
	result.Data = slices.Clone(result.Data)
	if result.FailureCode != "" {
		return *result, ErrOperationFailed
	}
	return *result, nil
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
