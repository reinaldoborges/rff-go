package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var ErrOperationResolutionDenied = errors.New("tracked operation resolution was not authorized")

// OperationResolutionRequest is an idempotent operator decision for one
// unresolved downstream operation.
type OperationResolutionRequest struct {
	ID             string
	Identity       OperationIdentity
	Assessment     OperationAssessment
	VerifyEvidence func(context.Context, OperationBinding, OperationEvidence) error
	ValidateResult func(context.Context, OperationBinding, OperationResult) error
}

// OperationResolutionAuthorization is the host's authorization and safe audit
// attribution. Reason must not contain secrets or raw provider errors.
type OperationResolutionAuthorization struct {
	Principal string
	Reason    string
}

// OperationResolutionStore replays and records fenced, audited operator
// decisions after the host has authorized this request.
type OperationResolutionStore interface {
	OperationInspector
	ReplayOperationResolution(context.Context, OperationIdentity, string, string, string, OperationAssessment) (OperationInspection, bool, error)
	RecordOperationResolution(context.Context, OperationIdentity, string, string, string, OperationAssessment, func() time.Time) (OperationInspection, error)
}

// AuthorizedOperationResolver separates operator authorization from durable
// resolution. A resolution never schedules or resumes hook work.
type AuthorizedOperationResolver struct {
	store     OperationResolutionStore
	authorize func(context.Context, OperationResolutionRequest) (OperationResolutionAuthorization, error)
	clock     func() time.Time
}

// NewAuthorizedOperationResolver constructs the host authorization boundary.
func NewAuthorizedOperationResolver(
	store OperationResolutionStore,
	authorize func(context.Context, OperationResolutionRequest) (OperationResolutionAuthorization, error),
	clock func() time.Time,
) (*AuthorizedOperationResolver, error) {
	if nilcheck.IsNil(store) || authorize == nil || clock == nil {
		return nil, ErrInvalidRequest
	}
	return &AuthorizedOperationResolver{store: store, authorize: authorize, clock: clock}, nil
}

// Resolve validates the safe evidence and result, authorizes this request, then
// records it idempotently without dispatching or resuming transition work.
func (r *AuthorizedOperationResolver) Resolve(ctx context.Context, request OperationResolutionRequest) (OperationInspection, error) {
	if request.ID == "" || request.Identity.ExecutionID == "" || request.Identity.Key == "" {
		return OperationInspection{}, ErrInvalidRequest
	}
	authorization, err := r.authorize(ctx, request)
	if err != nil {
		return OperationInspection{}, errors.Join(ErrOperationResolutionDenied, err)
	}
	if authorization.Principal == "" || authorization.Reason == "" {
		return OperationInspection{}, ErrOperationResolutionDenied
	}
	inspection, err := r.store.InspectTrackedOperation(ctx, request.Identity)
	if err != nil {
		return OperationInspection{}, err
	}
	replayed, found, err := r.store.ReplayOperationResolution(
		ctx, request.Identity, request.ID, authorization.Principal, authorization.Reason, request.Assessment,
	)
	if err != nil {
		return replayed, err
	}
	if found {
		return replayed, nil
	}
	if request.VerifyEvidence == nil || request.Assessment.Repeat != nil {
		return OperationInspection{}, ErrInvalidRequest
	}
	now := r.clock()
	if !OperationAssessmentValid(inspection.Binding, request.Assessment, now) {
		return OperationInspection{}, ErrCompatibility
	}
	if err := request.VerifyEvidence(ctx, inspection.Binding, request.Assessment.Evidence); err != nil {
		return OperationInspection{}, fmt.Errorf("%w: operator evidence is unverifiable", ErrCompatibility)
	}
	switch request.Assessment.Outcome {
	case OperationOutcomeSucceeded:
		if request.ValidateResult == nil || request.ValidateResult(ctx, inspection.Binding, *request.Assessment.Result) != nil {
			return OperationInspection{}, fmt.Errorf("%w: operator success result is unavailable", ErrCompatibility)
		}
	case OperationOutcomeFailed:
	case OperationOutcomeAbsent:
		if !request.Assessment.Evidence.Quiesced {
			return OperationInspection{}, ErrOperationUnresolved
		}
	case OperationOutcomeUnknown:
		return OperationInspection{}, ErrOperationUnresolved
	default:
		return OperationInspection{}, ErrInvalidRequest
	}
	return r.store.RecordOperationResolution(
		ctx, request.Identity, request.ID, authorization.Principal, authorization.Reason, request.Assessment, r.clock,
	)
}
