package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var ErrContinuationControlDenied = errors.New("continuation control or inspection not authorized")

// ContinuationControlAccess is supplied to the host before every control,
// replay, or inspection. Reasons must contain only safe audit data.
type ContinuationControlAccess struct {
	ExecutionID string
	RequestID   string
	Action      ContinuationControlAction
	Reason      string
}

// ContinuationInspection omits encoded event and input payloads.
type ContinuationInspection struct {
	Policy               fsm.Policy
	Phase                ContinuationPhase
	Attempts             uint
	CycleStarted         time.Time
	Deadline             time.Time
	StartedAt            time.Time
	AttemptPaused        time.Duration
	PausedAt             time.Time
	PermitExpires        time.Time
	NextAttemptAt        time.Time
	CompatibilityBlocked bool
	HasDecision          bool
	Continues            bool
	SuccessorID          []byte
	History              []ContinuationAttempt
	Controls             []ContinuationControlAudit
}

const InspectContinuation ContinuationControlAction = "inspect"

type ContinuationControlStore[E comparable, S comparable] interface {
	ApplyContinuationControl(context.Context, ContinuationMarker[E, S], func() time.Time, ContinuationControl) error
	ReadContinuation(context.Context, string) (ContinuationProgress, error)
}

// ContinuationControls gates every disclosure and operation through host
// authorization, including identical replays.
type ContinuationControls[E comparable, S comparable] struct {
	store     ContinuationControlStore[E, S]
	authorize func(context.Context, ContinuationControlAccess) (string, error)
}

func NewContinuationControls[E comparable, S comparable](
	store ContinuationControlStore[E, S],
	authorize func(context.Context, ContinuationControlAccess) (string, error),
) (*ContinuationControls[E, S], error) {
	if nilcheck.IsNil(store) || authorize == nil {
		return nil, ErrInvalidRequest
	}
	return &ContinuationControls[E, S]{store: store, authorize: authorize}, nil
}

func (c *ContinuationControls[E, S]) Apply(
	ctx context.Context, marker ContinuationMarker[E, S], clock func() time.Time,
	id string, action ContinuationControlAction, reason string,
) error {
	if marker.ExecutionID == "" || id == "" || reason == "" || clock == nil || (action != RetryContinuation && action != WaiveContinuation) {
		return ErrInvalidRequest
	}
	principal, err := c.authorize(ctx, ContinuationControlAccess{
		ExecutionID: marker.ExecutionID, RequestID: id, Action: action, Reason: reason,
	})
	if err != nil || principal == "" {
		return ErrContinuationControlDenied
	}
	return c.store.ApplyContinuationControl(ctx, marker, clock, ContinuationControl{ID: id, Action: action, Principal: principal, Reason: reason})
}

func (c *ContinuationControls[E, S]) Inspect(ctx context.Context, executionID string) (ContinuationInspection, error) {
	if executionID == "" {
		return ContinuationInspection{}, ErrInvalidRequest
	}
	principal, err := c.authorize(ctx, ContinuationControlAccess{ExecutionID: executionID, Action: InspectContinuation})
	if err != nil || principal == "" {
		return ContinuationInspection{}, ErrContinuationControlDenied
	}
	progress, err := c.store.ReadContinuation(ctx, executionID)
	if err != nil {
		return ContinuationInspection{}, err
	}
	result := ContinuationInspection{
		Policy: progress.Policy, Phase: progress.Phase, Attempts: progress.Attempts, CycleStarted: progress.CycleStarted,
		Deadline: progress.Deadline, StartedAt: progress.StartedAt, AttemptPaused: progress.AttemptPaused,
		PausedAt: progress.PausedAt, PermitExpires: progress.PermitExpires, NextAttemptAt: progress.NextAttemptAt,
		HasDecision: progress.Decision != nil, CompatibilityBlocked: progress.CompatibilityBlocked,
		SuccessorID: progress.SuccessorID, History: progress.History, Controls: progress.Controls,
	}
	if progress.Decision != nil {
		result.Continues = progress.Decision.Continue
	}
	return result, nil
}
