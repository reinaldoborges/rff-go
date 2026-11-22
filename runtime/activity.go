package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

var (
	// ErrAttemptBudget indicates that no started-hook attempts remain in the cycle.
	ErrAttemptBudget = errors.New("hook attempt budget exhausted")
	// ErrAttemptUnknown is supplied to the application classifier after worker loss.
	ErrAttemptUnknown = errors.New("hook attempt outcome is unknown")
	// ErrActivityLeaseHeld indicates that the worker's claim has not expired.
	ErrActivityLeaseHeld = errors.New("hook activity lease is still held")
	// ErrRetryIntervention indicates that the pinned policy cannot authorize a retry.
	ErrRetryIntervention = errors.New("hook retry requires intervention")
)

// HookDispatch describes one logical state-hook invocation.
type HookDispatch struct {
	Kind       HookKind
	RetryCycle uint
	DueAt      time.Time
	Failure    *fsm.Failure
}

// HookAttempt is the durable history of one started activity attempt.
type HookAttempt struct {
	Metadata    fsm.AttemptMetadata
	Kind        HookKind
	StartedAt   time.Time
	CompletedAt time.Time
	Result      *HookResult
	Unknown     bool
}

// HookProgress is persisted within an active transition execution.
type HookProgress struct {
	ActivityID  string
	Dispatch    HookDispatch
	AvailableAt time.Time
	Completed   bool
	Result      HookResult
	Attempts    []HookAttempt
}

// HookDispatcher is the coordinator side of durable hook activity dispatch.
type HookDispatcher[EntityID comparable] interface {
	DispatchHook(context.Context, TransitionLease[EntityID], func() time.Time, HookDispatch) (string, error)
	ReadHookProgress(context.Context, string) (HookProgress, error)
	ResolveUnknownHook(context.Context, TransitionLease[EntityID], func() time.Time, string, uint64, HookResult) error
	RedeliverHook(context.Context, TransitionLease[EntityID], func() time.Time, string, uint64, time.Time) error
}
