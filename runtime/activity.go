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

// HookProgress describes durable work and the current claim. Started,
// LeaseExpiresAt, and BudgetUsed are derived from the durable activity record.
type HookProgress struct {
	ActivityID     string
	Dispatch       HookDispatch
	AvailableAt    time.Time
	Completed      bool
	Result         HookResult
	Attempts       []HookAttempt
	Started        bool      `json:"-"`
	LeaseExpiresAt time.Time `json:"-"`
	BudgetUsed     uint      `json:"-"`
}

// HookDispatcher is the coordinator side of durable hook activity dispatch.
type HookDispatcher[EntityID comparable] interface {
	DispatchHook(context.Context, TransitionLease[EntityID], func() time.Time, HookDispatch) (string, error)
	ReadHookProgress(context.Context, string) (HookProgress, error)
	ResolveUnknownHook(context.Context, TransitionLease[EntityID], func() time.Time, string, uint64, HookResult) error
	RedeliverHook(context.Context, TransitionLease[EntityID], func() time.Time, string, uint64, time.Time) error
}

// WorkSuccessStore records successful transition work and its stable commit
// operation under the current transition fence, before the host commit.
type WorkSuccessStore[EntityID comparable] interface {
	MarkWorkSuccess(context.Context, TransitionLease[EntityID], func() time.Time) (string, error)
}

// InterventionStore persists an intervention after completed hook work.
type InterventionStore[EntityID comparable] interface {
	EnterHookIntervention(context.Context, TransitionLease[EntityID], func() time.Time) error
	EnterDeadlineIntervention(context.Context, TransitionLease[EntityID], func() time.Time, HookResult) error
}
