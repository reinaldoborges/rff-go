package runtime

import (
	"context"
	"errors"
	"time"
)

var (
	ErrLeaseHeld    = errors.New("transition lease is already held")
	ErrStaleLease   = errors.New("transition lease is stale")
	ErrInvalidLease = errors.New("invalid transition lease request")
)

// TransitionLease fences a coordinator's authority over one active execution.
type TransitionLease[EntityID comparable] struct {
	Entity      EntityID
	ExecutionID string
	Owner       string
	Generation  uint64
	ExpiresAt   time.Time
}

// TransitionLeaseStore serializes transition lease acquisition and renewal.
// Time is read from the supplied clock while holding the storage transaction;
// callers sharing a store must use a consistent clock. Clock callbacks must not
// re-enter the store. Each coordinator instance must have a unique owner;
// repeating an acquisition by the current owner returns the existing lease.
// Expiry is inclusive and never settles the execution.
type TransitionLeaseStore[EntityID comparable] interface {
	AcquireTransitionLease(context.Context, EntityID, string, string, func() time.Time, time.Duration) (TransitionLease[EntityID], error)
	RenewTransitionLease(context.Context, TransitionLease[EntityID], func() time.Time, time.Duration) (TransitionLease[EntityID], error)
}
