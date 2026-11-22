package runtime

import (
	"context"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

// HookControlAction identifies an authorized pre-commit control.
type HookControlAction string

const (
	RetryIntervention HookControlAction = "retry"
	AbortIntervention HookControlAction = "abort"
)

// HookControl is a host-authorized, execution-scoped idempotent request.
type HookControl struct {
	ID     string
	Action HookControlAction
}

// HookControlReceipt records the prior failure and new control-cycle deadline.
type HookControlReceipt struct {
	Control     HookControl
	AcceptedAt  time.Time
	Deadline    time.Time
	Failure     *fsm.Failure
	Disposition fsm.Disposition
}

// HookControlStore applies new controls atomically under a transition fence.
// An identical recorded control replays by execution identity even after its
// fence or reservation expires. The host must authorize both new controls and
// replays and perform its own actor attribution.
type HookControlStore[EntityID comparable] interface {
	ApplyHookControl(context.Context, TransitionLease[EntityID], func() time.Time, HookControl) (HookControlReceipt, error)
}

// AbortStore finalizes a successful recovery and releases the reservation.
type AbortStore[EntityID comparable] interface {
	FinishAbort(context.Context, TransitionLease[EntityID], func() time.Time) error
}
