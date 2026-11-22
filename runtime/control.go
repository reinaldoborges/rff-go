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

// AbortStore finalizes a successful recovery and releases the reservation.
type AbortStore[EntityID comparable] interface {
	FinishAbort(context.Context, TransitionLease[EntityID], func() time.Time) error
}
