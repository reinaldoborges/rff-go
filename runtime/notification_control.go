package runtime

import (
	"context"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

// NotificationControlAction identifies an authorized repair decision.
type NotificationControlAction string

const (
	RetryNotification NotificationControlAction = "retry"
	WaiveNotification NotificationControlAction = "waive"
)

// NotificationControl is an execution-scoped, host-authorized repair request.
type NotificationControl struct {
	ID     string
	Action NotificationControlAction
}

// NotificationControlReceipt retains the repair failure and decision.
type NotificationControlReceipt struct {
	Control    NotificationControl
	AcceptedAt time.Time
	Failure    fsm.Failure
}

// NotificationControlStore applies new controls under a fence and replays
// recorded controls after settlement. The host authorizes and audits both.
type NotificationControlStore[EntityID comparable] interface {
	ApplyNotificationControl(context.Context, TransitionLease[EntityID], func() time.Time, NotificationControl) error
}
