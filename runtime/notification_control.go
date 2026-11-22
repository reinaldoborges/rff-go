package runtime

import (
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
