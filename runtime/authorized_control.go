package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// ControlAction distinguishes operations for application authorization.
type ControlAction string

const (
	ControlRetry             ControlAction = "retry"
	ControlAbort             ControlAction = "abort"
	ControlNotificationRetry ControlAction = "notification-retry"
	ControlNotificationWaive ControlAction = "notification-waive"
	ControlMigrate           ControlAction = "migrate"
)

// ControlAuthorization identifies an attempted operation without defining an actor.
// The host supplies identity through its own context and owns authorization and audit.
type ControlAuthorization struct {
	ExecutionID string
	RequestID   string
	Action      ControlAction
	Migration   *MigrationRequest
}

// AuthorizedControlStore supplies the phase-gated control operations.
type AuthorizedControlStore[EntityID comparable] interface {
	ApplyCheckedNotificationControl(context.Context, TransitionLease[EntityID], func() time.Time, NotificationControl) error
}

// AuthorizedControls invokes host authorization before every request, including
// replays. The host owns actor attribution and must record its own audit.
type AuthorizedControls[EntityID comparable] struct {
	store     AuthorizedControlStore[EntityID]
	authorize func(context.Context, ControlAuthorization) error
}

func NewAuthorizedControls[EntityID comparable](
	store AuthorizedControlStore[EntityID], authorize func(context.Context, ControlAuthorization) error,
) (*AuthorizedControls[EntityID], error) {
	if nilcheck.IsNil(store) || authorize == nil {
		return nil, errors.New("control store and host authorization are required")
	}
	return &AuthorizedControls[EntityID]{store: store, authorize: authorize}, nil
}

func (c *AuthorizedControls[EntityID]) Notification(
	ctx context.Context, lease TransitionLease[EntityID], clock func() time.Time, control NotificationControl,
) error {
	var action ControlAction
	switch control.Action {
	case RetryNotification:
		action = ControlNotificationRetry
	case WaiveNotification:
		action = ControlNotificationWaive
	default:
		return ErrInvalidHook
	}
	if control.ID == "" || lease.ExecutionID == "" || clock == nil {
		return ErrInvalidRequest
	}
	if err := c.authorize(ctx, ControlAuthorization{ExecutionID: lease.ExecutionID, RequestID: control.ID, Action: action}); err != nil {
		return err
	}
	return c.store.ApplyCheckedNotificationControl(ctx, lease, clock, control)
}
