package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// EnterNotificationRepair records a definitive handoff failure.
func (s *Store[EntityID, StateID]) EnterNotificationRepair(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, intentID string, failure fsm.Failure,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if intentID == "" {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		_, execution, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status == transition.NotificationRepair && execution.NotificationIntentID == intentID {
			if execution.NotificationFailure != nil && *execution.NotificationFailure == failure {
				return nil
			}
			return transition.ErrConflict
		}
		if execution.Status != transition.Notifying || !execution.Notify || execution.NotificationIntentID != intentID ||
			execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded {
			return transition.ErrInvalidHook
		}
		execution.Status = transition.NotificationRepair
		execution.NotificationFailure = &failure
		return saveExecution(ctx, tx, lease.ExecutionID, execution)
	})
}

// ApplyNotificationControl applies a host-authorized repair retry or waiver.
func (s *Store[EntityID, StateID]) ApplyNotificationControl(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, control transition.NotificationControl,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if control.ID == "" || control.Action != transition.RetryNotification && control.Action != transition.WaiveNotification {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		stored, err := s.readControlExecution(ctx, tx, lease)
		if err != nil {
			return err
		}
		for _, prior := range stored.NotificationControls {
			if prior.Control.ID == control.ID {
				if prior.Control != control {
					return transition.ErrConflict
				}
				return nil
			}
		}
		now, execution, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if execution.Status != transition.NotificationRepair || execution.NotificationFailure == nil ||
			execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded {
			return transition.ErrInvalidHook
		}
		execution.NotificationControls = append(execution.NotificationControls, transition.NotificationControlReceipt{
			Control: control, AcceptedAt: now.UTC(), Failure: *execution.NotificationFailure,
		})
		if control.Action == transition.RetryNotification {
			execution.Status = transition.Notifying
			execution.NotificationFailure = nil
		} else {
			execution.Status = transition.Complete
			execution.NotificationWaived = true
		}
		if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
			return err
		}
		if execution.Status == transition.Complete {
			return releaseReservation(ctx, tx, lease.ExecutionID)
		}
		return nil
	})
}
