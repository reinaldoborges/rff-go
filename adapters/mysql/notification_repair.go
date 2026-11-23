package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
		_, execution, err := s.fencedExecutionForProgress(ctx, tx, lease, clock)
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

// ApplyCheckedNotificationControl validates stored identifiers before a new decision.
func (s *Store[EntityID, StateID]) ApplyCheckedNotificationControl(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, control transition.NotificationControl,
) error {
	if clock == nil {
		return transition.ErrInvalidLease
	}
	if control.ID == "" || control.Action != transition.RetryNotification && control.Action != transition.WaiveNotification {
		return transition.ErrInvalidHook
	}
	var compatibilityErr error
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		// Phase 1: replay an identical control before checking the lease clock.
		replayed, err := s.replayNotificationControl(ctx, tx, lease, control)
		if err != nil {
			return err
		}
		if replayed {
			return nil
		}

		// Phase 2: require a current fence and an eligible repair execution.
		now, execution, err := s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if err := validateNotificationRepairControl(execution); err != nil {
			return err
		}

		// Phase 3: revalidate persisted compatibility under a fresh fence.
		data, err := json.Marshal(execution)
		if err != nil {
			return fmt.Errorf("encode control execution: %w", err)
		}
		_, decodeErr := s.decodeRecord(data)
		now = clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		if fenceErr := s.validTransitionFence(ctx, tx, lease, now); fenceErr != nil {
			return fenceErr
		}
		if decodeErr != nil {
			var incompatible *transition.CompatibilityError
			if !errors.As(decodeErr, &incompatible) || !validDiagnostic(execution, incompatible.Diagnostic) {
				return decodeErr
			}
			execution.Compatibility = &incompatible.Diagnostic
			if err := saveExecution(ctx, tx, lease.ExecutionID, execution); err != nil {
				return err
			}
			compatibilityErr = decodeErr
			return nil
		}

		// Phase 4: persist the accepted control and, on waiver, release its reservation.
		return applyNotificationControl(ctx, tx, lease.ExecutionID, execution, control, now)
	})
	if err != nil {
		return err
	}
	return compatibilityErr
}

func (s *Store[EntityID, StateID]) replayNotificationControl(
	ctx context.Context, tx *sql.Tx, lease transition.TransitionLease[EntityID], control transition.NotificationControl,
) (bool, error) {
	stored, err := s.readControlExecution(ctx, tx, lease)
	if err != nil {
		return false, err
	}
	for _, prior := range stored.NotificationControls {
		if prior.Control.ID != control.ID {
			continue
		}
		if prior.Control != control {
			return false, transition.ErrConflict
		}
		return true, nil
	}
	return false, nil
}

func validateNotificationRepairControl(execution diskRecord) error {
	if execution.Status != transition.NotificationRepair || execution.NotificationFailure == nil ||
		execution.CommitReceipt == nil || execution.CommitReceipt.Outcome != transition.CommitSucceeded {
		return transition.ErrInvalidHook
	}
	if execution.Compatibility != nil {
		return transition.ErrCompatibility
	}
	return nil
}

func applyNotificationControl(
	ctx context.Context, tx *sql.Tx, executionID string, execution diskRecord, control transition.NotificationControl, acceptedAt time.Time,
) error {
	execution.NotificationControls = append(execution.NotificationControls, transition.NotificationControlReceipt{
		Control: control, AcceptedAt: acceptedAt.UTC(), Failure: *execution.NotificationFailure,
	})
	if control.Action == transition.RetryNotification {
		execution.Status = transition.Notifying
		execution.NotificationFailure = nil
	} else {
		execution.Status = transition.Complete
		execution.NotificationWaived = true
	}
	if err := saveExecution(ctx, tx, executionID, execution); err != nil {
		return err
	}
	if execution.Status == transition.Complete {
		return releaseReservation(ctx, tx, executionID)
	}
	return nil
}
