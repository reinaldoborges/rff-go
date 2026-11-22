package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type OperatorAuditRecord struct {
	Actor       string
	Action      string
	ExecutionID string
	Allowed     bool
	At          time.Time
}

func (f *Fixture) authorizeOperator(ctx context.Context, actor, action, executionID string) error {
	if actor == "" {
		return ErrInvalidRequest
	}
	var allowed bool
	err := f.transaction(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_operators WHERE id = ?)`, actor).Scan(&allowed); err != nil {
			return fmt.Errorf("authenticate test operator: %w", err)
		}
		now := f.clock().UTC()
		if now.IsZero() {
			return ErrInvalidRequest
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_operator_audit (actor, action, execution_id, allowed, occurred_at)
			VALUES (?, ?, ?, ?, ?)`, actor, action, executionID, allowed, now.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("audit operator action: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !allowed {
		return ErrDenied
	}
	return nil
}

// ListExecutions lists runtime records for an authorized local operator.
func (f *Fixture) ListExecutions(ctx context.Context, actor string) ([]string, error) {
	if err := f.authorizeOperator(ctx, actor, "list", ""); err != nil {
		return nil, err
	}
	return f.store.ListExecutionIDs(ctx)
}

// InspectExecution exposes payload-free runtime diagnostics to a local operator.
func (f *Fixture) InspectExecution(ctx context.Context, actor, id string) (transition.ExecutionInspection, error) {
	if id == "" {
		return transition.ExecutionInspection{}, ErrInvalidRequest
	}
	if err := f.authorizeOperator(ctx, actor, "inspect", id); err != nil {
		return transition.ExecutionInspection{}, err
	}
	return f.store.InspectExecution(ctx, id)
}

// ControlExecution submits a phase-gated control on behalf of a local operator.
func (f *Fixture) ControlExecution(ctx context.Context, actor, id, key string, action transition.ControlAction) error {
	if id == "" || key == "" || (action != transition.ControlRetry && action != transition.ControlAbort &&
		action != transition.ControlNotificationRetry && action != transition.ControlNotificationWaive) {
		return ErrInvalidRequest
	}
	if err := f.authorizeOperator(ctx, actor, string(action), id); err != nil {
		return err
	}
	execution, err := f.store.ReadActiveExecution(ctx, id)
	if errors.Is(err, transition.ErrExecutionNotActive) {
		record, inspectErr := f.store.InspectExecution(ctx, id)
		if inspectErr != nil {
			return inspectErr
		}
		switch action {
		case transition.ControlRetry, transition.ControlAbort:
			expected := transition.RetryIntervention
			if action == transition.ControlAbort {
				expected = transition.AbortIntervention
			}
			for _, prior := range record.Controls {
				if prior.Control.ID == key {
					if prior.Control.Action != expected {
						return transition.ErrConflict
					}
					return nil
				}
			}
		default:
			expected := transition.RetryNotification
			if action == transition.ControlNotificationWaive {
				expected = transition.WaiveNotification
			}
			for _, prior := range record.NotificationControls {
				if prior.Control.ID == key {
					if prior.Control.Action != expected {
						return transition.ErrConflict
					}
					return nil
				}
			}
		}
	}
	if err != nil {
		return err
	}
	lease, err := f.store.AcquireTransitionLease(ctx, execution.Entity, id, "taskmanager", f.clock, time.Minute)
	if err != nil {
		return err
	}
	check := func(ctx context.Context, _ transition.ControlAuthorization) error {
		var allowed bool
		if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_operators WHERE id = ?)`, actor).Scan(&allowed); err != nil {
			return fmt.Errorf("reauthorize test operator: %w", err)
		}
		if !allowed {
			return ErrDenied
		}
		return nil
	}
	switch action {
	case transition.ControlRetry, transition.ControlAbort:
		controls, err := transition.NewAuthorizedHookControls(f.store, f.runner, check)
		if err != nil {
			return err
		}
		hookAction := transition.RetryIntervention
		if action == transition.ControlAbort {
			hookAction = transition.AbortIntervention
		}
		_, err = controls.Hook(ctx, lease, f.clock, transition.HookControl{ID: key, Action: hookAction})
		return err
	default:
		controls, err := transition.NewAuthorizedControls[string](f.store, check)
		if err != nil {
			return err
		}
		notificationAction := transition.RetryNotification
		if action == transition.ControlNotificationWaive {
			notificationAction = transition.WaiveNotification
		}
		return controls.Notification(ctx, lease, f.clock, transition.NotificationControl{ID: key, Action: notificationAction})
	}
}

// OperatorAudit returns the application's operator-action audit to a local operator.
func (f *Fixture) OperatorAudit(ctx context.Context, actor string) ([]OperatorAuditRecord, error) {
	if actor == "" {
		return nil, ErrInvalidRequest
	}
	var allowed bool
	if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_operators WHERE id = ?)`, actor).Scan(&allowed); err != nil {
		return nil, fmt.Errorf("authenticate audit reader: %w", err)
	}
	if !allowed {
		return nil, ErrDenied
	}
	rows, err := f.db.QueryContext(ctx, `SELECT actor, action, execution_id, allowed, occurred_at FROM tm_operator_audit ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("read operator audit: %w", err)
	}
	defer rows.Close()
	var records []OperatorAuditRecord
	for rows.Next() {
		var record OperatorAuditRecord
		var at string
		if err := rows.Scan(&record.Actor, &record.Action, &record.ExecutionID, &record.Allowed, &at); err != nil {
			return nil, fmt.Errorf("scan operator audit: %w", err)
		}
		record.At, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, fmt.Errorf("decode operator audit time: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operator audit: %w", err)
	}
	return records, nil
}

// SeedOperator registers a local test-only operator, distinct from domain users.
func (f *Fixture) SeedOperator(ctx context.Context, operator string) error {
	if operator == "" {
		return ErrInvalidRequest
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		var user bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_users WHERE id = ?)`, operator).Scan(&user); err != nil {
			return fmt.Errorf("check operator identity: %w", err)
		}
		if user {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO tm_operators (id) VALUES (?)`, operator); err != nil {
			return fmt.Errorf("seed operator: %w", err)
		}
		return nil
	})
}

// InspectMembership gives a test-only operator access to retained history.
func (f *Fixture) InspectMembership(ctx context.Context, operator, id string) (Membership, error) {
	if id == "" {
		return Membership{}, ErrInvalidRequest
	}
	if err := f.authorizeOperator(ctx, operator, "inspect-membership", id); err != nil {
		return Membership{}, err
	}
	membership, _, _, err := f.readMembership(ctx, id)
	return membership, err
}
