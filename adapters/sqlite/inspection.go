package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ListExecutionIDs lists both active and terminal retained executions.
func (s *Store[EntityID, StateID]) ListExecutionIDs(ctx context.Context) (ids []string, err error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM rff_executions ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close execution query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("read execution ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	return ids, nil
}

// InspectExecution reads a consistent, payload-free protocol summary.
func (s *Store[EntityID, StateID]) InspectExecution(ctx context.Context, id string) (inspection transition.ExecutionInspection, err error) {
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrExecutionNotActive
		}
		if err != nil {
			return fmt.Errorf("read execution for inspection: %w", err)
		}
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil || record.ID != id || !transition.SupportedExecutionSchema(record.SchemaVersion) {
			return fmt.Errorf("%w: invalid inspected execution", ErrUnsupportedSchema)
		}
		if record.Compatibility != nil && !validDiagnostic(record, *record.Compatibility) {
			return fmt.Errorf("%w: invalid compatibility diagnostic", ErrUnsupportedSchema)
		}
		inspection = transition.ExecutionInspection{
			ID: record.ID, Operation: record.Operation, MachineID: record.MachineID, MachineVersion: record.MachineVersion,
			Status: record.Status, AcceptedAt: record.AcceptedAt, Deadline: record.Deadline, CycleDeadline: record.CycleDeadline,
			RejectionRecoveryDeadline: record.RejectionRecoveryDeadline, Failure: record.Failure, FailureDisposition: record.FailureDisposition,
			Compatibility: record.Compatibility, CommitReceipt: record.CommitReceipt, NotificationIntentID: record.NotificationIntentID,
			NotificationFailure: record.NotificationFailure, NotificationWaived: record.NotificationWaived, Controls: record.Controls,
			NotificationControls: record.NotificationControls, Migrations: record.Migrations,
		}
		work, err := loadHook(ctx, tx, id)
		if errors.Is(err, transition.ErrNoHookActivity) {
			return nil
		}
		if err != nil {
			return err
		}
		progress := work.Progress
		progress.Started, progress.LeaseExpiresAt, progress.BudgetUsed = work.Started, work.ExpiresAt, work.BudgetUsed
		inspection.Hook = &progress
		return nil
	})
	if err != nil {
		return transition.ExecutionInspection{}, err
	}
	return inspection, nil
}
