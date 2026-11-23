package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type operationRecord struct {
	Version    int
	Binding    transition.OperationBinding
	ActivityID string
	Generation uint64
	Result     *transition.OperationResult
}

func decodeOperation(id string, key, data []byte) (operationRecord, error) {
	var record operationRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("%w: invalid operation record", ErrUnsupportedSchema)
	}
	b := record.Binding
	if record.Version != 1 || b.Identity.ExecutionID != id || !bytes.Equal([]byte(b.Identity.Key), key) ||
		id == "" || b.Identity.Key == "" || b.Kind == "" || b.BehaviorVersion == "" ||
		b.ComparatorVersion == "" || b.ResultCodec.Name == "" || b.ResultCodec.Version == "" ||
		record.ActivityID == "" || record.Generation == 0 ||
		(record.Result != nil && !transition.ValidOperationResult(*record.Result, b.ResultCodec)) {
		return record, fmt.Errorf("%w: inconsistent operation record", ErrUnsupportedSchema)
	}
	return record, nil
}

func validateOperations(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT o.execution_id, o.operation_key, e.id FROM rff_operations o LEFT JOIN rff_executions e ON e.id = o.execution_id`)
	if err != nil {
		return fmt.Errorf("%w: read operations: %w", ErrUnsupportedSchema, err)
	}
	defer rows.Close()
	seen := make(map[transition.OperationIdentity]bool)
	for rows.Next() {
		var id string
		var key []byte
		var execution sql.NullString
		if err := rows.Scan(&id, &key, &execution); err != nil {
			return fmt.Errorf("%w: read operation: %w", ErrUnsupportedSchema, err)
		}
		identity := transition.OperationIdentity{ExecutionID: id, Key: string(key)}
		if !execution.Valid || id == "" || len(key) == 0 || seen[identity] {
			return fmt.Errorf("%w: missing execution or duplicate operation", ErrUnsupportedSchema)
		}
		seen[identity] = true
	}
	return rows.Err()
}

func operationClaim(ctx context.Context, tx *sql.Tx, lease transition.HookLease, now time.Time) error {
	work, err := loadClaim(ctx, tx, lease, now)
	if err != nil {
		return err
	}
	if !work.Started || work.Progress.Completed {
		return transition.ErrStaleActivityLease
	}
	return ensureCompatibleExecution(ctx, tx, lease.ExecutionID)
}

// InspectTrackedOperation reads a durable operation without requiring an active hook.
func (s *Store[EntityID, StateID]) InspectTrackedOperation(ctx context.Context, identity transition.OperationIdentity) (transition.OperationInspection, error) {
	var inspection transition.OperationInspection
	if identity.ExecutionID == "" || identity.Key == "" {
		return inspection, transition.ErrInvalidRequest
	}
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, identity.ExecutionID, []byte(identity.Key)).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return inspection, transition.ErrOperationNotFound
	}
	if err != nil {
		return inspection, err
	}
	record, err := decodeOperation(identity.ExecutionID, []byte(identity.Key), data)
	if err != nil {
		return inspection, err
	}
	return transition.OperationInspection{Binding: record.Binding, ActivityID: record.ActivityID, Generation: record.Generation, MayHaveDispatched: true, Result: record.Result}, nil
}

func (s *Store[EntityID, StateID]) BeginTrackedOperation(
	ctx context.Context, lease transition.HookLease, clock func() time.Time, binding transition.OperationBinding,
	equal func(context.Context, []byte, []byte) (bool, error),
) (*transition.OperationResult, bool, error) {
	if clock == nil || equal == nil || binding.Identity.ExecutionID != lease.ExecutionID || binding.Identity.Key == "" ||
		binding.Kind == "" || binding.BehaviorVersion == "" || binding.ComparatorVersion == "" ||
		binding.ResultCodec.Name == "" || binding.ResultCodec.Version == "" {
		return nil, false, transition.ErrInvalidRequest
	}
	var result *transition.OperationResult
	var started bool
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		if err := operationClaim(ctx, tx, lease, clock()); err != nil {
			return err
		}
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, lease.ExecutionID, []byte(binding.Identity.Key)).Scan(&data)
		if err == nil {
			record, err := decodeOperation(lease.ExecutionID, []byte(binding.Identity.Key), data)
			if err != nil {
				return err
			}
			old := record.Binding
			if old.Kind != binding.Kind || old.BehaviorVersion != binding.BehaviorVersion ||
				old.ComparatorVersion != binding.ComparatorVersion || old.ResultCodec != binding.ResultCodec {
				return transition.ErrConflict
			}
			matches, err := equal(ctx, bytes.Clone(old.Input), bytes.Clone(binding.Input))
			if claimErr := operationClaim(ctx, tx, lease, clock()); claimErr != nil {
				return claimErr
			}
			if err != nil {
				return fmt.Errorf("%w: operation comparison unavailable", transition.ErrCompatibility)
			}
			if !matches {
				return transition.ErrConflict
			}
			if record.Result == nil {
				return transition.ErrOperationUnresolved
			}
			result = record.Result
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read operation: %w", err)
		}
		record := operationRecord{Version: 1, Binding: binding, ActivityID: lease.ActivityID, Generation: lease.Generation}
		data, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode operation: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_operations (execution_id, operation_key, record) VALUES (?, ?, ?)`, lease.ExecutionID, []byte(binding.Identity.Key), data); err != nil {
			return fmt.Errorf("record operation intent: %w", err)
		}
		if err := operationClaim(ctx, tx, lease, clock()); err != nil {
			return err
		}
		started = true
		return nil
	})
	return result, started, err
}

func (s *Store[EntityID, StateID]) CompleteTrackedOperation(
	ctx context.Context, lease transition.HookLease, clock func() time.Time, identity transition.OperationIdentity, result transition.OperationResult,
) error {
	if clock == nil || identity.ExecutionID != lease.ExecutionID || identity.Key == "" {
		return transition.ErrInvalidRequest
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		if err := operationClaim(ctx, tx, lease, clock()); err != nil {
			return err
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, identity.ExecutionID, []byte(identity.Key)).Scan(&data); err != nil {
			return fmt.Errorf("read operation: %w", err)
		}
		record, err := decodeOperation(identity.ExecutionID, []byte(identity.Key), data)
		if err != nil {
			return err
		}
		if record.ActivityID != lease.ActivityID || record.Generation != lease.Generation || record.Result != nil {
			return transition.ErrStaleActivityLease
		}
		if !transition.ValidOperationResult(result, record.Binding.ResultCodec) {
			return transition.ErrCompatibility
		}
		record.Result = &result
		data, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode operation result: %w", err)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = ? AND operation_key = ?`, data, identity.ExecutionID, []byte(identity.Key)); err != nil {
			return err
		}
		return operationClaim(ctx, tx, lease, clock())
	})
}

func ensureOperationsSettled(ctx context.Context, tx *sql.Tx, id string) error {
	operations, err := loadOperationInspections(ctx, tx, id)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if operation.Result == nil {
			return transition.ErrOperationUnresolved
		}
	}
	return nil
}

func loadOperationInspections(ctx context.Context, tx *sql.Tx, id string) ([]transition.OperationInspection, error) {
	rows, err := tx.QueryContext(ctx, `SELECT operation_key, record FROM rff_operations WHERE execution_id = ? ORDER BY operation_key`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var operations []transition.OperationInspection
	for rows.Next() {
		var key, data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return nil, err
		}
		record, err := decodeOperation(id, key, data)
		if err != nil {
			return nil, err
		}
		operations = append(operations, transition.OperationInspection{
			Binding: record.Binding, ActivityID: record.ActivityID, Generation: record.Generation,
			MayHaveDispatched: true, Result: record.Result,
		})
	}
	return operations, rows.Err()
}
