package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ActiveExecutionIDs lists executions reserved for reconciliation.
func (s *Store[EntityID, StateID]) ActiveExecutionIDs(ctx context.Context) (ids []string, err error) {
	rows, err := s.db.QueryContext(ctx, `SELECT execution_id FROM rff_active ORDER BY execution_id`)
	if err != nil {
		return nil, fmt.Errorf("list active executions: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close active execution query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("read active execution ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list active executions: %w", err)
	}
	return ids, nil
}

// ReadActiveExecution loads one reserved execution using its pinned identifiers.
func (s *Store[EntityID, StateID]) ReadActiveExecution(ctx context.Context, id string) (transition.Execution[EntityID, StateID], error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT e.record FROM rff_executions AS e
		JOIN rff_active AS a ON a.execution_id = e.id AND a.entity = e.entity
		WHERE e.id = ?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.Execution[EntityID, StateID]{}, transition.ErrExecutionNotActive
	}
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("read active execution: %w", err)
	}
	record, err := s.decodeRecord(data)
	if err != nil {
		return transition.Execution[EntityID, StateID]{}, err
	}
	if record.ID != id {
		return transition.Execution[EntityID, StateID]{}, fmt.Errorf("%w: execution ID does not match index", ErrUnsupportedSchema)
	}
	return record, nil
}
