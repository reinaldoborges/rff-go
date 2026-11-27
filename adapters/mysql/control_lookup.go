package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func (s *Store[EntityID, StateID]) readControlExecution(
	ctx context.Context, tx *sql.Tx, lease transition.TransitionLease[EntityID],
) (diskRecord, error) {
	var stored diskRecord
	if err := transition.ValidateDurableID(lease.ExecutionID); err != nil {
		return stored, errors.Join(err, transition.ErrStaleLease)
	}
	entity, err := s.encodeEntity(lease.Entity)
	if err != nil {
		return stored, err
	}
	var data []byte
	err = tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ? AND entity = ?`, lease.ExecutionID, entity).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return stored, transition.ErrStaleLease
	}
	if err != nil {
		return stored, fmt.Errorf("read control execution: %w", err)
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		return stored, fmt.Errorf("%w: decode control execution: %w", ErrUnsupportedSchema, err)
	}
	if !transition.SupportedExecutionSchema(stored.SchemaVersion) || stored.ID != lease.ExecutionID || !bytes.Equal(stored.Entity, entity) {
		return stored, fmt.Errorf("%w: invalid control execution", ErrUnsupportedSchema)
	}
	return stored, nil
}
