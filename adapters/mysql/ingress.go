package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func decodeInput(id string, data []byte) (transition.InputRecord, error) {
	var record transition.InputRecord
	if err := json.Unmarshal(data, &record); err != nil || record.SchemaVersion != 1 || record.ID != id ||
		record.ID == "" || record.Channel == "" || record.WorkflowID == "" ||
		(record.Kind != transition.Command && record.Kind != transition.ExternalEvent && record.Kind != transition.HumanDecision) ||
		record.Value.Codec.Name == "" || record.Value.Codec.Version == "" {
		return transition.InputRecord{}, fmt.Errorf("%w: invalid input", ErrUnsupportedSchema)
	}
	return record, nil
}

func saveInputOutcome(ctx context.Context, tx *sql.Tx, record *transition.InputRecord, outcome transition.InputOutcome) error {
	record.Outcome = &outcome
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode input outcome: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rff_inputs SET record = ? WHERE id = ?`, data, record.ID); err != nil {
		return fmt.Errorf("persist input outcome: %w", err)
	}
	return nil
}

// AcceptInput atomically records or reconciles an input by its global ID.
func (s *Store[EntityID, StateID]) AcceptInput(ctx context.Context, id string,
	decide func(*transition.InputRecord) (*transition.InputRecord, error),
) (record transition.InputRecord, created bool, err error) {
	if id == "" || decide == nil {
		return record, false, transition.ErrInvalidRequest
	}
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_inputs WHERE id = ?`, id).Scan(&data)
		var existing *transition.InputRecord
		if err == nil {
			decoded, err := decodeInput(id, data)
			if err != nil {
				return err
			}
			existing = &decoded
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read input: %w", err)
		}
		candidate, err := decide(existing)
		if err != nil {
			return err
		}
		if existing != nil {
			if candidate != nil {
				return transition.ErrInvalidDecision
			}
			record, err = decodeInput(id, data)
			return err
		}
		if candidate == nil || candidate.Outcome != nil {
			return transition.ErrInvalidDecision
		}
		data, err = json.Marshal(candidate)
		if err != nil {
			return fmt.Errorf("encode input: %w", err)
		}
		record, err = decodeInput(id, data)
		if err != nil {
			return transition.ErrInvalidDecision
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_inputs (id, record) VALUES (?, ?)`, id, data); err != nil {
			return fmt.Errorf("persist input: %w", err)
		}
		created = true
		return nil
	})
	if err != nil {
		return transition.InputRecord{}, false, err
	}
	return record, created, nil
}

// ReadInput reads a retained input and its outcome.
func (s *Store[EntityID, StateID]) ReadInput(ctx context.Context, id string) (transition.InputRecord, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT record FROM rff_inputs WHERE id = ?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.InputRecord{}, transition.ErrInvalidRequest
	}
	if err != nil {
		return transition.InputRecord{}, fmt.Errorf("read input: %w", err)
	}
	return decodeInput(id, data)
}

// PendingInputIDs lists unfinished inputs for a handler channel.
func (s *Store[EntityID, StateID]) PendingInputIDs(ctx context.Context, channel string) (ids []string, err error) {
	if channel == "" {
		return nil, transition.ErrInvalidRequest
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, record FROM rff_inputs ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list inputs: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close input query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, fmt.Errorf("read input: %w", err)
		}
		record, err := decodeInput(id, data)
		if err != nil {
			return nil, err
		}
		if record.Channel == channel && record.Outcome == nil {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list inputs: %w", err)
	}
	return ids, nil
}

// CompleteInput records the first host outcome and rejects conflicting reports.
func (s *Store[EntityID, StateID]) CompleteInput(ctx context.Context, id string, outcome transition.InputOutcome) (record transition.InputRecord, err error) {
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_inputs WHERE id = ?`, id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return transition.ErrInvalidRequest
		}
		if err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		record, err = decodeInput(id, data)
		if err != nil {
			return err
		}
		if record.Outcome != nil {
			prior := record.Outcome
			if prior.Receipt != outcome.Receipt || (prior.Failure == nil) != (outcome.Failure == nil) ||
				prior.Failure != nil && *prior.Failure != *outcome.Failure {
				return transition.ErrConflict
			}
			return nil
		}
		record.Outcome = &outcome
		data, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode input outcome: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_inputs SET record = ? WHERE id = ?`, data, id); err != nil {
			return fmt.Errorf("persist input outcome: %w", err)
		}
		record, err = decodeInput(id, data)
		return err
	})
	if err != nil {
		return transition.InputRecord{}, err
	}
	return record, nil
}

func validateInputs(ctx context.Context, tx *sql.Tx) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, record FROM rff_inputs`)
	if err != nil {
		return fmt.Errorf("%w: read inputs: %w", ErrUnsupportedSchema, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close input query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return fmt.Errorf("%w: read input: %w", ErrUnsupportedSchema, err)
		}
		if _, err := decodeInput(id, data); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read inputs: %w", ErrUnsupportedSchema, err)
	}
	return nil
}
