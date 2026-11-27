package sqlite

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

func decodeInput(id string, data []byte) (transition.InputRecord, error) {
	var record transition.InputRecord
	if err := json.Unmarshal(data, &record); err != nil || record.SchemaVersion != 1 || record.ID != id ||
		transition.ValidateDurableID(record.ID) != nil || record.Channel == "" || record.WorkflowID == "" ||
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
	if _, err := tx.ExecContext(ctx, `UPDATE rff_inputs SET record = ?, outcome_present = 1 WHERE id = ?`, data, record.ID); err != nil {
		return fmt.Errorf("persist input outcome: %w", err)
	}
	return nil
}

type inputFindingError struct{}

func (inputFindingError) Error() string { return transition.ErrIntegrity.Error() }
func (inputFindingError) Unwrap() error { return transition.ErrIntegrity }

func recordInputFinding(ctx context.Context, tx *sql.Tx, locator, revision int64, channel []byte, field string) error {
	if locator <= 0 || revision <= 0 || (field != "record" && field != "projection") {
		return fmt.Errorf("%w: invalid input locator", ErrUnsupportedSchema)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at, resolved_at)
		SELECT 'input', ?, ?, 'invalid_input', ?, ?, ?, NULL WHERE NOT EXISTS
		(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
		 WHERE kind = 'input' AND locator = ? AND resolved_at IS NULL)`,
		locator, revision, field, channel, time.Now().UTC().Format(time.RFC3339Nano), locator); err != nil {
		return fmt.Errorf("persist input integrity finding: %w", err)
	}
	return inputFindingError{}
}

func inputQuarantined(ctx context.Context, tx *sql.Tx, locator int64) (bool, error) {
	_, _, quarantined, err := inputFinding(ctx, tx, locator)
	return quarantined, err
}

func inputFinding(ctx context.Context, tx *sql.Tx, locator int64) ([]byte, int64, bool, error) {
	var channel []byte
	var revision int64
	var quarantined bool
	err := tx.QueryRowContext(ctx, `SELECT scope_channel, finding_revision FROM rff_integrity_findings INDEXED BY rff_integrity_open_lookup
		WHERE kind = 'input' AND locator = ? AND resolved_at IS NULL`, locator).Scan(&channel, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	quarantined = true
	return channel, revision, quarantined, nil
}

func loadInputByID(ctx context.Context, tx *sql.Tx, id string) (transition.InputRecord, int64, int64, error) {
	var locator, revision, outcomePresent int64
	var data, channel []byte
	err := tx.QueryRowContext(ctx, `SELECT row_id, record, channel, outcome_present, revision FROM rff_inputs WHERE id = ?`, id).
		Scan(&locator, &data, &channel, &outcomePresent, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.InputRecord{}, 0, 0, transition.ErrInvalidRequest
	}
	if err != nil {
		return transition.InputRecord{}, 0, 0, fmt.Errorf("read input: %w", err)
	}
	if locator <= 0 || revision <= 0 || (outcomePresent != 0 && outcomePresent != 1) {
		return transition.InputRecord{}, 0, 0, ErrUnsupportedSchema
	}
	quarantined, err := inputQuarantined(ctx, tx, locator)
	if err != nil {
		return transition.InputRecord{}, 0, 0, err
	}
	if quarantined {
		return transition.InputRecord{}, locator, revision, transition.ErrIntegrity
	}
	record, err := decodeInput(id, data)
	if err != nil {
		return transition.InputRecord{}, 0, 0, recordInputFinding(ctx, tx, locator, revision, []byte{}, "record")
	}
	expectedOutcome := int64(0)
	if record.Outcome != nil {
		expectedOutcome = 1
	}
	if !bytes.Equal(channel, []byte(record.Channel)) || outcomePresent != expectedOutcome {
		return transition.InputRecord{}, 0, 0, recordInputFinding(ctx, tx, locator, revision, []byte(record.Channel), "projection")
	}
	return record, locator, revision, nil
}

func (s *Store[EntityID, StateID]) inputTransaction(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin input transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback input transaction: %w", rollbackErr))
		}
	}()
	var orphan bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings f INDEXED BY rff_integrity_open_lookup
		WHERE f.kind = 'input' AND f.resolved_at IS NULL
		AND NOT EXISTS (SELECT 1 FROM rff_inputs i WHERE i.row_id = f.locator))`).Scan(&orphan); err != nil {
		return fmt.Errorf("check input quarantine: %w", err)
	}
	if orphan {
		return transition.ErrIntegrity
	}
	result := fn(tx)
	var detected inputFindingError
	if result != nil && !errors.As(result, &detected) {
		return result
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit input transaction: %w", err)
	}
	return result
}

// AcceptInput atomically records or reconciles an input by its global ID.
func (s *Store[EntityID, StateID]) AcceptInput(ctx context.Context, id string,
	decide func(*transition.InputRecord) (*transition.InputRecord, error),
) (record transition.InputRecord, created bool, err error) {
	if err := transition.ValidateDurableID(id); err != nil {
		return record, false, err
	}
	if decide == nil {
		return record, false, transition.ErrInvalidRequest
	}
	err = s.inputTransaction(ctx, func(tx *sql.Tx) error {
		var locator int64
		err := tx.QueryRowContext(ctx, `SELECT row_id FROM rff_inputs WHERE id = ?`, id).Scan(&locator)
		var existing *transition.InputRecord
		if err == nil {
			decoded, _, _, err := loadInputByID(ctx, tx, id)
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
			record, _, _, err = loadInputByID(ctx, tx, id)
			return err
		}
		if candidate == nil || candidate.Outcome != nil {
			return transition.ErrInvalidDecision
		}
		data, err := json.Marshal(candidate)
		if err != nil {
			return fmt.Errorf("encode input: %w", err)
		}
		record, err = decodeInput(id, data)
		if err != nil {
			return transition.ErrInvalidDecision
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_inputs (id, record, channel, outcome_present) VALUES (?, ?, ?, 0)`,
			id, data, []byte(record.Channel)); err != nil {
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
	var record transition.InputRecord
	err := s.inputTransaction(ctx, func(tx *sql.Tx) error {
		var err error
		record, _, _, err = loadInputByID(ctx, tx, id)
		return err
	})
	return record, err
}

// PendingInputIDs lists unfinished inputs for a handler channel.
func (s *Store[EntityID, StateID]) PendingInputIDs(ctx context.Context, channel string) (ids []string, err error) {
	if channel == "" {
		return nil, transition.ErrInvalidRequest
	}
	err = s.inputTransaction(ctx, func(tx *sql.Tx) error {
		var unknown bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_input_channel_lookup
			WHERE kind = 'input' AND scope_channel = X'' AND resolved_at IS NULL)`).Scan(&unknown); err != nil {
			return fmt.Errorf("check unknown-channel input findings: %w", err)
		}
		if unknown {
			return transition.ErrIntegrity
		}
		var quarantined bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_findings INDEXED BY rff_integrity_input_channel_lookup
			WHERE kind = 'input' AND scope_channel = ? AND resolved_at IS NULL)`, []byte(channel)).Scan(&quarantined); err != nil {
			return fmt.Errorf("check channel input findings: %w", err)
		}
		if quarantined {
			return transition.ErrIntegrity
		}
		rows, err := tx.QueryContext(ctx, `SELECT row_id, id, record, channel, outcome_present, revision
			FROM rff_inputs INDEXED BY rff_input_selection_lookup WHERE channel = ? AND outcome_present = 0 ORDER BY id`, []byte(channel))
		if err != nil {
			return fmt.Errorf("list inputs: %w", err)
		}
		type candidate struct {
			locator, outcomePresent, revision int64
			id                                string
			data, projectedChannel            []byte
		}
		var candidates []candidate
		for rows.Next() {
			var item candidate
			if err := rows.Scan(&item.locator, &item.id, &item.data, &item.projectedChannel, &item.outcomePresent, &item.revision); err != nil {
				return errors.Join(fmt.Errorf("read input candidate: %w", err), rows.Close())
			}
			candidates = append(candidates, item)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return fmt.Errorf("list inputs: %w", err)
		}
		ids = make([]string, 0, len(candidates))
		for _, item := range candidates {
			if item.locator <= 0 || item.revision <= 0 || item.outcomePresent != 0 {
				return ErrUnsupportedSchema
			}
			findingChannel, findingRevision, quarantined, err := inputFinding(ctx, tx, item.locator)
			if err != nil {
				return err
			}
			if quarantined {
				if findingRevision != item.revision || len(findingChannel) == 0 || bytes.Equal(findingChannel, []byte(channel)) {
					return transition.ErrIntegrity
				}
				continue
			}
			record, err := decodeInput(item.id, item.data)
			if err != nil {
				return recordInputFinding(ctx, tx, item.locator, item.revision, []byte{}, "record")
			}
			if !bytes.Equal(item.projectedChannel, []byte(record.Channel)) || record.Outcome != nil {
				return recordInputFinding(ctx, tx, item.locator, item.revision, []byte(record.Channel), "projection")
			}
			ids = append(ids, item.id)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, transition.ErrIntegrity) {
			return nil, fmt.Errorf("%w: %w", transition.ErrIncompleteInputList, err)
		}
		return nil, err
	}
	return ids, nil
}

// CompleteInput records the first host outcome and rejects conflicting reports.
func (s *Store[EntityID, StateID]) CompleteInput(ctx context.Context, id string, outcome transition.InputOutcome) (record transition.InputRecord, err error) {
	if err := transition.ValidateDurableID(id); err != nil {
		return record, err
	}
	err = s.inputTransaction(ctx, func(tx *sql.Tx) error {
		var locator int64
		record, locator, _, err = loadInputByID(ctx, tx, id)
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
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode input outcome: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_inputs SET record = ?, outcome_present = 1 WHERE row_id = ?`, data, locator); err != nil {
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
	rows, err := tx.QueryContext(ctx, `SELECT row_id, id, record, channel, outcome_present, revision FROM rff_inputs`)
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
		var locator, outcomePresent, revision int64
		var data, channel []byte
		if err := rows.Scan(&locator, &id, &data, &channel, &outcomePresent, &revision); err != nil {
			return fmt.Errorf("%w: read input: %w", ErrUnsupportedSchema, err)
		}
		if locator <= 0 || revision <= 0 || (outcomePresent != 0 && outcomePresent != 1) {
			return fmt.Errorf("%w: invalid input projection structure", ErrUnsupportedSchema)
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
