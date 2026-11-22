// Package sqlite provides a durable SQLite acceptance store.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ErrUnsupportedSchema indicates an unreadable or unsupported adapter schema.
var ErrUnsupportedSchema = errors.New("unsupported SQLite transition schema")

// Store persists transition acceptance in a SQLite file.
type Store[EntityID comparable, StateID comparable] struct {
	db          *sql.DB
	entityCodec Codec[EntityID]
	stateCodec  Codec[StateID]
}

// Open initializes or opens a durable file using application identifier codecs.
func Open[EntityID comparable, StateID comparable](path string, entityCodec Codec[EntityID], stateCodec Codec[StateID]) (*Store[EntityID, StateID], error) {
	if path == "" || path == ":memory:" || nilcheck.IsNil(entityCodec) || nilcheck.IsNil(stateCodec) {
		return nil, errors.New("SQLite file path and identifier codecs are required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite path: %w", err)
	}
	uri := url.URL{Scheme: "file", Path: absolute}
	query := uri.Query()
	query.Set("mode", "rwc")
	query.Set("_txlock", "immediate")
	query.Set("_busy_timeout", "5000")
	query.Set("_foreign_keys", "on")
	query.Set("_synchronous", "FULL")
	uri.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", uri.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &Store[EntityID, StateID]{db: db, entityCodec: entityCodec, stateCodec: stateCodec}
	if err := store.initialize(context.Background()); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return store, nil
}

// Close releases the SQLite connection.
func (s *Store[EntityID, StateID]) Close() error {
	return s.db.Close()
}

// Accept implements the atomic acceptance-store contract.
func (s *Store[EntityID, StateID]) Accept(
	ctx context.Context, entity EntityID, key string, decide transition.AcceptanceDecisionFunc[EntityID, StateID],
) (result transition.Execution[EntityID, StateID], created bool, err error) {
	if key == "" || decide == nil {
		return result, false, transition.ErrInvalidDecision
	}
	encodedEntity, err := s.encodeEntity(entity)
	if err != nil {
		return result, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, false, fmt.Errorf("begin acceptance transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback acceptance: %w", rollbackErr))
		}
	}()
	var data []byte
	rowErr := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE entity = ? AND request_key = ?`, encodedEntity, key).Scan(&data)
	var existing *transition.Execution[EntityID, StateID]
	if rowErr == nil {
		record, decodeErr := s.decodeRecord(data)
		if decodeErr != nil {
			return result, false, decodeErr
		}
		if record.Entity != entity || record.Key != key {
			return result, false, errors.New("stored execution identity does not match its index")
		}
		existing = &record
	} else if !errors.Is(rowErr, sql.ErrNoRows) {
		return result, false, fmt.Errorf("read execution: %w", rowErr)
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE entity = ?)`, encodedEntity).Scan(&active); err != nil {
		return result, false, fmt.Errorf("read reservation: %w", err)
	}
	var viewExisting *transition.Execution[EntityID, StateID]
	if existing != nil {
		copy, decodeErr := s.decodeRecord(data)
		if decodeErr != nil {
			return result, false, decodeErr
		}
		viewExisting = &copy
	}
	next, err := decide(transition.AcceptanceView[EntityID, StateID]{Existing: viewExisting, Active: active})
	if err != nil {
		return result, false, err
	}
	if existing != nil {
		if next != nil {
			return result, false, transition.ErrInvalidDecision
		}
		if err := tx.Commit(); err != nil {
			return result, false, fmt.Errorf("commit replay: %w", err)
		}
		return *existing, false, nil
	}
	if next == nil || active || next.Entity != entity || next.Key != key || next.ID == "" ||
		next.SchemaVersion != transition.CurrentSchemaVersion || next.Status != transition.Waiting ||
		next.CommitOperationID != "" || next.CommitReceipt != nil || next.NotificationIntentID != "" || next.NotificationFailure != nil ||
		next.NotificationWaived || len(next.NotificationControls) != 0 ||
		next.Compatibility != nil ||
		next.RecoveryStarted || next.Failure != nil || next.FailureDisposition != "" ||
		!next.CycleDeadline.IsZero() || !next.RejectionRecoveryDeadline.IsZero() || len(next.Controls) != 0 {
		return result, false, transition.ErrInvalidDecision
	}
	var idExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_executions WHERE id = ?)`, next.ID).Scan(&idExists); err != nil {
		return result, false, fmt.Errorf("check execution ID: %w", err)
	}
	if idExists {
		return result, false, transition.ErrInvalidDecision
	}
	encoded, err := s.encodeRecord(*next, encodedEntity)
	if err != nil {
		return result, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_executions (id, entity, request_key, record) VALUES (?, ?, ?, ?)`, next.ID, encodedEntity, key, encoded); err != nil {
		return result, false, fmt.Errorf("insert execution: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO rff_active (entity, execution_id) VALUES (?, ?)`, encodedEntity, next.ID); err != nil {
		return result, false, fmt.Errorf("reserve entity: %w", err)
	}
	result, err = s.decodeRecord(encoded)
	if err != nil {
		return result, false, err
	}
	if err := tx.Commit(); err != nil {
		return transition.Execution[EntityID, StateID]{}, false, fmt.Errorf("commit acceptance: %w", err)
	}
	return result, true, nil
}
