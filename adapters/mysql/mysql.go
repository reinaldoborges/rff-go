// Package mysql provides a durable MySQL acceptance store.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	driver "github.com/go-sql-driver/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// ErrUnsupportedSchema indicates an unreadable or unsupported adapter schema.
var ErrUnsupportedSchema = errors.New("unsupported MySQL transition schema")

// ErrUnsupportedServerVersion indicates that the connected MySQL server is too old.
var ErrUnsupportedServerVersion = errors.New("unsupported MySQL server version")

// Store persists transition acceptance in a MySQL database.
type Store[EntityID comparable, StateID comparable] struct {
	db          *sql.DB
	entityCodec Codec[EntityID]
	stateCodec  Codec[StateID]
}

// Open initializes or opens a MySQL database using application identifier codecs.
func Open[EntityID comparable, StateID comparable](dsn string, entityCodec Codec[EntityID], stateCodec Codec[StateID]) (*Store[EntityID, StateID], error) {
	if dsn == "" || nilcheck.IsNil(entityCodec) || nilcheck.IsNil(stateCodec) {
		return nil, errors.New("MySQL DSN and identifier codecs are required")
	}
	config, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if config.DBName == "" {
		return nil, errors.New("MySQL DSN must select a database")
	}
	connector, err := driver.NewConnector(config)
	if err != nil {
		return nil, fmt.Errorf("open MySQL: %w", err)
	}
	db := sql.OpenDB(strictConnector{Connector: connector})
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	store := &Store[EntityID, StateID]{db: db, entityCodec: entityCodec, stateCodec: stateCodec}
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("connect MySQL: %w", err), db.Close())
	}
	if err := validateServerVersion(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := store.initialize(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return store, nil
}

func validateServerVersion(ctx context.Context, db *sql.DB) error {
	var version string
	if err := db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err != nil {
		return fmt.Errorf("read MySQL server version: %w", err)
	}
	if !supportedServerVersion(version) {
		return fmt.Errorf("%w: %s; MySQL 8.4 or later is required", ErrUnsupportedServerVersion, version)
	}
	return nil
}

func supportedServerVersion(version string) bool {
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return false
	}
	var major, minor int
	count, _ := fmt.Sscanf(version, "%d.%d", &major, &minor)
	return count == 2 && major >= 8 && (major > 8 || minor >= 4)
}

// Close releases the MySQL connection pool.
func (s *Store[EntityID, StateID]) Close() error {
	return s.db.Close()
}

// beginTx takes the adapter-wide InnoDB lock before any adapter transaction.
// This mirrors SQLite's immediate write transaction and gives all processes
// one serialization point for acceptance, lease fencing, and protocol updates.
func (s *Store[EntityID, StateID]) beginTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	var lockID int
	if err := tx.QueryRowContext(ctx, `SELECT id FROM rff_transaction_lock WHERE id = 1 FOR UPDATE`).Scan(&lockID); err != nil {
		return nil, errors.Join(fmt.Errorf("lock MySQL adapter transaction: %w", err), tx.Rollback())
	}
	return tx, nil
}

// Accept implements the atomic acceptance-store contract.
func (s *Store[EntityID, StateID]) Accept(
	ctx context.Context, entity EntityID, key string, decide transition.AcceptanceDecisionFunc[EntityID, StateID],
) (result transition.Execution[EntityID, StateID], created bool, err error) {
	if key == "" || decide == nil {
		return result, false, transition.ErrInvalidDecision
	}
	return s.AcceptAtomic(ctx, entity, key, func(_ context.Context, _ *sql.Tx, view transition.AcceptanceView[EntityID, StateID]) (*transition.Execution[EntityID, StateID], error) {
		return decide(view)
	})
}

// AcceptAtomic lets the host authorize and write application-owned data in the
// same transaction as acceptance. The callback must use only tx, never commit,
// roll back, retain it, re-enter the store, or modify adapter tables. Replays
// must be read-only; callback errors and invalid decisions roll back host writes.
func (s *Store[EntityID, StateID]) AcceptAtomic(
	ctx context.Context, entity EntityID, key string,
	decide func(context.Context, *sql.Tx, transition.AcceptanceView[EntityID, StateID]) (*transition.Execution[EntityID, StateID], error),
) (result transition.Execution[EntityID, StateID], created bool, err error) {
	return s.acceptAtomic(ctx, entity, key, decide, nil)
}

type inputAcceptance struct {
	classify func(error) *fsm.Failure
	outcome  *transition.InputOutcome
	record   transition.InputRecord
}

// AcceptInputAtomic records an input outcome at the same boundary as execution
// acceptance or a classified definitive rejection. The input must already exist.
// The classifier returns nil for transient or unknown failures. The callback
// has the same transaction restrictions as AcceptAtomic; its writes are rolled
// back on rejection. Callers must validate the input identity before invocation.
func (s *Store[EntityID, StateID]) AcceptInputAtomic(
	ctx context.Context, entity EntityID, inputID string,
	decide func(context.Context, *sql.Tx, transition.AcceptanceView[EntityID, StateID]) (*transition.Execution[EntityID, StateID], error),
	classify func(error) *fsm.Failure,
) (transition.Execution[EntityID, StateID], bool, *transition.InputOutcome, error) {
	if err := transition.ValidateDurableID(inputID); err != nil {
		return transition.Execution[EntityID, StateID]{}, false, nil, err
	}
	if classify == nil {
		return transition.Execution[EntityID, StateID]{}, false, nil, transition.ErrInvalidRequest
	}
	input := &inputAcceptance{classify: classify}
	result, created, err := s.acceptAtomic(ctx, entity, inputID, decide, input)
	return result, created, input.outcome, err
}

func (s *Store[EntityID, StateID]) acceptAtomic(
	ctx context.Context, entity EntityID, key string,
	decide func(context.Context, *sql.Tx, transition.AcceptanceView[EntityID, StateID]) (*transition.Execution[EntityID, StateID], error),
	input *inputAcceptance,
) (result transition.Execution[EntityID, StateID], created bool, err error) {
	if key == "" || decide == nil {
		return result, false, transition.ErrInvalidDecision
	}
	encodedEntity, err := s.encodeEntity(entity)
	if err != nil {
		return result, false, err
	}
	tx, err := s.beginTx(ctx)
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
	if input != nil {
		var inputData []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_inputs WHERE id = ?`, key).Scan(&inputData); err != nil {
			return result, false, fmt.Errorf("read accepted input: %w", err)
		}
		input.record, err = decodeInput(key, inputData)
		if err != nil {
			return result, false, err
		}
		if input.record.Outcome != nil {
			input.outcome = input.record.Outcome
			if input.outcome.Failure == nil {
				if existing == nil || existing.ID != input.outcome.Receipt {
					return result, false, transition.ErrInvalidDecision
				}
				result = *existing
			}
			if err := tx.Commit(); err != nil {
				return result, false, fmt.Errorf("commit input replay: %w", err)
			}
			return result, false, nil
		}
		if _, err := tx.ExecContext(ctx, `SAVEPOINT rff_input_decision`); err != nil {
			return result, false, fmt.Errorf("start input decision: %w", err)
		}
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE entity = ? UNION ALL SELECT 1 FROM rff_successors WHERE entity = ? AND phase IN ('prepared', 'denied'))`, encodedEntity, encodedEntity).Scan(&active); err != nil {
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
	next, err := decide(ctx, tx, transition.AcceptanceView[EntityID, StateID]{Existing: viewExisting, Active: active})
	if err != nil {
		if errors.Is(err, transition.ErrInvalidDurableID) {
			return result, false, err
		}
		if input != nil {
			failure := input.classify(err)
			if failure != nil {
				if _, rollbackErr := tx.ExecContext(ctx, `ROLLBACK TO rff_input_decision`); rollbackErr != nil {
					return result, false, fmt.Errorf("rollback rejected input: %w", rollbackErr)
				}
				if _, releaseErr := tx.ExecContext(ctx, `RELEASE rff_input_decision`); releaseErr != nil {
					return result, false, fmt.Errorf("release input decision: %w", releaseErr)
				}
				input.outcome = &transition.InputOutcome{Failure: failure}
				if saveErr := saveInputOutcome(ctx, tx, &input.record, *input.outcome); saveErr != nil {
					return result, false, saveErr
				}
				if commitErr := tx.Commit(); commitErr != nil {
					return result, false, fmt.Errorf("commit input rejection: %w", commitErr)
				}
				return result, false, nil
			}
		}
		return result, false, err
	}
	if input != nil {
		if _, err := tx.ExecContext(ctx, `RELEASE rff_input_decision`); err != nil {
			return result, false, fmt.Errorf("release input decision: %w", err)
		}
	}
	if existing != nil {
		if next != nil {
			return result, false, transition.ErrInvalidDecision
		}
		if input != nil {
			input.outcome = &transition.InputOutcome{Receipt: existing.ID}
			if err := saveInputOutcome(ctx, tx, &input.record, *input.outcome); err != nil {
				return result, false, err
			}
		}
		if err := tx.Commit(); err != nil {
			return result, false, fmt.Errorf("commit replay: %w", err)
		}
		return *existing, false, nil
	}
	if next != nil {
		if err := transition.ValidateDurableID(next.ID); err != nil {
			return result, false, err
		}
	}
	if next == nil || active || next.Entity != entity || next.Key != key ||
		next.SchemaVersion != transition.CurrentSchemaVersion || next.Status != transition.Waiting ||
		next.CommitOperationID != "" || next.CommitReceipt != nil || next.NotificationIntentID != "" || next.NotificationFailure != nil ||
		next.NotificationWaived || len(next.NotificationControls) != 0 ||
		next.Compatibility != nil || next.OriginalRequest != nil || len(next.Migrations) != 0 ||
		next.RecoveryStarted || next.Failure != nil || next.FailureDisposition != "" ||
		!next.CycleDeadline.IsZero() || !next.RejectionRecoveryDeadline.IsZero() || len(next.Controls) != 0 {
		return result, false, transition.ErrInvalidDecision
	}
	if err := transition.ValidateContinuationSnapshot(next.Continuation); err != nil ||
		next.Continuation != nil && (next.Continuation.MachineID != next.MachineID || next.Continuation.MachineVersion != next.MachineVersion) {
		return result, false, transition.ErrInvalidDecision
	}
	var idExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_executions WHERE id = ? UNION ALL SELECT 1 FROM rff_successors WHERE execution_id = ?)`, next.ID, []byte(next.ID)).Scan(&idExists); err != nil {
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
	if input != nil {
		input.outcome = &transition.InputOutcome{Receipt: result.ID}
		if err := saveInputOutcome(ctx, tx, &input.record, *input.outcome); err != nil {
			return result, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return transition.Execution[EntityID, StateID]{}, false, fmt.Errorf("commit acceptance: %w", err)
	}
	return result, true, nil
}
