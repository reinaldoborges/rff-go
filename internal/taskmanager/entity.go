package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

const (
	membershipEntityKind = "membership"
	taskEntityKind       = "task"
)

// Seed reservations from older fixture databases before assigning new identities.
func (f *Fixture) reconcileRuntimeEntities(ctx context.Context) error {
	ids, err := f.store.ActiveExecutionIDs(ctx)
	if err != nil {
		return err
	}
	for _, executionID := range ids {
		execution, err := f.store.ReadActiveExecution(ctx, executionID)
		if errors.Is(err, transition.ErrExecutionNotActive) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read active fixture execution: %w", err)
		}
		var mapped bool
		if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_runtime_entities WHERE entity = ?)`,
			execution.Entity).Scan(&mapped); err != nil {
			return fmt.Errorf("read active fixture reservation: %w", err)
		}
		if mapped {
			continue
		}
		var kind, id string
		switch execution.MachineID {
		case "taskmanager-task":
			const prefix = "taskmanager-task:"
			if !strings.HasPrefix(execution.Entity, prefix) || len(execution.Entity) == len(prefix) {
				return fmt.Errorf("active task has no task identity: %s", executionID)
			}
			kind, id = taskEntityKind, strings.TrimPrefix(execution.Entity, prefix)
		case "taskmanager-membership":
			kind, id = membershipEntityKind, execution.Entity
		default:
			return fmt.Errorf("unknown active fixture machine: %s", execution.MachineID)
		}
		if err := f.transaction(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO tm_runtime_entities (kind, domain_id, entity)
				VALUES (?, ?, ?) ON CONFLICT(kind, domain_id) DO NOTHING`, kind, id, execution.Entity)
			if err != nil {
				return fmt.Errorf("reserve active fixture entity: %w", err)
			}
			var entity string
			if err := tx.QueryRowContext(ctx, `SELECT entity FROM tm_runtime_entities WHERE kind = ? AND domain_id = ?`,
				kind, id).Scan(&entity); err != nil {
				return err
			}
			if entity != execution.Entity {
				return fmt.Errorf("active fixture identity does not match its reservation: %s", executionID)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func legacyEntity(kind, id string) string {
	if kind == taskEntityKind {
		return taskEntity(id)
	}
	return id
}

func (f *Fixture) runtimeEntity(ctx context.Context, kind, id string) (string, error) {
	if id == "" || (kind != taskEntityKind && kind != membershipEntityKind) {
		return "", ErrInvalidRequest
	}
	var entity string
	err := f.transaction(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT entity FROM tm_runtime_entities WHERE kind = ? AND domain_id = ?`, kind, id).Scan(&entity)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read fixture entity: %w", err)
		}
		entity = legacyEntity(kind, id)
		for {
			var used bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_runtime_entities WHERE entity = ?)
				OR EXISTS(SELECT 1 FROM tm_memberships WHERE id = ? AND NOT (? = 'membership' AND id = ?))
				OR EXISTS(SELECT 1 FROM tm_tasks WHERE 'taskmanager-task:' || id = ? AND NOT (? = 'task' AND id = ?))`,
				entity, entity, kind, id, entity, kind, id).Scan(&used); err != nil {
				return fmt.Errorf("check fixture entity reservation: %w", err)
			}
			if !used {
				break
			}
			random, err := newExecutionID()
			if err != nil {
				return fmt.Errorf("allocate fixture entity: %w", err)
			}
			entity = "taskmanager-entity:" + random
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tm_runtime_entities (kind, domain_id, entity) VALUES (?, ?, ?)`,
			kind, id, entity); err != nil {
			return fmt.Errorf("reserve fixture entity: %w", err)
		}
		return nil
	})
	return entity, err
}

var errNoLegacyExecution = errors.New("no legacy execution")

// Pending inputs can outlive an old execution's active reservation.
func (f *Fixture) entityForInput(ctx context.Context, kind, id, key string) (string, error) {
	entity, err := f.runtimeEntity(ctx, kind, id)
	if err != nil {
		return "", err
	}
	legacy := legacyEntity(kind, id)
	if entity == legacy {
		return entity, nil
	}
	_, _, err = f.store.Accept(ctx, legacy, key, func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing == nil {
			return nil, errNoLegacyExecution
		}
		return nil, nil
	})
	if errors.Is(err, errNoLegacyExecution) {
		return entity, nil
	}
	if err != nil {
		return "", fmt.Errorf("inspect legacy fixture execution: %w", err)
	}
	return legacy, nil
}

func validRuntimeEntity(ctx context.Context, tx *sql.Tx, kind, id, entity string) (bool, error) {
	if entity == legacyEntity(kind, id) {
		return true, nil
	}
	var valid bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_runtime_entities WHERE kind = ? AND domain_id = ? AND entity = ?)`,
		kind, id, entity).Scan(&valid)
	return valid, err
}
