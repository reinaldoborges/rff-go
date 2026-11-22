package taskmanager

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// MigrateExecution upgrades pending task work to the fixture's second behavior version.
func (f *Fixture) MigrateExecution(ctx context.Context, actor, id, key string) (result error) {
	if id == "" || key == "" {
		return ErrInvalidRequest
	}
	if err := f.authorizeOperator(ctx, actor, string(transition.ControlMigrate), id); err != nil {
		return err
	}
	execution, err := f.store.ReadActiveExecution(ctx, id)
	if errors.Is(err, transition.ErrExecutionNotActive) {
		record, inspectErr := f.store.InspectExecution(ctx, id)
		if inspectErr != nil {
			return inspectErr
		}
		if record.MachineID == "taskmanager-task" {
			for _, prior := range record.Migrations {
				if prior.Request.ID == key {
					if prior.Request.MachineID == record.MachineID && prior.Request.MachineVersion == "2" &&
						prior.ParametersDigest == fmt.Sprintf("%x", sha256.Sum256(nil)) {
						return nil
					}
					return transition.ErrConflict
				}
			}
		}
	}
	if err != nil {
		return err
	}
	if execution.MachineID != "taskmanager-task" {
		return ErrInvalidRequest
	}
	// Reserve the fixture's single domain connection before taking SQLite's
	// write lock so a concurrent domain writer cannot starve verification.
	conn, err := f.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve migration domain connection: %w", err)
	}
	defer func() { result = errors.Join(result, conn.Close()) }()
	lease, err := f.store.AcquireTransitionLease(ctx, execution.Entity, id, "taskmanager", f.clock, time.Minute)
	if err != nil {
		return err
	}
	migrator, err := transition.NewExecutionMigrator(f.store, f.runner,
		func(_ context.Context, source transition.MigrationSource[string], request transition.MigrationRequest) (transition.MigrationSnapshot[string], error) {
			target, err := (sqlite.StringCodec{}).Decode(source.Target)
			if err != nil {
				return transition.MigrationSnapshot[string]{}, err
			}
			var previous string
			if source.HasPrevious {
				previous, err = (sqlite.StringCodec{}).Decode(source.Previous)
				if err != nil {
					return transition.MigrationSnapshot[string]{}, err
				}
			}
			return transition.MigrationSnapshot[string]{
				MachineID: request.MachineID, MachineVersion: request.MachineVersion,
				Previous: previous, HasPrevious: source.HasPrevious, Target: target,
				Input: source.Input, Event: source.Event, Policy: source.Policy,
			}, nil
		},
		func(ctx context.Context, source transition.MigrationSource[string], snapshot transition.MigrationSnapshot[string]) error {
			codec, err := resolveMembershipInput(source.Input.Codec)
			if err != nil {
				return err
			}
			input, err := codec.Decode(ctx, source.Input.Data)
			if err != nil {
				return err
			}
			if input.Task == nil {
				return ErrInvalidRequest
			}
			valid, err := validRuntimeEntity(ctx, conn, taskEntityKind, input.Task.ID, source.Entity)
			if err != nil {
				return fmt.Errorf("verify migration entity: %w", err)
			}
			if !valid {
				return ErrConflict
			}
			var group, status string
			err = conn.QueryRowContext(ctx, `SELECT group_id, status FROM tm_tasks WHERE id = ?`, input.Task.ID).Scan(&group, &status)
			if source.Operation == transition.Initialize && errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if errors.Is(err, sql.ErrNoRows) {
				return ErrConflict
			}
			if err != nil {
				return fmt.Errorf("verify migration task: %w", err)
			}
			if source.Operation != transition.Transition || !snapshot.HasPrevious ||
				group != input.GroupID || status != snapshot.Previous {
				return ErrConflict
			}
			return nil
		})
	if err != nil {
		return err
	}
	authorized, err := transition.NewAuthorizedMigration[string](migrator, func(ctx context.Context, _ transition.ControlAuthorization) error {
		var allowed bool
		if err := conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tm_operators WHERE id = ?)`, actor).Scan(&allowed); err != nil {
			return fmt.Errorf("reauthorize test operator: %w", err)
		}
		if !allowed {
			return ErrDenied
		}
		return nil
	})
	if err != nil {
		return err
	}
	_, err = authorized.Migrate(ctx, lease, f.clock, transition.MigrationRequest{
		ID: key, MachineID: execution.MachineID, MachineVersion: "2",
	})
	return err
}
