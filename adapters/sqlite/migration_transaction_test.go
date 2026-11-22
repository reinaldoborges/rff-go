package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestMigrationSerializesSameFileDomainWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "migration-domain.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.ExecContext(ctx, `CREATE TABLE domain_state (state TEXT NOT NULL);
		INSERT INTO domain_state VALUES ('open')`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			MachineID: "machine", MachineVersion: "1", Operation: transition.Transition,
			Previous: "open", HasPrevious: true, Target: "done", Status: transition.Waiting,
			AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	verified := make(chan error, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := store.ApplyExecutionMigration(ctx, lease, clock,
			transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"},
			func(_ context.Context, source transition.MigrationSource[string]) (transition.MigrationSnapshot[string], error) {
				return transition.MigrationSnapshot[string]{
					MachineID: "machine", MachineVersion: "2", Previous: "open", HasPrevious: true,
					Target: "done", Input: source.Input, Event: source.Event, Policy: source.Policy,
				}, nil
			},
			func(ctx context.Context, _ transition.MigrationSource[string], snapshot transition.MigrationSnapshot[string], _ transition.Execution[string, string], _ transition.HookCall) error {
				var state string
				err := db.QueryRowContext(ctx, `SELECT state FROM domain_state`).Scan(&state)
				if err == nil && state != snapshot.Previous {
					err = fmt.Errorf("domain source changed: %s", state)
				}
				verified <- err
				<-release
				return err
			})
		done <- err
	}()
	var verifyErr error
	select {
	case verifyErr = <-verified:
	case err := <-done:
		t.Fatalf("migration ended before verification: %v", err)
	}
	_, writeErr := db.ExecContext(ctx, `UPDATE domain_state SET state = 'changed'`)
	close(release)
	migrationErr := <-done
	var sqliteErr sqlite3.Error
	if verifyErr != nil || !errors.As(writeErr, &sqliteErr) || sqliteErr.Code != sqlite3.ErrBusy || migrationErr != nil {
		t.Fatalf("domain verification did not serialize migration: verify=%v write=%v migration=%v", verifyErr, writeErr, migrationErr)
	}
	if _, err := db.ExecContext(ctx, `UPDATE domain_state SET state = 'changed'`); err != nil {
		t.Fatalf("domain write remained blocked after migration: %v", err)
	}
}

func TestMigrationRollsBackFencedHookWhenSnapshotWriteFails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "migration-rollback.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := fixedClock(now)
	_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
			MachineID: "machine", MachineVersion: "1", Operation: transition.Initialize, Target: "open",
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_migration_snapshot BEFORE UPDATE ON rff_executions
		BEGIN SELECT RAISE(ABORT, 'injected snapshot failure'); END`); err != nil {
		t.Fatal(err)
	}
	request := transition.MigrationRequest{ID: "migration", MachineID: "machine", MachineVersion: "2"}
	convert := func(_ context.Context, source transition.MigrationSource[string]) (transition.MigrationSnapshot[string], error) {
		return transition.MigrationSnapshot[string]{
			MachineID: "machine", MachineVersion: "2", Target: "open",
			Input: source.Input, Event: source.Event, Policy: source.Policy,
		}, nil
	}
	validate := func(context.Context, transition.MigrationSource[string], transition.MigrationSnapshot[string], transition.Execution[string, string], transition.HookCall) error {
		return nil
	}
	if _, err := store.ApplyExecutionMigration(ctx, lease, clock, request, convert, validate); err == nil || !strings.Contains(err.Error(), "injected snapshot failure") {
		t.Fatalf("snapshot write failure was not reported: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.ReadActiveExecution(ctx, "execution")
	if err != nil || record.MachineVersion != "1" || len(record.Migrations) != 0 {
		t.Fatalf("failed migration persisted a partial snapshot: %+v, %v", record, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_migration_snapshot`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatalf("failed migration persisted the hook fence: %v", err)
	}
	if _, err := store.ApplyExecutionMigration(ctx, lease, clock, request, convert, validate); err != nil {
		t.Fatalf("migration could not be retried after rollback: %v", err)
	}
	progress, err := store.ReadHookProgress(ctx, "execution")
	if err != nil || len(progress.Attempts) != 1 || !progress.Attempts[0].Unknown || progress.BudgetUsed != 1 {
		t.Fatalf("successful retry did not fence the started attempt: %+v, %v", progress, err)
	}
}
