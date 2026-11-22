package taskmanager_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestFixtureRevalidatesPreviouslyBlockedExecution(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "compatibility.db")
	fixture, err := taskmanager.Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if fixture != nil {
			if err := fixture.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	id, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, execution.Entity, id, "taskmanager", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BlockExecutionCompatibility(ctx, lease, clock, transition.CompatibilityDiagnostic{
		Component: transition.CompatibilityMachine, Name: execution.MachineID, Version: execution.MachineVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	fixture = nil
	fixture, err = taskmanager.Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Membership(ctx, "owner", "membership"); !errors.Is(err, taskmanager.ErrNotFound) {
		t.Fatalf("blocked invitation committed before recovery: %v", err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited {
		t.Fatalf("restored execution did not resume: membership=%+v error=%v", membership, err)
	}
}

func TestFixtureMigrationBlocksChangedTaskSource(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "migration-source.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.StartTask)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE tm_tasks SET status = 'Done' WHERE id = 'task'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.MigrateExecution(ctx, "operator", id, "migration"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("migration accepted a changed task source: %v", err)
	}
	record, err := fixture.InspectExecution(ctx, "operator", id)
	if err != nil || record.MachineVersion != "1" || len(record.Migrations) != 0 ||
		record.Compatibility == nil || record.Compatibility.Component != transition.CompatibilityMigration {
		t.Fatalf("changed source did not block the original execution: %+v, %v", record, err)
	}
	task, err := fixture.Task(ctx, "owner", "task")
	if err != nil || task.Status != taskmanager.TaskDone {
		t.Fatalf("migration changed the authoritative task: %+v, %v", task, err)
	}
}

func TestFixtureMigratesPendingTask(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "migration.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	initializationID, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.MigrateExecution(ctx, "operator", initializationID, "initialization-migration"); err != nil {
		t.Fatalf("migrate pending initialization: %v", err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := fixture.TransitionTask(ctx, "owner", "task", "start", taskmanager.StartTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.MigrateExecution(ctx, "owner", id, "migration"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("domain owner acquired migration control: %v", err)
	}
	if err := fixture.MigrateExecution(ctx, "operator", id, "migration"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.MigrateExecution(ctx, "operator", id, "migration"); err != nil {
		t.Fatalf("active migration did not replay: %v", err)
	}
	record, err := fixture.InspectExecution(ctx, "operator", id)
	if err != nil || record.MachineVersion != "2" || len(record.Migrations) != 1 {
		t.Fatalf("migration did not persist the new behavior: %+v, %v", record, err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	task, err := fixture.Task(ctx, "owner", "task")
	if err != nil || task.Status != taskmanager.TaskInProgress {
		t.Fatalf("migrated task did not progress: %+v, %v", task, err)
	}
	if err := fixture.MigrateExecution(ctx, "operator", id, "migration"); err != nil {
		t.Fatalf("completed migration did not replay: %v", err)
	}
	audit, err := fixture.OperatorAudit(ctx, "operator")
	if err != nil {
		t.Fatal(err)
	}
	var allowed, denied bool
	for _, entry := range audit {
		if entry.ExecutionID == id && entry.Action == string(transition.ControlMigrate) {
			allowed = allowed || entry.Actor == "operator" && entry.Allowed
			denied = denied || entry.Actor == "owner" && !entry.Allowed
		}
	}
	if !allowed || !denied {
		t.Fatalf("migration authorization was not audited: %+v", audit)
	}
}
