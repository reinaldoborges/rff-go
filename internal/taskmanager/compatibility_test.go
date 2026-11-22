package taskmanager_test

import (
	"context"
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
