// Package fixturecontract checks task-manager behavior through its typed API.
package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

// Fixture is the domain-facing seam required by this contract.
type Fixture interface {
	SeedUsers(context.Context, ...string) error
	CreateGroup(context.Context, string, string, string) error
	Invite(context.Context, string, string, string, string, string, time.Time) (string, error)
	Decide(context.Context, string, string, string, taskmanager.InvitationDecision) (string, error)
	Group(context.Context, string, string) (taskmanager.Group, error)
	Membership(context.Context, string, string) (taskmanager.Membership, error)
	CreateTask(context.Context, string, string, string, string, string) (string, error)
	TransitionTask(context.Context, string, string, string, taskmanager.TaskEvent) (string, error)
	Task(context.Context, string, string) (taskmanager.Task, error)
	Drive(context.Context) error
}

// Run checks access, typed request replay and basic domain progression.
func Run(t *testing.T, newFixture func(*testing.T, func() time.Time) Fixture) {
	t.Helper()
	t.Run("invitation and access", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		if err := fixture.SeedUsers(ctx, "owner", "invitee", "outsider"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Invite(ctx, "outsider", "group", "invitee", "membership", "denied", now.Add(time.Hour)); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("outsider invited a user: %v", err)
		}
		id, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour))
		if err != nil || id == "" {
			t.Fatalf("invitation was not accepted: %q, %v", id, err)
		}
		if _, err := fixture.Membership(ctx, "owner", "membership"); !errors.Is(err, taskmanager.ErrNotFound) {
			t.Fatalf("membership committed before work: %v", err)
		}
		replayed, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour))
		if err != nil || replayed != id {
			t.Fatalf("invitation replay changed execution: %q, %v", replayed, err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		invited, err := fixture.Membership(ctx, "owner", "membership")
		if err != nil || invited.Status != taskmanager.Invited || !invited.Deadline.Equal(now.Add(time.Hour)) {
			t.Fatalf("invitation was not committed: %+v, %v", invited, err)
		}
		if _, err := fixture.Group(ctx, "invitee", "group"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("invited user gained access: %v", err)
		}
		if _, err := fixture.Decide(ctx, "outsider", "membership", "denied", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("outsider decided invitation: %v", err)
		}
		decision, err := fixture.Decide(ctx, "invitee", "membership", "decision", taskmanager.AcceptInvitation)
		if err != nil || decision == "" {
			t.Fatalf("acceptance was not accepted: %q, %v", decision, err)
		}
		if _, err := fixture.Group(ctx, "invitee", "group"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("acceptance granted access before commit: %v", err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		group, err := fixture.Group(ctx, "invitee", "group")
		if err != nil || group.ID != "group" {
			t.Fatalf("active member has no group access: %+v, %v", group, err)
		}
		replayed, err = fixture.Decide(ctx, "invitee", "membership", "decision", taskmanager.AcceptInvitation)
		if err != nil || replayed != decision {
			t.Fatalf("decision replay changed execution: %q, %v", replayed, err)
		}
		if _, err := fixture.Decide(ctx, "invitee", "membership", "decision", taskmanager.DeclineInvitation); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("changed decision replay did not conflict: %v", err)
		}
	})
	t.Run("task progression", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		if err := fixture.SeedUsers(ctx, "owner", "outsider"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.CreateTask(ctx, "outsider", "group", "task", "Task", "denied"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("outsider created task: %v", err)
		}
		if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		task, err := fixture.Task(ctx, "owner", "task")
		if err != nil || task.Status != taskmanager.TaskOpen {
			t.Fatalf("task was not initialized: %+v, %v", task, err)
		}
		if _, err := fixture.TransitionTask(ctx, "owner", "task", "invalid", taskmanager.CompleteTask); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("open task completed without starting: %v", err)
		}
		for _, step := range []struct {
			key    string
			event  taskmanager.TaskEvent
			status taskmanager.TaskStatus
		}{{"start", taskmanager.StartTask, taskmanager.TaskInProgress}, {"complete", taskmanager.CompleteTask, taskmanager.TaskDone}} {
			if _, err := fixture.TransitionTask(ctx, "owner", "task", step.key, step.event); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			task, err = fixture.Task(ctx, "owner", "task")
			if err != nil || task.Status != step.status {
				t.Fatalf("task did not progress to %s: %+v, %v", step.status, task, err)
			}
		}
		if _, err := fixture.TransitionTask(ctx, "owner", "task", "cancel", taskmanager.CancelTask); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("completed task was cancelled: %v", err)
		}
	})
}
