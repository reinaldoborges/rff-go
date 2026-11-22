package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

// GovernanceFixture adds assignment, removal and ownership operations.
type GovernanceFixture interface {
	MembershipFixture
	AssignTask(context.Context, string, string, string) error
	TaskAssignmentHistory(context.Context, string, string) ([]taskmanager.TaskAssignment, error)
	RemoveMember(context.Context, string, string, string) (string, error)
	InspectRemoval(context.Context, string, string) (taskmanager.RemovalAudit, error)
	TransferOwnership(context.Context, string, string, string, string) (taskmanager.OwnershipTransfer, error)
}

func activeMember(t *testing.T, fixture GovernanceFixture, now time.Time) {
	t.Helper()
	ctx := context.Background()
	if err := fixture.SeedUsers(ctx, "owner", "member", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "member", "member-membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "member", "member-membership", "accept", taskmanager.AcceptInvitation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
}

// RunGovernance checks assignment-aware removal and ownership transfer replay.
func RunGovernance(t *testing.T, newFixture func(*testing.T, func() time.Time) GovernanceFixture) {
	t.Helper()
	t.Run("task permissions require active membership", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		if _, err := fixture.Invite(ctx, "owner", "group", "outsider", "invited-membership", "invite-outsider", now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		for _, actor := range []string{"outsider", "operator"} {
			if _, err := fixture.CreateTask(ctx, actor, "group", "denied-task", "Task", "denied-"+actor); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("%s created a task without active membership: %v", actor, err)
			}
		}
		if _, err := fixture.CreateTask(ctx, "member", "group", "task", "Task", "create"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		for _, actor := range []string{"outsider", "operator"} {
			if _, err := fixture.TransitionTask(ctx, actor, "task", "denied-"+actor, taskmanager.StartTask); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("%s changed a task without active membership: %v", actor, err)
			}
		}
		task, err := fixture.Task(ctx, "member", "task")
		if err != nil || task.Status != taskmanager.TaskOpen {
			t.Fatalf("denied transitions changed the task: %+v, %v", task, err)
		}
		if _, err := fixture.TransitionTask(ctx, "member", "task", "start", taskmanager.StartTask); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		task, err = fixture.Task(ctx, "member", "task")
		if err != nil || task.Status != taskmanager.TaskInProgress {
			t.Fatalf("active member could not progress the task: %+v, %v", task, err)
		}
	})
	t.Run("assignment-aware removal", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if err := fixture.AssignTask(ctx, "member", "task", "member-membership"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.RemoveMember(ctx, "owner", "member-membership", "assigned"); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("member with assigned task was removed: %v", err)
		}
		if _, err := fixture.Group(ctx, "member", "group"); err != nil {
			t.Fatalf("rejected removal changed access: %v", err)
		}
		if err := fixture.AssignTask(ctx, "owner", "task", ""); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
		id, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove")
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		membership, err := fixture.InspectMembership(ctx, "operator", "member-membership")
		if err != nil || membership.Status != taskmanager.Removed {
			t.Fatalf("removed membership was not retained: %+v, %v", membership, err)
		}
		if _, err := fixture.Group(ctx, "member", "group"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("removed member retained access: %v", err)
		}
		replayed, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove")
		if err != nil || replayed != id {
			t.Fatalf("removal replay changed: %q, %v", replayed, err)
		}
		audit, err := fixture.InspectRemoval(ctx, "operator", "member-membership")
		if err != nil || audit.Actor != "owner" || audit.MembershipID != membership.ID || !audit.At.Equal(now) {
			t.Fatalf("removal audit was not retained: %+v, %v", audit, err)
		}
		history, err := fixture.TaskAssignmentHistory(ctx, "owner", "task")
		if err != nil || len(history) != 1 || history[0].MembershipID != membership.ID {
			t.Fatalf("removal lost task assignment history: %+v, %v", history, err)
		}
		if _, err := fixture.Invite(ctx, "owner", "group", "member", "new-membership", "reinvite", now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		retained, err := fixture.InspectMembership(ctx, "operator", "member-membership")
		if err != nil || retained != membership {
			t.Fatalf("reinvitation changed removed membership: %+v, %v", retained, err)
		}
	})
	t.Run("ownership transfer and replay", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		if _, err := fixture.TransferOwnership(ctx, "outsider", "group", "member", "denied"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("non-owner transferred ownership: %v", err)
		}
		if _, err := fixture.TransferOwnership(ctx, "owner", "group", "owner", "self"); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("self-transfer was accepted: %v", err)
		}
		if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create-task"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if err := fixture.AssignTask(ctx, "owner", "task", "member-membership"); err != nil {
			t.Fatal(err)
		}
		receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
		if err != nil || receipt.Actor != "owner" || receipt.FormerOwner != "owner" || receipt.NewOwner != "member" || !receipt.At.Equal(now) {
			t.Fatalf("transfer audit was not recorded: %+v, %v", receipt, err)
		}
		group, err := fixture.Group(ctx, "owner", "group")
		if err != nil || group.Owner != "member" {
			t.Fatalf("transfer did not preserve former owner membership: %+v, %v", group, err)
		}
		task, err := fixture.Task(ctx, "member", "task")
		if err != nil || task.Assignee != "member-membership" {
			t.Fatalf("transfer changed task assignment: %+v, %v", task, err)
		}
		for _, id := range []string{"owner-membership", "member-membership"} {
			membership, err := fixture.InspectMembership(ctx, "operator", id)
			if err != nil || membership.Status != taskmanager.Active {
				t.Fatalf("transfer changed active membership %q: %+v, %v", id, membership, err)
			}
		}
		if _, err := fixture.RemoveMember(ctx, "member", "owner-membership", "remove"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Group(ctx, "owner", "group"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("removed former owner retained access: %v", err)
		}
		if err := fixture.CloseGroup(ctx, "member", "group"); err != nil {
			t.Fatal(err)
		}
		replayed, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
		if err != nil || replayed != receipt {
			t.Fatalf("original actor could not replay after removal and closure: %+v, %v", replayed, err)
		}
		if _, err := fixture.TransferOwnership(ctx, "owner", "group", "outsider", "transfer"); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("changed transfer replay did not conflict: %v", err)
		}
	})
	t.Run("assignment requires an active same-group membership", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		if err := fixture.SeedUsers(ctx, "invitee"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.CreateGroup(ctx, "outsider", "other-group", "outsider-membership"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "invited-membership", "invite-new", now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if err := fixture.AssignTask(ctx, "outsider", "task", "member-membership"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("outsider assigned a task: %v", err)
		}
		for _, membership := range []string{"invited-membership", "outsider-membership"} {
			if err := fixture.AssignTask(ctx, "member", "task", membership); !errors.Is(err, taskmanager.ErrConflict) {
				t.Fatalf("invalid assignee %q was accepted: %v", membership, err)
			}
		}
		history, err := fixture.TaskAssignmentHistory(ctx, "owner", "task")
		if err != nil || len(history) != 0 {
			t.Fatalf("denied assignments created history: %+v, %v", history, err)
		}
		if err := fixture.AssignTask(ctx, "member", "task", "member-membership"); err != nil {
			t.Fatal(err)
		}
		task, err := fixture.Task(ctx, "owner", "task")
		if err != nil || task.Assignee != "member-membership" {
			t.Fatalf("active same-group assignment was not retained: %+v, %v", task, err)
		}
	})
	t.Run("invalid recipients preserve ownership", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		if err := fixture.SeedUsers(ctx, "invitee"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.CreateGroup(ctx, "outsider", "other-group", "outsider-membership"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "invited-membership", "invite-new", now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			recipient, key string
		}{{"invitee", "invited"}, {"outsider", "cross-group"}, {"owner", "self"}} {
			receipt, err := fixture.TransferOwnership(ctx, "owner", "group", test.recipient, test.key)
			if !errors.Is(err, taskmanager.ErrConflict) || receipt != (taskmanager.OwnershipTransfer{}) {
				t.Fatalf("invalid recipient %q produced a transfer: %+v, %v", test.recipient, receipt, err)
			}
		}
		if _, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "removed"); !errors.Is(err, taskmanager.ErrConflict) ||
			receipt != (taskmanager.OwnershipTransfer{}) {
			t.Fatalf("removed recipient produced a transfer: %+v, %v", receipt, err)
		}
		group, err := fixture.Group(ctx, "owner", "group")
		if err != nil || group.Owner != "owner" {
			t.Fatalf("invalid transfers changed ownership: %+v, %v", group, err)
		}
		if err := fixture.CloseGroup(ctx, "owner", "group"); err != nil {
			t.Fatal(err)
		}
		if receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "invitee", "closed"); !errors.Is(err, taskmanager.ErrDenied) ||
			receipt != (taskmanager.OwnershipTransfer{}) {
			t.Fatalf("closed group produced a transfer: %+v, %v", receipt, err)
		}
	})
}
