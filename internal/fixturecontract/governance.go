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
		receipt, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
		if err != nil || receipt.Actor != "owner" || receipt.FormerOwner != "owner" || receipt.NewOwner != "member" || !receipt.At.Equal(now) {
			t.Fatalf("transfer audit was not recorded: %+v, %v", receipt, err)
		}
		group, err := fixture.Group(ctx, "owner", "group")
		if err != nil || group.Owner != "member" {
			t.Fatalf("transfer did not preserve former owner membership: %+v, %v", group, err)
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
}
