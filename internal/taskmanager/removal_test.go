package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestActiveMemberRemovalRequiresUnassignmentAndRetainsHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "removal.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "member"); err != nil {
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
		t.Fatalf("removed member with an assigned task: %v", err)
	}
	if _, err := fixture.Group(ctx, "member", "group"); err != nil {
		t.Fatalf("rejected removal changed access: %v", err)
	}
	if err := fixture.AssignTask(ctx, "owner", "task", ""); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	execution, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := fixture.InspectMembership(ctx, "operator", "member-membership")
	if err != nil || membership.Status != taskmanager.Removed {
		t.Fatalf("removal did not retain terminal membership: %+v, %v", membership, err)
	}
	if replay, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove"); err != nil || replay != execution {
		t.Fatalf("removal replay changed: %q, %v", replay, err)
	}
	if _, err := fixture.RemoveMember(ctx, "owner", "owner-membership", "remove"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("changed removal replay did not conflict: %v", err)
	}
	if _, err := fixture.RemoveMember(ctx, "owner", "owner-membership", "remove-owner"); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("current owner could be removed: %v", err)
	}
	if _, err := fixture.Group(ctx, "member", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("removed member retained access: %v", err)
	}
	if _, err := fixture.Decide(ctx, "member", "member-membership", "accept", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("removed member replayed invitation decision: %v", err)
	}
	audit, err := fixture.InspectRemoval(ctx, "operator", "member-membership")
	if err != nil || audit.Actor != "owner" || audit.MembershipID != "member-membership" || !audit.At.Equal(now) {
		t.Fatalf("removal audit not retained: %+v, %v", audit, err)
	}
	history, err := fixture.TaskAssignmentHistory(ctx, "owner", "task")
	if err != nil || len(history) != 1 || history[0].MembershipID != membership.ID {
		t.Fatalf("removal lost task history: %+v, %v", history, err)
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
}
