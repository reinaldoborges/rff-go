package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestAcceptedRemovalRechecksCurrentInvariants(t *testing.T) {
	for _, invalidation := range []string{"transfer", "assignment", "closure"} {
		t.Run(invalidation, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "race.db"), func() time.Time { return now })
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
			if _, err := fixture.Invite(ctx, "owner", "group", "member", "membership", "invite", now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Decide(ctx, "member", "membership", "accept", taskmanager.AcceptInvitation); err != nil {
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
			execution, err := fixture.RemoveMember(ctx, "owner", "membership", "remove")
			if err != nil {
				t.Fatal(err)
			}
			switch invalidation {
			case "transfer":
				_, err = fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
			case "assignment":
				err = fixture.AssignTask(ctx, "owner", "task", "membership")
			case "closure":
				err = fixture.CloseGroup(ctx, "owner", "group")
			}
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if err := fixture.Drive(ctx); err != nil {
					t.Fatal(err)
				}
			}
			record, err := fixture.InspectExecution(ctx, "operator", execution)
			if err != nil || record.Status != transition.Aborted || record.CommitReceipt == nil || record.CommitReceipt.Outcome != transition.CommitDenied {
				t.Fatalf("invalidated removal was not durably rejected and recovered: %+v, %v", record, err)
			}
			membership, err := fixture.InspectMembership(ctx, "operator", "membership")
			if err != nil || membership.Status != taskmanager.Active {
				t.Fatalf("invalidated removal changed membership: %+v, %v", membership, err)
			}
			if _, err := fixture.InspectRemoval(ctx, "operator", "membership"); !errors.Is(err, taskmanager.ErrNotFound) {
				t.Fatalf("rejected removal produced business audit: %v", err)
			}
		})
	}
}

func TestAcceptedRemovalSurvivesTransferToAnotherMember(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "former-owner.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "new-owner", "member"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"new-owner", "member"} {
		if _, err := fixture.Invite(ctx, "owner", "group", user, user+"-membership", "invite-"+user, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Decide(ctx, user, user+"-membership", "accept-"+user, taskmanager.AcceptInvitation); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "new-owner", "transfer"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := fixture.Membership(ctx, "new-owner", "member-membership")
	if err != nil || membership.Status != taskmanager.Removed {
		t.Fatalf("accepted removal was deauthorized by transfer: %+v, %v", membership, err)
	}
	audit, err := fixture.InspectRemoval(ctx, "operator", "member-membership")
	if err != nil || audit.Actor != "owner" {
		t.Fatalf("removal lost original actor: %+v, %v", audit, err)
	}
	if _, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("former owner retained removal replay permission: %v", err)
	}
}
