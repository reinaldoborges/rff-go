package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestInvitationExpiresAtPersistedDeadlineAfterRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "expiry.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "old-membership", "invite", deadline); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	now = deadline
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "invitee", "old-membership", "late", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("acceptance at expiry was not rejected: %v", err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	old, err := fixture.Membership(ctx, "owner", "old-membership")
	if err != nil || old.Status != taskmanager.Expired || !old.Deadline.Equal(deadline) {
		t.Fatalf("persisted deadline did not expire invitation: membership=%+v error=%v", old, err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "new-membership", "reinvite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	old, err = fixture.Membership(ctx, "owner", "old-membership")
	if err != nil || old.Status != taskmanager.Expired {
		t.Fatalf("reinvitating changed terminal history: membership=%+v error=%v", old, err)
	}
	newMembership, err := fixture.Membership(ctx, "owner", "new-membership")
	if err != nil || newMembership.Status != taskmanager.Invited {
		t.Fatalf("reinvitation did not create a new membership: membership=%+v error=%v", newMembership, err)
	}
}
