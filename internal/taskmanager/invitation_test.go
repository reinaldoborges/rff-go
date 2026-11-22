package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestInvitationStartsThroughRuntimeAndDoesNotGrantAccess(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "invitations.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	if _, err := fixture.Invite(ctx, "outsider", "group", "invitee", "membership", "request", deadline); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("non-owner invitation was not denied: %v", err)
	}
	executionID, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "request", deadline)
	if err != nil || executionID == "" {
		t.Fatalf("invitation was not durably accepted: execution=%q error=%v", executionID, err)
	}
	if _, err := fixture.Membership(ctx, "owner", "membership"); !errors.Is(err, taskmanager.ErrNotFound) {
		t.Fatalf("membership committed before its hook ran: %v", err)
	}
	replayed, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "request", deadline)
	if err != nil || replayed != executionID {
		t.Fatalf("invitation replay changed execution: execution=%q error=%v", replayed, err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited || !membership.Deadline.Equal(deadline) {
		t.Fatalf("invitation did not commit: membership=%+v error=%v", membership, err)
	}
	if _, err := fixture.Group(ctx, "invitee", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("invited user gained group access: %v", err)
	}
}
