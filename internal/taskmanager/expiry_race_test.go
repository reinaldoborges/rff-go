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

func TestExpiryWinsWhenAcceptedDecisionCannotCommitBeforeDeadline(t *testing.T) {
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
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", deadline); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	now = deadline.Add(-time.Second)
	if _, err := fixture.Decide(ctx, "invitee", "membership", "accept", taskmanager.AcceptInvitation); err != nil {
		t.Fatalf("pre-deadline decision was not accepted: %v", err)
	}
	now = deadline
	for range 4 {
		if err := fixture.Drive(ctx); err != nil && !errors.Is(err, transition.ErrBusy) {
			t.Fatalf("reconcile expiry race: %v", err)
		}
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Expired {
		t.Fatalf("expired invitation was activated or not recovered: membership=%+v error=%v", membership, err)
	}
	if _, err := fixture.Group(ctx, "invitee", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("expired invitee gained access: %v", err)
	}
}
