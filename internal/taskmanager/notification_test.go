package taskmanager_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestInvitationNotificationFailureDoesNotStopExpiryAndRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "notification.db")
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
	unavailable := errors.New("notification sink unavailable")
	var seen []taskmanager.InvitationNotification
	deliver := func(_ context.Context, notification taskmanager.InvitationNotification) error {
		seen = append(seen, notification)
		if len(seen) == 1 {
			return unavailable
		}
		return nil
	}
	if err := fixture.SetNotificationSink(deliver); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", deadline); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); !errors.Is(err, unavailable) {
		t.Fatalf("notification failure was not surfaced: %v", err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited || !membership.Deadline.Equal(deadline) {
		t.Fatalf("notification failure changed invitation: membership=%+v error=%v", membership, err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	now = deadline
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.SetNotificationSink(deliver); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err = fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Expired {
		t.Fatalf("notification failure stopped expiry: membership=%+v error=%v", membership, err)
	}
	if len(seen) != 2 || seen[0].ID == "" || seen[0].ID != seen[1].ID || seen[1].MembershipID != "membership" {
		t.Fatalf("notification intent was not redelivered with stable identity: %+v", seen)
	}
}

func TestLegacyNotificationSurvivesMembershipIdentityRemapping(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "legacy-notification.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
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
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "taskmanager-task:task", "invite", now.Add(time.Hour)); err != nil {
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
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	fixture = nil
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE tm_runtime_entities`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "invitee", "taskmanager-task:task", "accept", taskmanager.AcceptInvitation); err != nil {
		t.Fatal(err)
	}
	var seen []taskmanager.InvitationNotification
	if err := fixture.SetNotificationSink(func(_ context.Context, notification taskmanager.InvitationNotification) error {
		seen = append(seen, notification)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].MembershipID != "taskmanager-task:task" || seen[0].UserID != "invitee" {
		t.Fatalf("legacy notification did not retain membership identity: %+v", seen)
	}
}
