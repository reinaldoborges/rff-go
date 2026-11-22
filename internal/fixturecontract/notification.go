package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

// NotificationFixture adds the host-owned invitation delivery seam.
type NotificationFixture interface {
	MembershipFixture
	SetNotificationSink(func(context.Context, taskmanager.InvitationNotification) error) error
}

// DenialFixture exposes domain behavior, notification delivery and execution inspection.
type DenialFixture interface {
	NotificationFixture
	OperatorFixture
}

// RunDenialIsolation checks rejected requests create no execution or notification.
func RunDenialIsolation(t *testing.T, newFixture func(*testing.T, func() time.Time) DenialFixture) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture := newFixture(t, func() time.Time { return now })
	if err := fixture.SeedUsers(ctx, "owner", "invitee", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	var notifications []taskmanager.InvitationNotification
	if err := fixture.SetNotificationSink(func(_ context.Context, notification taskmanager.InvitationNotification) error {
		notifications = append(notifications, notification)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "create"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "invitation", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := fixture.ListExecutions(ctx, "operator")
	if err != nil || len(notifications) != 1 {
		t.Fatalf("baseline execution or notification missing: executions=%v notifications=%+v error=%v", before, notifications, err)
	}
	if id, err := fixture.Invite(ctx, "outsider", "group", "outsider", "denied", "denied", now.Add(time.Hour)); !errors.Is(err, taskmanager.ErrDenied) || id != "" {
		t.Fatalf("unauthorized invitation was accepted: id=%q error=%v", id, err)
	}
	if id, err := fixture.TransitionTask(ctx, "owner", "task", "invalid", taskmanager.CompleteTask); !errors.Is(err, taskmanager.ErrConflict) || id != "" {
		t.Fatalf("invalid task transition was accepted: id=%q error=%v", id, err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.ListExecutions(ctx, "operator")
	if err != nil || len(after) != len(before) || len(notifications) != 1 {
		t.Fatalf("rejected requests created work or notification: before=%v after=%v notifications=%+v error=%v", before, after, notifications, err)
	}
	task, err := fixture.Task(ctx, "owner", "task")
	if err != nil || task.Status != taskmanager.TaskOpen {
		t.Fatalf("invalid transition changed the task: %+v, %v", task, err)
	}
	if _, err := fixture.Membership(ctx, "owner", "denied"); !errors.Is(err, taskmanager.ErrNotFound) {
		t.Fatalf("denied invitation created a membership: %v", err)
	}
}

// RunNotification checks independent delivery retry and membership expiry.
func RunNotification(t *testing.T, newFixture func(*testing.T, func() time.Time) NotificationFixture) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)
	fixture := newFixture(t, func() time.Time { return now })
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	unavailable := errors.New("delivery unavailable")
	var seen []taskmanager.InvitationNotification
	if err := fixture.SetNotificationSink(func(_ context.Context, notification taskmanager.InvitationNotification) error {
		seen = append(seen, notification)
		if len(seen) == 1 {
			return unavailable
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", deadline); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); !errors.Is(err, unavailable) {
		t.Fatalf("delivery failure was not reported: %v", err)
	}
	invitation, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || invitation.Status != taskmanager.Invited || !invitation.Deadline.Equal(deadline) {
		t.Fatalf("delivery failure changed committed invitation: %+v, %v", invitation, err)
	}
	now = deadline
	for range 2 {
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expired, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || expired.Status != taskmanager.Expired || !expired.Deadline.Equal(deadline) {
		t.Fatalf("delivery failure prevented expiry: %+v, %v", expired, err)
	}
	if len(seen) != 2 || seen[0].ID == "" || seen[0].ID != seen[1].ID || seen[1].MembershipID != "membership" {
		t.Fatalf("delivery did not retry with stable identity: %+v", seen)
	}
}
