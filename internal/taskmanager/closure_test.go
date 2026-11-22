package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestGroupClosureRevokesInvitationsAndOperatorCanInspectHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "closure.db"), func() time.Time { return now })
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
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CloseGroup(ctx, "outsider", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("outsider closed the group: %v", err)
	}
	if err := fixture.CloseGroup(ctx, "owner", "group"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Group(ctx, "owner", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("closed group still grants access: %v", err)
	}
	if _, err := fixture.Group(ctx, "operator", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("test operator acquired group permissions: %v", err)
	}
	membership, err := fixture.InspectMembership(ctx, "operator", "membership")
	if err != nil || membership.Status != taskmanager.Revoked || membership.Reason != "group-closed" {
		t.Fatalf("closure did not retain revoked invitation: membership=%+v error=%v", membership, err)
	}
	owner, err := fixture.InspectMembership(ctx, "operator", "owner-membership")
	if err != nil || owner.Status != taskmanager.Active {
		t.Fatalf("closure changed owner history: membership=%+v error=%v", owner, err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "outsider", "new", "new", now.Add(time.Hour)); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("closed group accepted invitation: %v", err)
	}
}

func TestClosingGroupRejectsPreviouslyAcceptedInvitationAtCommit(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "accepted-before-closure.db"), func() time.Time { return now })
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
	if err := fixture.SeedOperator(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "pending", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CloseGroup(ctx, "owner", "group"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if membership, err := fixture.InspectMembership(ctx, "operator", "pending"); !errors.Is(err, taskmanager.ErrNotFound) {
		t.Fatalf("closed group committed pending invitation: %+v, %v", membership, err)
	}
}
