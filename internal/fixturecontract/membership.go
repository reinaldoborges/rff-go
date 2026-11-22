package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

// MembershipFixture adds the history and closure operations needed here.
type MembershipFixture interface {
	Fixture
	SeedOperator(context.Context, string) error
	InspectMembership(context.Context, string, string) (taskmanager.Membership, error)
	CloseGroup(context.Context, string, string) error
}

// RunMembership checks terminal history, deadlines and group closure.
func RunMembership(t *testing.T, newFixture func(*testing.T, func() time.Time) MembershipFixture) {
	t.Helper()
	for _, test := range []struct {
		name, actor string
		decision    taskmanager.InvitationDecision
		status      taskmanager.MembershipStatus
	}{{"decline", "invitee", taskmanager.DeclineInvitation, taskmanager.Declined},
		{"revoke", "owner", taskmanager.RevokeInvitation, taskmanager.Revoked}} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fixture := newFixture(t, func() time.Time { return now })
			if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "first", "invite-first", now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Decide(ctx, test.actor, "first", "decision", test.decision); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Decide(ctx, "invitee", "first", "competing", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrConflict) {
				t.Fatalf("competing decision was accepted: %v", err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			old, err := fixture.Membership(ctx, "owner", "first")
			if err != nil || old.Status != test.status {
				t.Fatalf("terminal outcome was not recorded: %+v, %v", old, err)
			}
			if _, err := fixture.Group(ctx, "invitee", "group"); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("terminal invitee gained access: %v", err)
			}
			if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "second", "invite-second", now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			retained, err := fixture.Membership(ctx, "owner", "first")
			if err != nil || retained != old {
				t.Fatalf("terminal history changed: before=%+v after=%+v error=%v", old, retained, err)
			}
			current, err := fixture.Membership(ctx, "owner", "second")
			if err != nil || current.Status != taskmanager.Invited {
				t.Fatalf("reinvitation was not a new membership: %+v, %v", current, err)
			}
		})
	}
	t.Run("persisted expiry boundary", func(t *testing.T) {
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
		if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", deadline); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		now = deadline
		if _, err := fixture.Decide(ctx, "invitee", "membership", "late", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrConflict) {
			t.Fatalf("acceptance at expiry was permitted: %v", err)
		}
		for range 2 {
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
		}
		membership, err := fixture.Membership(ctx, "owner", "membership")
		if err != nil || membership.Status != taskmanager.Expired || !membership.Deadline.Equal(deadline) {
			t.Fatalf("persisted deadline did not determine expiry: %+v, %v", membership, err)
		}
	})
	t.Run("competing terminal decisions", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
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
		type outcome struct {
			status taskmanager.MembershipStatus
			id     string
			err    error
		}
		start := make(chan struct{})
		results := make(chan outcome, 2)
		for _, decision := range []struct {
			event  taskmanager.InvitationDecision
			status taskmanager.MembershipStatus
		}{{taskmanager.AcceptInvitation, taskmanager.Active}, {taskmanager.DeclineInvitation, taskmanager.Declined}} {
			go func() {
				<-start
				id, err := fixture.Decide(ctx, "invitee", "membership", string(decision.event), decision.event)
				results <- outcome{status: decision.status, id: id, err: err}
			}()
		}
		close(start)
		var winner taskmanager.MembershipStatus
		conflicts := 0
		for range 2 {
			result := <-results
			switch {
			case result.err == nil && result.id != "":
				if winner != "" {
					t.Fatal("both decisions were accepted")
				}
				winner = result.status
			case errors.Is(result.err, taskmanager.ErrConflict):
				conflicts++
			default:
				t.Fatalf("unexpected competing decision outcome: %+v", result)
			}
		}
		if winner == "" || conflicts != 1 {
			t.Fatalf("competing decisions did not serialize: winner=%q conflicts=%d", winner, conflicts)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		membership, err := fixture.Membership(ctx, "owner", "membership")
		if err != nil || membership.Status != winner {
			t.Fatalf("committed decision differed from the accepted winner: %+v, %v", membership, err)
		}
	})
	t.Run("closure revokes pending invitations", func(t *testing.T) {
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
		if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if err := fixture.CloseGroup(ctx, "outsider", "group"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("outsider closed group: %v", err)
		}
		if err := fixture.CloseGroup(ctx, "owner", "group"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Group(ctx, "owner", "group"); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("closed group remained accessible: %v", err)
		}
		membership, err := fixture.InspectMembership(ctx, "operator", "membership")
		if err != nil || membership.Status != taskmanager.Revoked || membership.Reason != "group-closed" {
			t.Fatalf("closure did not retain revoked invitation: %+v, %v", membership, err)
		}
		if _, err := fixture.Invite(ctx, "owner", "group", "outsider", "new", "new", now.Add(time.Hour)); !errors.Is(err, taskmanager.ErrDenied) {
			t.Fatalf("closed group accepted invitation: %v", err)
		}
	})
}
