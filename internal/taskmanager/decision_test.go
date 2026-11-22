package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestInvitationDecisionActivatesOnlyTheInviteeAndReplays(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "decisions.db"), func() time.Time { return now })
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
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "outsider", "membership", "bad", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("unrelated user decided an invitation: %v", err)
	}
	execution, err := fixture.Decide(ctx, "invitee", "membership", "decision", taskmanager.AcceptInvitation)
	if err != nil || execution == "" {
		t.Fatalf("acceptance was not durably accepted: execution=%q error=%v", execution, err)
	}
	if _, err := fixture.Group(ctx, "invitee", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("acceptance granted access before commit: %v", err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	group, err := fixture.Group(ctx, "invitee", "group")
	if err != nil || group.ID != "group" {
		membership, readErr := fixture.Membership(ctx, "owner", "membership")
		t.Fatalf("accepted member has no group access: group=%+v error=%v membership=%+v read error=%v", group, err, membership, readErr)
	}
	replayed, err := fixture.Decide(ctx, "invitee", "membership", "decision", taskmanager.AcceptInvitation)
	if err != nil || replayed != execution {
		t.Fatalf("decision replay changed outcome: execution=%q error=%v", replayed, err)
	}
	if _, err := fixture.Decide(ctx, "invitee", "membership", "decision", taskmanager.DeclineInvitation); !errors.Is(err, taskmanager.ErrConflict) {
		t.Fatalf("changed decision replay did not conflict: %v", err)
	}
}

func TestInvitationTerminalDecisionsPreserveHistoryAndAllowReinvitation(t *testing.T) {
	tests := []struct {
		name       string
		decision   taskmanager.InvitationDecision
		actor      string
		unauth     string
		wantStatus taskmanager.MembershipStatus
	}{
		{"decline", taskmanager.DeclineInvitation, "invitee", "owner", taskmanager.Declined},
		{"revoke", taskmanager.RevokeInvitation, "owner", "invitee", taskmanager.Revoked},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "terminal.db"), func() time.Time { return now })
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
			if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "first", "invite-first", now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Decide(ctx, test.unauth, "first", "unauthorized", test.decision); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("unauthorized decision was accepted: %v", err)
			}
			if _, err := fixture.Decide(ctx, test.actor, "first", "terminal", test.decision); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.Decide(ctx, "invitee", "first", "competing", taskmanager.AcceptInvitation); !errors.Is(err, taskmanager.ErrConflict) {
				t.Fatalf("competing decision was accepted: %v", err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			old, err := fixture.Membership(ctx, "owner", "first")
			if err != nil || old.Status != test.wantStatus {
				t.Fatalf("wrong terminal membership: %+v, %v", old, err)
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
				t.Fatalf("new invitation not created: %+v, %v", current, err)
			}
		})
	}
}
