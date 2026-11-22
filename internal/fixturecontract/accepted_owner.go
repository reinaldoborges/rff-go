package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

// RunAcceptedOwnerWork checks that transfer does not deauthorize accepted work.
func RunAcceptedOwnerWork(t *testing.T, newFixture func(*testing.T, func() time.Time) GovernanceFixture) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture := newFixture(t, func() time.Time { return now })
	activeMember(t, fixture, now)
	if _, err := fixture.Invite(ctx, "owner", "group", "outsider", "pending-membership", "pending", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatalf("previously accepted invitation was deauthorized: %v", err)
	}
	invitation, err := fixture.Membership(ctx, "member", "pending-membership")
	if err != nil || invitation.Status != taskmanager.Invited {
		t.Fatalf("accepted invitation did not commit after transfer: %+v, %v", invitation, err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "outsider", "other", "new", now.Add(time.Hour)); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("former owner retained invitation authority: %v", err)
	}
}
