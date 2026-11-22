package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestCompetingOwnershipTransfersSerializeAndAcceptedInvitationSurvivesTransfer(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "transfer-race.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "first", "second", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"first", "second"} {
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
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "pending-membership", "pending", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	type result struct {
		receipt taskmanager.OwnershipTransfer
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	for _, recipient := range []string{"first", "second"} {
		go func(recipient string) {
			ready.Done()
			<-start
			receipt, err := fixture.TransferOwnership(ctx, "owner", "group", recipient, "transfer-"+recipient)
			results <- result{receipt: receipt, err: err}
		}(recipient)
	}
	ready.Wait()
	close(start)
	var winner string
	for range 2 {
		result := <-results
		if result.err == nil {
			if winner != "" {
				t.Fatal("two transfers succeeded from the same owner")
			}
			winner = result.receipt.NewOwner
		} else if !errors.Is(result.err, taskmanager.ErrDenied) {
			t.Fatalf("unexpected competing transfer result: %v", result.err)
		}
	}
	group, err := fixture.Group(ctx, "owner", "group")
	if err != nil || winner == "" || group.Owner != winner {
		t.Fatalf("ownership race left an inconsistent owner: group=%+v winner=%q error=%v", group, winner, err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatalf("previously accepted invitation was deauthorized: %v", err)
	}
	invitation, err := fixture.Membership(ctx, winner, "pending-membership")
	if err != nil || invitation.Status != taskmanager.Invited {
		t.Fatalf("accepted work did not commit after transfer: membership=%+v error=%v", invitation, err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "other", "after-transfer", now.Add(time.Hour)); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("former owner retained invitation authority: %v", err)
	}
}
