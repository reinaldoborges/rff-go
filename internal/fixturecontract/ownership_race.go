package fixturecontract

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

// RunOwnershipRace checks serialized competing transfer and closure operations.
func RunOwnershipRace(t *testing.T, newFixture func(*testing.T, func() time.Time) GovernanceFixture) {
	t.Helper()
	t.Run("competing transfers", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		if _, err := fixture.Invite(ctx, "owner", "group", "outsider", "second-membership", "invite-second", now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Decide(ctx, "outsider", "second-membership", "accept-second", taskmanager.AcceptInvitation); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
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
		for _, recipient := range []string{"member", "outsider"} {
			go func(recipient string) {
				ready.Done()
				<-start
				receipt, err := fixture.TransferOwnership(ctx, "owner", "group", recipient, "transfer-"+recipient)
				results <- result{receipt: receipt, err: err}
			}(recipient)
		}
		ready.Wait()
		close(start)
		winner := ""
		for range 2 {
			result := <-results
			if result.err == nil {
				if winner != "" {
					t.Fatal("two transfers succeeded from one owner")
				}
				winner = result.receipt.NewOwner
			} else if !errors.Is(result.err, taskmanager.ErrDenied) {
				t.Fatalf("unexpected competing transfer outcome: %v", result.err)
			}
		}
		group, err := fixture.Group(ctx, "owner", "group")
		if err != nil || winner == "" || group.Owner != winner {
			t.Fatalf("competing transfers left inconsistent ownership: %+v winner=%q error=%v", group, winner, err)
		}
	})
	t.Run("transfer versus closure", func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fixture := newFixture(t, func() time.Time { return now })
		activeMember(t, fixture, now)
		start := make(chan struct{})
		transferred, closed := make(chan error, 1), make(chan error, 1)
		var ready sync.WaitGroup
		ready.Add(2)
		go func() {
			ready.Done()
			<-start
			_, err := fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
			transferred <- err
		}()
		go func() {
			ready.Done()
			<-start
			closed <- fixture.CloseGroup(ctx, "owner", "group")
		}()
		ready.Wait()
		close(start)
		transferErr, closeErr := <-transferred, <-closed
		switch {
		case transferErr == nil && errors.Is(closeErr, taskmanager.ErrDenied):
			group, err := fixture.Group(ctx, "member", "group")
			if err != nil || group.Owner != "member" || group.Closed {
				t.Fatalf("transfer winner left inconsistent group: %+v, %v", group, err)
			}
		case closeErr == nil && errors.Is(transferErr, taskmanager.ErrDenied):
			if _, err := fixture.Group(ctx, "owner", "group"); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("closure winner left group accessible: %v", err)
			}
		default:
			t.Fatalf("transfer and closure did not serialize: transfer=%v closure=%v", transferErr, closeErr)
		}
	})
}
