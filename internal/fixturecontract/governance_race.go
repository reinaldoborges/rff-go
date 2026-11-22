package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// GovernanceRaceFixture combines domain mutation and inspection ports.
type GovernanceRaceFixture interface {
	GovernanceFixture
	OperatorFixture
}

// RunGovernanceRace checks invariants at the commit boundary of accepted work.
func RunGovernanceRace(t *testing.T, newFixture func(*testing.T, func() time.Time) GovernanceRaceFixture) {
	t.Helper()
	for _, invalidation := range []string{"transfer", "assignment", "closure"} {
		t.Run(invalidation, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fixture := newFixture(t, func() time.Time { return now })
			activeMember(t, fixture, now)
			if _, err := fixture.CreateTask(ctx, "owner", "group", "task", "Task", "task"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			id, err := fixture.RemoveMember(ctx, "owner", "member-membership", "remove")
			if err != nil {
				t.Fatal(err)
			}
			switch invalidation {
			case "transfer":
				_, err = fixture.TransferOwnership(ctx, "owner", "group", "member", "transfer")
			case "assignment":
				err = fixture.AssignTask(ctx, "owner", "task", "member-membership")
			case "closure":
				err = fixture.CloseGroup(ctx, "owner", "group")
			}
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if err := fixture.Drive(ctx); err != nil {
					t.Fatal(err)
				}
			}
			record, err := fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.Aborted || record.CommitReceipt == nil ||
				record.CommitReceipt.Outcome != transition.CommitDenied {
				t.Fatalf("invalidated accepted removal was not rejected and recovered: %+v, %v", record, err)
			}
			member, err := fixture.InspectMembership(ctx, "operator", "member-membership")
			if err != nil || member.Status != taskmanager.Active {
				t.Fatalf("rejected removal changed membership: %+v, %v", member, err)
			}
			if _, err := fixture.InspectRemoval(ctx, "operator", "member-membership"); !errors.Is(err, taskmanager.ErrNotFound) {
				t.Fatalf("rejected removal produced business audit: %v", err)
			}
		})
	}
}
