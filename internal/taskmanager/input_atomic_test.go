package taskmanager

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type interruptedInputStore struct {
	transition.InputStore
	failure error
}

func (s interruptedInputStore) CompleteInput(context.Context, string, transition.InputOutcome) (transition.InputRecord, error) {
	return transition.InputRecord{}, s.failure
}

func TestRejectedRemovalSurvivesLostInputCompletion(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "input.db")
	fixture, err := Open(path, func() time.Time { return now })
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
	if err := fixture.SeedUsers(ctx, "owner", "member"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "member", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Decide(ctx, "member", "membership", "accept", AcceptInvitation); err != nil {
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
	if err := fixture.AssignTask(ctx, "owner", "task", "membership"); err != nil {
		t.Fatal(err)
	}
	id, err := inputID("owner", "group", "remove")
	if err != nil {
		t.Fatal(err)
	}
	lost := errors.New("input completion response lost")
	ingress, err := transition.NewInputIngress[fixtureInput](
		interruptedInputStore{InputStore: fixture.store, failure: lost},
		"taskmanager-decisions", resolveMembershipInput, fixture.acceptDecision,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[fixtureInput]{
		ID: id, WorkflowID: "membership", Kind: transition.ExternalEvent, Codec: membershipInputCodec,
		Value: fixtureInput{Actor: "owner", GroupID: "group", UserID: "member", MembershipID: "membership", Event: removeMembership},
	}
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, lost) {
		t.Fatalf("interrupted completion was not reported: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err = Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ingress, err = transition.NewInputIngress[fixtureInput](
		interruptedInputStore{InputStore: fixture.store, failure: lost},
		"taskmanager-decisions", resolveMembershipInput, fixture.acceptDecision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.AssignTask(ctx, "owner", "task", ""); err != nil {
		t.Fatal(err)
	}
	outcome, err := ingress.Submit(ctx, request)
	if err != nil || outcome.Failure == nil || outcome.Failure.Code != "conflict" || outcome.Receipt != "" {
		t.Fatalf("rejected removal changed after unassignment: %+v, %v", outcome, err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != Active {
		t.Fatalf("replayed rejection changed membership: %+v, %v", membership, err)
	}
}
