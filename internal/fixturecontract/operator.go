package fixturecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// OperatorFixture adds authorized execution inspection and intervention.
type OperatorFixture interface {
	MembershipFixture
	ListExecutions(context.Context, string) ([]string, error)
	InspectExecution(context.Context, string, string) (transition.ExecutionInspection, error)
	ControlExecution(context.Context, string, string, string, transition.ControlAction) error
	OperatorAudit(context.Context, string) ([]taskmanager.OperatorAuditRecord, error)
}

// RunOperator checks operator isolation, audit, retry and abort phase gates.
func RunOperator(t *testing.T, newFixture func(*testing.T, func() time.Time) OperatorFixture) {
	t.Helper()
	for _, action := range []transition.ControlAction{transition.ControlRetry, transition.ControlAbort} {
		t.Run(string(action), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fixture := newFixture(t, func() time.Time { return now })
			if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.SeedOperator(ctx, "operator"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
				t.Fatal(err)
			}
			id, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(3*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.InspectExecution(ctx, "owner", id); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("domain owner gained operator inspection: %v", err)
			}
			ids, err := fixture.ListExecutions(ctx, "operator")
			if err != nil || len(ids) != 1 || ids[0] != id {
				t.Fatalf("operator could not list execution: %v, %v", ids, err)
			}
			if err := fixture.ControlExecution(ctx, "owner", id, "denied", action); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("domain owner gained operator control: %v", err)
			}
			if err := fixture.ControlExecution(ctx, "operator", id, "early", action); !errors.Is(err, transition.ErrInvalidHook) {
				t.Fatalf("invalid-phase control was accepted: %v", err)
			}
			now = now.Add(61 * time.Minute)
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			record, err := fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.NeedsIntervention {
				t.Fatalf("execution did not enter intervention: %+v, %v", record, err)
			}
			if err := fixture.ControlExecution(ctx, "operator", id, "control", action); err != nil {
				t.Fatal(err)
			}
			first, err := fixture.InspectExecution(ctx, "operator", id)
			if err != nil || len(first.Controls) != 1 {
				t.Fatalf("control was not recorded: %+v, %v", first, err)
			}
			now = now.Add(time.Second)
			if err := fixture.ControlExecution(ctx, "operator", id, "control", action); err != nil {
				t.Fatalf("control replay failed: %v", err)
			}
			replay, err := fixture.InspectExecution(ctx, "operator", id)
			if err != nil || len(replay.Controls) != 1 || !replay.CycleDeadline.Equal(first.CycleDeadline) {
				t.Fatalf("control replay renewed its cycle: %+v, %v", replay, err)
			}
			for range 2 {
				if err := fixture.Drive(ctx); err != nil {
					t.Fatal(err)
				}
			}
			record, err = fixture.InspectExecution(ctx, "operator", id)
			want := transition.Complete
			if action == transition.ControlAbort {
				want = transition.Aborted
			}
			if err != nil || record.Status != want {
				t.Fatalf("operator control did not settle: %+v, %v", record, err)
			}
			membership, err := fixture.InspectMembership(ctx, "operator", "membership")
			if action == transition.ControlAbort {
				if !errors.Is(err, taskmanager.ErrNotFound) {
					t.Fatalf("aborted invitation committed: %+v, %v", membership, err)
				}
			} else if err != nil || membership.Status != taskmanager.Invited {
				t.Fatalf("retried invitation did not commit: %+v, %v", membership, err)
			}
			audit, err := fixture.OperatorAudit(ctx, "operator")
			if err != nil {
				t.Fatal(err)
			}
			denied, allowed := false, false
			for _, entry := range audit {
				if entry.ExecutionID == id && entry.Action == string(action) {
					denied = denied || entry.Actor == "owner" && !entry.Allowed
					allowed = allowed || entry.Actor == "operator" && entry.Allowed
				}
			}
			if !denied || !allowed {
				t.Fatalf("operator control authorization was not audited: %+v", audit)
			}
		})
	}
}
