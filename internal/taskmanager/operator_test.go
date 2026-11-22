package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestOperatorExecutionInspectionIsAuthorizedAndAudited(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "operator.db"), func() time.Time { return now })
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
	execution, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.InspectExecution(ctx, "owner", execution); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("domain owner was allowed to inspect execution: %v", err)
	}
	ids, err := fixture.ListExecutions(ctx, "operator")
	if err != nil || len(ids) != 1 || ids[0] != execution {
		t.Fatalf("operator cannot list execution: %v, %v", ids, err)
	}
	record, err := fixture.InspectExecution(ctx, "operator", execution)
	if err != nil || record.ID != execution {
		t.Fatalf("operator cannot inspect execution: %+v, %v", record, err)
	}
	audit, err := fixture.OperatorAudit(ctx, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if len(audit) != 3 ||
		audit[0].Actor != "owner" || audit[0].Action != "inspect" || audit[0].ExecutionID != execution || audit[0].Allowed ||
		audit[1].Actor != "operator" || audit[1].Action != "list" || !audit[1].Allowed ||
		audit[2].Actor != "operator" || audit[2].Action != "inspect" || audit[2].ExecutionID != execution || !audit[2].Allowed {
		t.Fatalf("operator actions were not audited: %+v", audit)
	}
}

func TestOperatorRetryUsesPhaseGatesAndRecordsActor(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "control.db"), func() time.Time { return now })
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
	execution, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.ControlExecution(ctx, "owner", execution, "denied", transition.ControlRetry); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("domain owner acquired operator controls: %v", err)
	}
	if err := fixture.ControlExecution(ctx, "operator", execution, "wrong-phase", transition.ControlRetry); err == nil {
		t.Fatal("retry before intervention was accepted")
	}
	now = now.Add(61 * time.Minute)
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := fixture.InspectExecution(ctx, "operator", execution)
	if err != nil || before.Status != transition.NeedsIntervention {
		t.Fatalf("expected intervention: %+v, %v", before, err)
	}
	if err := fixture.ControlExecution(ctx, "operator", execution, "retry", transition.ControlRetry); err != nil {
		t.Fatal(err)
	}
	first, err := fixture.InspectExecution(ctx, "operator", execution)
	if err != nil || len(first.Controls) != 1 {
		t.Fatalf("retry was not recorded: %+v, %v", first, err)
	}
	now = now.Add(time.Second)
	if err := fixture.ControlExecution(ctx, "operator", execution, "retry", transition.ControlRetry); err != nil {
		t.Fatalf("identical retry did not replay: %v", err)
	}
	replayed, err := fixture.InspectExecution(ctx, "operator", execution)
	if err != nil || len(replayed.Controls) != 1 || !replayed.CycleDeadline.Equal(first.CycleDeadline) {
		t.Fatalf("retry replay changed control cycle: %+v, %v", replayed, err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited {
		t.Fatalf("authorized retry did not resume work: %+v, %v", membership, err)
	}
	if err := fixture.ControlExecution(ctx, "operator", execution, "retry", transition.ControlRetry); err != nil {
		t.Fatalf("completed execution lost retry receipt: %v", err)
	}
	if err := fixture.ControlExecution(ctx, "operator", execution, "retry", transition.ControlAbort); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed completed control did not conflict: %v", err)
	}
	audit, err := fixture.OperatorAudit(ctx, "operator")
	if err != nil {
		t.Fatal(err)
	}
	foundDenied, foundAllowed := false, false
	for _, entry := range audit {
		if entry.ExecutionID == execution && entry.Action == string(transition.ControlRetry) {
			foundDenied = foundDenied || entry.Actor == "owner" && !entry.Allowed
			foundAllowed = foundAllowed || entry.Actor == "operator" && entry.Allowed
		}
	}
	if !foundDenied || !foundAllowed {
		t.Fatalf("retry authorization was not audited: %+v", audit)
	}
}

func TestOperatorAbortRecoversWithoutCommittingInvitation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "abort.db"), func() time.Time { return now })
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
	execution, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(61 * time.Minute)
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.ControlExecution(ctx, "operator", execution, "waive", transition.ControlNotificationWaive); err == nil {
		t.Fatal("notification waiver was accepted outside notification repair")
	}
	if err := fixture.ControlExecution(ctx, "operator", execution, "abort", transition.ControlAbort); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	record, err := fixture.InspectExecution(ctx, "operator", execution)
	if err != nil || record.Status != transition.Aborted {
		t.Fatalf("abort did not finish recovery: %+v, %v", record, err)
	}
	if _, err := fixture.InspectMembership(ctx, "operator", "membership"); !errors.Is(err, taskmanager.ErrNotFound) {
		t.Fatalf("aborted invitation was committed: %v", err)
	}
}
