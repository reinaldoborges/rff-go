package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type notificationControlClassifier struct{}

func (notificationControlClassifier) ClassifyHookFailure(transition.Execution[string, string], error) (transition.HookResult, error) {
	return transition.HookResult{}, errors.New("unexpected hook failure")
}

type notificationControlCommit struct{}

func (notificationControlCommit) Commit(_ context.Context, _ transition.TransitionLease[string], execution transition.Execution[string, string]) (transition.CommitReceipt, error) {
	return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded}, nil
}

type notificationControlFailure struct{}

func (notificationControlFailure) Handoff(context.Context, transition.TransitionLease[string], transition.NotificationIntent[string, string]) error {
	return &transition.NotificationHandoffFailure{Failure: fsm.Failure{Code: "injected"}}
}

func TestOperatorNotificationRepairControls(t *testing.T) {
	for _, action := range []transition.ControlAction{transition.ControlNotificationRetry, transition.ControlNotificationWaive} {
		t.Run(string(action), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			path := filepath.Join(t.TempDir(), "repair.db")
			fixture, err := taskmanager.Open(path, clock)
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
			id, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			config := transition.CoordinatorConfig{ID: "taskmanager", Clock: clock, LeaseTTL: time.Minute}
			coordinator, err := transition.NewCoordinator[string, string](store, notificationControlClassifier{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			claim, err := store.ClaimHook(ctx, "worker", clock, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StartHook(ctx, claim, clock); err != nil {
				t.Fatal(err)
			}
			if err := store.ReportHook(ctx, claim, transition.HookResult{Success: true}, clock); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			commits, err := transition.NewCommitCoordinator[string, string](store, notificationControlCommit{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := commits.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			notifications, err := transition.NewNotificationCoordinator[string, string](store, notificationControlFailure{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := notifications.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			record, err := fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.NotificationRepair {
				t.Fatalf("failed handoff did not enter repair: %+v, %v", record, err)
			}
			if err := fixture.ControlExecution(ctx, "owner", id, "repair", action); !errors.Is(err, taskmanager.ErrDenied) {
				t.Fatalf("owner was allowed notification control: %v", err)
			}
			record, err = fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.NotificationRepair || len(record.NotificationControls) != 0 {
				t.Fatalf("denied control changed repair: %+v, %v", record, err)
			}
			if err := fixture.ControlExecution(ctx, "operator", id, "repair", action); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Drive(ctx); err != nil {
				t.Fatal(err)
			}
			record, err = fixture.InspectExecution(ctx, "operator", id)
			if err != nil || record.Status != transition.Complete {
				t.Fatalf("authorized notification control did not settle: %+v, %v", record, err)
			}
			outbox, err := sqlite.NewNotificationOutbox(store, clock)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
			if action == transition.ControlNotificationRetry {
				if err != nil || delivery.Intent.ExecutionID != id {
					t.Fatalf("retry did not hand off notification: %+v, %v", delivery, err)
				}
			} else if !errors.Is(err, transition.ErrNoNotificationActivity) {
				t.Fatalf("waiver created notification work: %+v, %v", delivery, err)
			}
			if err := fixture.ControlExecution(ctx, "operator", id, "repair", action); err != nil {
				t.Fatalf("notification control did not replay: %v", err)
			}
			audit, err := fixture.OperatorAudit(ctx, "operator")
			if err != nil {
				t.Fatal(err)
			}
			allowed, denied := false, false
			for _, entry := range audit {
				if entry.ExecutionID == id && entry.Action == string(action) {
					allowed = allowed || entry.Actor == "operator" && entry.Allowed
					denied = denied || entry.Actor == "owner" && !entry.Allowed
				}
			}
			if !allowed || !denied {
				t.Fatalf("notification controls not audited: %+v", audit)
			}
		})
	}
}
