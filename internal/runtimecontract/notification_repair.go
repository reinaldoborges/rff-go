package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// NotificationRepairStore combines the ports used by the repair contract.
type NotificationRepairStore interface {
	Store
	transition.AuthorizedControlStore[string]
}

type repairHandoff struct {
	available bool
	delivered int
}

func (h *repairHandoff) Handoff(context.Context, transition.TransitionLease[string], transition.NotificationIntent[string, string]) error {
	if !h.available {
		return &transition.NotificationHandoffFailure{Failure: fsm.Failure{Code: "unavailable"}}
	}
	h.delivered++
	return nil
}

// RunNotificationRepair checks post-commit repair and authorized retry or waiver.
func RunNotificationRepair(t *testing.T, newStore func(*testing.T) NotificationRepairStore) {
	t.Helper()
	for _, action := range []transition.NotificationControlAction{transition.RetryNotification, transition.WaiveNotification} {
		t.Run(string(action), func(t *testing.T) {
			ctx := context.Background()
			store := newStore(t)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: "execution", Entity: "entity", Key: "key",
					Operation: transition.Initialize, Target: "open", Status: transition.Waiting, Notify: true,
					AcceptedAt: now, Deadline: now.Add(time.Hour),
					Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
				}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			config := transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Hour, Clock: clock}
			coordinator, err := transition.NewCoordinator[string, string](store, unknownClassifier{}, config)
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
			domain := &host{}
			commits, err := transition.NewCommitCoordinator[string, string](store, domain, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := commits.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			lease, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			allowed := true
			denied := errors.New("control denied")
			controls, err := transition.NewAuthorizedControls[string](store, func(context.Context, transition.ControlAuthorization) error {
				if !allowed {
					return denied
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			control := transition.NotificationControl{ID: "repair", Action: action}
			if err := controls.Notification(ctx, lease, clock, control); !errors.Is(err, transition.ErrInvalidHook) {
				t.Fatalf("control before notification repair was accepted: %v", err)
			}
			handoff := &repairHandoff{}
			notifications, err := transition.NewNotificationCoordinator[string, string](store, handoff, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := notifications.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			record, err := store.InspectExecution(ctx, "execution")
			if err != nil || record.Status != transition.NotificationRepair || domain.current != "open" || domain.writes != 1 {
				t.Fatalf("handoff failure changed committed state or failed to enter repair: %+v, %v", record, err)
			}
			allowed = false
			if err := controls.Notification(ctx, lease, clock, control); !errors.Is(err, denied) {
				t.Fatalf("unauthorized repair was accepted: %v", err)
			}
			record, err = store.InspectExecution(ctx, "execution")
			if err != nil || record.Status != transition.NotificationRepair || len(record.NotificationControls) != 0 {
				t.Fatalf("denied repair changed execution: %+v, %v", record, err)
			}
			allowed = true
			if err := controls.Notification(ctx, lease, clock, control); err != nil {
				t.Fatal(err)
			}
			handoff.available = true
			if err := notifications.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			record, err = store.InspectExecution(ctx, "execution")
			wantHandoffs := 1
			if action == transition.WaiveNotification {
				wantHandoffs = 0
			}
			if err != nil || record.Status != transition.Complete || handoff.delivered != wantHandoffs ||
				domain.writes != 1 || domain.current != "open" {
				t.Fatalf("repair did not settle without changing the commit: %+v handoffs=%d error=%v", record, handoff.delivered, err)
			}
			allowed = false
			if err := controls.Notification(ctx, lease, clock, control); !errors.Is(err, denied) {
				t.Fatalf("repair replay bypassed authorization: %v", err)
			}
			allowed = true
			if err := controls.Notification(ctx, lease, clock, control); err != nil {
				t.Fatalf("settled repair could not replay: %v", err)
			}
		})
	}
}
