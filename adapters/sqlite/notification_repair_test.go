package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type repairHandoff struct{ available bool }

type toggleStateCodec struct {
	sqlite.StringCodec
	available *bool
}

func (c toggleStateCodec) Decode(data []byte) (string, error) {
	if !*c.available {
		return "", errors.New("state codec unavailable")
	}
	return string(data), nil
}

func (h *repairHandoff) Handoff(
	_ context.Context, _ transition.TransitionLease[string], _ transition.NotificationIntent[string, string],
) error {
	if !h.available {
		return &transition.NotificationHandoffFailure{Failure: fsm.Failure{Code: "outbox-unavailable"}}
	}
	return nil
}

func TestNotificationRepairRequiresAuthorizedRetryOrWaiver(t *testing.T) {
	for _, action := range []transition.NotificationControlAction{transition.RetryNotification, transition.WaiveNotification} {
		t.Run(string(action), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "repair.db")
			stateAvailable := true
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, toggleStateCodec{available: &stateAvailable})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := fixedClock(now)
			_, _, err = store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: "execution",
					Entity: "entity", Key: "key", Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
					Target: "target", Status: transition.Waiting,
					Notify: true, AcceptedAt: now, Deadline: now.Add(time.Hour), Policy: fsm.Policy{
						MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour,
					},
				}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			fence, err := store.AcquireTransitionLease(ctx, "entity", "execution", "coordinator", clock, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
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
			operation, err := store.MarkWorkSuccess(ctx, fence, clock)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
				ExecutionID: "execution", OperationID: operation, Outcome: transition.CommitSucceeded,
			}); err != nil {
				t.Fatal(err)
			}
			control := transition.NotificationControl{ID: "repair", Action: action}
			denied := errors.New("not authorized")
			allowed := true
			authorize := func(_ context.Context, request transition.ControlAuthorization) error {
				if request.ExecutionID != "execution" || request.RequestID != control.ID {
					t.Fatalf("incorrect authorization request: %+v", request)
				}
				if !allowed {
					return denied
				}
				return nil
			}
			controls, err := transition.NewAuthorizedControls[string](store, authorize)
			if err != nil {
				t.Fatal(err)
			}
			if err := controls.Notification(ctx, fence, clock, control); !errors.Is(err, transition.ErrInvalidHook) {
				t.Fatalf("control before repair should be rejected: %v", err)
			}
			host := &repairHandoff{}
			coordinator, err := transition.NewNotificationCoordinator[string, string](store, host, transition.CoordinatorConfig{
				ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			execution, err := store.ReadActiveExecution(ctx, "execution")
			if err != nil || execution.Status != transition.NotificationRepair || execution.NotificationFailure == nil ||
				execution.NotificationFailure.Code != "outbox-unavailable" {
				t.Fatalf("definitive failure did not enter repair: execution=%+v error=%v", execution, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, toggleStateCodec{available: &stateAvailable})
			if err != nil {
				t.Fatal(err)
			}
			controls, err = transition.NewAuthorizedControls[string](store, authorize)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err = transition.NewNotificationCoordinator[string, string](store, host, transition.CoordinatorConfig{
				ID: "coordinator", LeaseTTL: time.Hour, Clock: clock,
			})
			if err != nil {
				t.Fatal(err)
			}
			execution, err = store.ReadActiveExecution(ctx, "execution")
			if err != nil || execution.Status != transition.NotificationRepair || execution.CommitReceipt == nil ||
				execution.CommitReceipt.Outcome != transition.CommitSucceeded {
				t.Fatalf("restart lost post-commit repair state: execution=%+v error=%v", execution, err)
			}
			stale := fence
			stale.Generation++
			if err := controls.Notification(ctx, stale, clock, control); !errors.Is(err, transition.ErrStaleLease) {
				t.Fatalf("stale repair control should be rejected: %v", err)
			}
			allowed = false
			if err := controls.Notification(ctx, fence, clock, control); !errors.Is(err, denied) {
				t.Fatalf("unauthorized repair control should be rejected: %v", err)
			}
			afterDenial, err := store.InspectExecution(ctx, "execution")
			if err != nil || afterDenial.Status != transition.NotificationRepair || len(afterDenial.NotificationControls) != 0 {
				t.Fatalf("denied repair control changed the execution: inspection=%+v error=%v", afterDenial, err)
			}
			allowed = true
			stateAvailable = false
			if err := controls.Notification(ctx, fence, clock, control); !errors.Is(err, transition.ErrCompatibility) {
				t.Fatalf("unavailable state did not block repair control: %v", err)
			}
			blocked, err := store.InspectExecution(ctx, "execution")
			if err != nil || blocked.Status != transition.NotificationRepair || len(blocked.NotificationControls) != 0 ||
				blocked.Compatibility == nil || blocked.Compatibility.Component != transition.CompatibilityState {
				t.Fatalf("incompatible repair control changed the execution: inspection=%+v error=%v", blocked, err)
			}
			stateAvailable = true
			if err := store.RevalidateCompatibility(ctx, fence, clock, *blocked.Compatibility, nil); err != nil {
				t.Fatal(err)
			}
			if err := controls.Notification(ctx, fence, clock, control); err != nil {
				t.Fatal(err)
			}
			host.available = true
			if err := coordinator.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			replayed, created, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return nil, nil
			})
			if err != nil || created || replayed.Status != transition.Complete ||
				replayed.NotificationWaived != (action == transition.WaiveNotification) {
				t.Fatalf("repair control did not complete as requested: execution=%+v error=%v", replayed, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			controls, err = transition.NewAuthorizedControls[string](store, func(context.Context, transition.ControlAuthorization) error {
				if !allowed {
					return denied
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			allowed = false
			if err := controls.Notification(ctx, fence, clock, control); !errors.Is(err, denied) {
				t.Fatalf("settled notification replay bypassed authorization: %v", err)
			}
			allowed = true
			if err := controls.Notification(ctx, fence, clock, control); err != nil {
				t.Fatalf("settled notification control did not replay: %v", err)
			}
			if err := controls.Notification(ctx, fence, clock, transition.NotificationControl{ID: control.ID, Action: transition.WaiveNotification}); action == transition.RetryNotification && !errors.Is(err, transition.ErrConflict) {
				t.Fatalf("changed control replay did not conflict: %v", err)
			}
		})
	}
}
