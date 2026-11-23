// Package continuationcontract supplies shared adapter tests for discovery.
package continuationcontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type Store interface {
	transition.AcceptanceStore[string, string]
	transition.TransitionLeaseStore[string]
	transition.HookDispatcher[string]
	transition.WorkSuccessStore[string]
	transition.ContinuationMarkerStore[string, string]
	ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error)
	StartHook(context.Context, transition.HookLease, func() time.Time) (transition.StartedHook, error)
	ReportHook(context.Context, transition.HookLease, transition.HookResult, func() time.Time) error
	RecordCommitReceipt(context.Context, transition.TransitionLease[string], func() time.Time, transition.CommitReceipt) error
	ReadActiveExecution(context.Context, string) (transition.Execution[string, string], error)
	CompleteNotificationHandoff(context.Context, transition.TransitionLease[string], func() time.Time, string) error
	ContinuationReady(context.Context, string) (bool, error)
}

func RunIdentitySafety(t *testing.T, store Store) {
	t.Helper()
	tests := []struct {
		name, id, machine string
	}{
		{name: "binary execution ID", id: string([]byte{0xff, 'a'}), machine: "plan"},
		{name: "binary machine ID", id: "valid", machine: string([]byte{0xfe, 'a'})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute}
			accepted, created, err := store.Accept(context.Background(), tt.name, tt.name, func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion, ID: tt.id, Entity: tt.name, Key: tt.name,
					Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
					MachineID: tt.machine, MachineVersion: "1", Policy: policy,
					Continuation: &transition.ContinuationSnapshot{MachineID: tt.machine, MachineVersion: "1", Policy: policy},
					AcceptedAt:   now, Deadline: now.Add(time.Minute),
				}, nil
			})
			if err != nil || !created || accepted.ID != tt.id || accepted.MachineID != tt.machine || accepted.Continuation.MachineID != tt.machine {
				t.Fatalf("execution identity changed on acceptance: %+v, %v", accepted, err)
			}
			replayed, created, err := store.Accept(context.Background(), tt.name, tt.name, func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				if view.Existing == nil {
					t.Fatal("missing existing execution")
				}
				return nil, nil
			})
			if err != nil || created || replayed.ID != tt.id || replayed.MachineID != tt.machine || replayed.Continuation.MachineID != tt.machine {
				t.Fatalf("execution identity changed on replay: %+v, %v", replayed, err)
			}
		})
	}
}

func RunReadiness(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour}
	_, _, err := store.Accept(ctx, "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "origin", Entity: "entity", Key: "key",
			Operation: transition.Initialize, Target: "due", Status: transition.Waiting,
			MachineID: "plan", MachineVersion: "1", Notify: true, Policy: policy,
			Continuation: &transition.ContinuationSnapshot{MachineID: "plan", MachineVersion: "1", Policy: policy},
			AcceptedAt:   now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	premature := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", OperationID: "not-yet-committable", Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1", Policy: policy,
	}
	if err := store.RecordContinuationMarker(ctx, premature); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("marker before commit operation must be rejected: %v", err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "entity", "origin", "owner", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
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
	operation, err := store.MarkWorkSuccess(ctx, lease, clock)
	if err != nil {
		t.Fatal(err)
	}
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", OperationID: operation, Entity: "entity", Target: "due",
		MachineID: "plan", MachineVersion: "1", Policy: policy,
	}
	if err := store.RecordContinuationMarker(ctx, marker); err != nil {
		t.Fatal(err)
	}
	if ready, err := store.ContinuationReady(ctx, "origin"); err != nil || ready {
		t.Fatalf("missing receipt must block: ready=%v err=%v", ready, err)
	}
	rejection := transition.CommitReceipt{ExecutionID: "origin", OperationID: operation, Outcome: transition.CommitDenied, Failure: &fsm.Failure{Code: "denied"}}
	if err := store.RecordCommitReceipt(ctx, lease, clock, rejection); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("contradictory rejection must be reported: %v", err)
	}
	receipt := transition.CommitReceipt{ExecutionID: "origin", OperationID: operation, Outcome: transition.CommitSucceeded}
	if err := store.RecordCommitReceipt(ctx, lease, clock, receipt); err != nil {
		t.Fatal(err)
	}
	if ready, err := store.ContinuationReady(ctx, "origin"); err != nil || ready {
		t.Fatalf("held notification reservation must block: ready=%v err=%v", ready, err)
	}
	execution, err := store.ReadActiveExecution(ctx, "origin")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteNotificationHandoff(ctx, lease, clock, execution.NotificationIntentID); err != nil {
		t.Fatal(err)
	}
	if ready, err := store.ContinuationReady(ctx, "origin"); err != nil || !ready {
		t.Fatalf("released origin must be ready: ready=%v err=%v", ready, err)
	}
}
