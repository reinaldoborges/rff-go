package continuationcontract

import (
	"context"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// PrepareOrigin creates an accepted execution at its durable commit boundary.
func PrepareOrigin(t *testing.T, store Store, marker transition.ContinuationMarker[string, string]) transition.ContinuationMarker[string, string] {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, marker.Entity, "key-"+marker.ExecutionID, func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: marker.ExecutionID, Entity: marker.Entity,
			Key: "key-" + marker.ExecutionID, Operation: transition.Initialize, Target: marker.Target,
			Status: transition.Waiting, MachineID: marker.MachineID, MachineVersion: marker.MachineVersion,
			Policy: marker.Policy, Continuation: &transition.ContinuationSnapshot{
				MachineID: marker.MachineID, MachineVersion: marker.MachineVersion, Policy: marker.Policy,
			},
			AcceptedAt: now, Deadline: now.Add(time.Hour),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, marker.Entity, marker.ExecutionID, "owner", clock, time.Hour)
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
	marker.OperationID, err = store.MarkWorkSuccess(ctx, lease, clock)
	if err != nil {
		t.Fatal(err)
	}
	return marker
}
