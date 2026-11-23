package successorcontract

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func CrashCancellation(t *testing.T, store RestartStore, host *Host, boundary string) {
	t.Helper()
	ctx := context.Background()
	binding := Prepare(t, store, host, "cancel-restart")
	if err := host.ClaimSuccessor(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if boundary == "before-host-cancel" {
		os.Exit(87)
	}
	if boundary == "during-host-cancel" {
		_, err := host.CancelPrepared(ctx, binding, func() error { os.Exit(87); return nil })
		t.Fatalf("cancellation transaction did not crash: %v", err)
	}
	host.LoseReply = true
	if _, err := host.CancelPrepared(ctx, binding, nil); err == nil {
		t.Fatal("cancellation response was not lost")
	}
	if boundary == "after-host-cancel" {
		os.Exit(87)
	}
	recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Reconcile(ctx, binding.Origin, false); !errors.Is(err, transition.ErrSuccessorCancelled) {
		t.Fatalf("reconcile cancellation: %v", err)
	}
	if boundary == "after-runtime-cancel" {
		os.Exit(87)
	}
	t.Fatalf("unknown cancellation crash boundary %q", boundary)
}

type unavailableCancellationHost struct{}

func (unavailableCancellationHost) LookupSuccessor(context.Context, Binding) (Result, error) {
	return Result{}, errors.New("host unavailable")
}
func (unavailableCancellationHost) AcceptSuccessor(context.Context, Binding) (Result, error) {
	return Result{}, errors.New("unexpected delivery")
}

func CheckCancellationRestart(t *testing.T, store RestartStore, host *Host, boundary string) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("origin markers after restart: %+v error=%v", markers, err)
	}
	marker := markers[0]
	inspection, err := store.InspectSuccessor(ctx, marker)
	if err != nil {
		t.Fatal(err)
	}
	binding := inspection.Binding
	if boundary != "after-runtime-cancel" && inspection.Phase != transition.SuccessorPrepared {
		t.Fatalf("runtime reservation released before reconciliation: %+v", inspection)
	}
	if boundary == "after-runtime-cancel" && inspection.Phase != transition.SuccessorCancelled {
		t.Fatalf("runtime cancellation was not durable: %+v", inspection)
	}
	if boundary != "after-runtime-cancel" {
		unavailable, err := transition.NewSuccessorRecovery[string, string](store, unavailableCancellationHost{})
		if err != nil {
			t.Fatal(err)
		}
		if err := unavailable.Reconcile(ctx, marker, true); err == nil {
			t.Fatal("unavailable host was treated as a definitive outcome")
		}
		_, _, err = store.Accept(ctx, marker.Entity, "competitor", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if !view.Active {
				t.Error("unresolved cancellation lost its runtime reservation")
			}
			return nil, transition.ErrBusy
		})
		if !errors.Is(err, transition.ErrBusy) {
			t.Fatalf("unresolved cancellation allowed a competing execution: %v", err)
		}
	}
	hostResult, err := host.LookupSuccessor(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	if boundary == "before-host-cancel" || boundary == "during-host-cancel" {
		if hostResult.Outcome != transition.SuccessorHostPending {
			t.Fatalf("uncommitted cancellation did not retain host claim: %+v", hostResult)
		}
		if err := host.Write(marker.Entity, "outside", "revision-2"); err == nil {
			t.Fatal("uncertain cancellation released the host writer fence")
		}
		recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
		if err != nil {
			t.Fatal(err)
		}
		if err := recovery.Reconcile(ctx, marker, true); !errors.Is(err, transition.ErrSuccessorPending) {
			t.Fatalf("pending claim must remain unresolved: %v", err)
		}
		failed := errors.New("cancellation failed before commit")
		if _, err := host.CancelPrepared(ctx, binding, func() error { return failed }); !errors.Is(err, failed) {
			t.Fatalf("failed cancellation: %v", err)
		}
		if err := host.Write(marker.Entity, "outside", "revision-2"); err == nil {
			t.Fatal("failed cancellation released the host fence")
		}
		stillPending, err := host.LookupSuccessor(ctx, binding)
		if err != nil || stillPending.Outcome != transition.SuccessorHostPending {
			t.Fatalf("failed cancellation changed host evidence: %+v error=%v", stillPending, err)
		}
		host.LoseReply = true
		if _, err := host.CancelPrepared(ctx, binding, nil); err == nil {
			t.Fatal("cancellation response was not lost")
		}
	}
	hostResult, err = host.AcceptSuccessor(ctx, binding)
	if err != nil || hostResult.Outcome != transition.SuccessorHostCanceled || hostResult.Cancellation == nil {
		t.Fatalf("late acceptance must observe cancellation tombstone: %+v error=%v", hostResult, err)
	}
	replayed, err := host.CancelPrepared(ctx, binding, nil)
	if err != nil || replayed.Outcome != transition.SuccessorHostCanceled || replayed.Cancellation == nil ||
		!bytes.Equal(replayed.Cancellation.Evidence, hostResult.Cancellation.Evidence) {
		t.Fatalf("host cancellation replay: %+v error=%v", replayed, err)
	}
	changed := binding
	changed.Execution.Input.Data = []byte("different")
	if _, err := host.AcceptSuccessor(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed request must conflict with host tombstone: %v", err)
	}
	ids, err := store.ListExecutionIDs(ctx)
	if err != nil || len(ids) != 1 || ids[0] != marker.ExecutionID {
		t.Fatalf("cancellation must not create a successor: %v error=%v", ids, err)
	}
	recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := recovery.Reconcile(ctx, marker, true); !errors.Is(err, transition.ErrSuccessorCancelled) {
			t.Fatalf("recover cancellation idempotently: %v", err)
		}
	}
	inspection, err = store.InspectSuccessor(ctx, marker)
	if err != nil || inspection.Phase != transition.SuccessorCancelled || len(inspection.Cancellation) == 0 {
		t.Fatalf("runtime cancellation evidence not durable: %+v error=%v", inspection, err)
	}
	if !bytes.Equal(inspection.Cancellation, hostResult.Cancellation.Evidence) {
		t.Fatal("runtime did not retain the authoritative host cancellation evidence")
	}
	if _, _, err := store.PrepareSuccessor(ctx, marker, func(*Binding) (*Binding, error) { return nil, nil }); !errors.Is(err, transition.ErrSuccessorCancelled) {
		t.Fatalf("cancelled successor prepared again: %v", err)
	}
	if err := host.Write(marker.Entity, "outside", "revision-2"); err != nil {
		t.Fatalf("fenced cancellation did not release host fence: %v", err)
	}
	_, created, err := store.Accept(ctx, marker.Entity, "competitor", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Active {
			t.Error("fenced cancellation did not release runtime reservation")
		}
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "competitor", Entity: marker.Entity,
			Key: "competitor", Status: transition.Waiting,
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("competitor after cancellation: created=%v error=%v", created, err)
	}
}
