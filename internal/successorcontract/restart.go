package successorcontract

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type RestartStore interface {
	Store
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
	ListExecutionIDs(context.Context) ([]string, error)
}

// Crash terminates the child after the selected durable protocol boundary.
func Crash(t *testing.T, store RestartStore, host *Host, boundary string) {
	t.Helper()
	binding := Setup(t, store, host, "restart")
	if boundary == "before-prepare" {
		os.Exit(87)
	}
	binding = PrepareBinding(t, store, binding)
	if boundary == "after-prepare" || boundary == "before-host" {
		os.Exit(87)
	}
	result, err := host.AcceptSuccessor(context.Background(), binding)
	if err != nil || result.Receipt == nil {
		t.Fatalf("host acceptance: %+v error=%v", result, err)
	}
	if boundary == "after-host" || boundary == "before-finalize" {
		os.Exit(87)
	}
	if _, err := store.FinalizeSuccessor(context.Background(), *result.Receipt); err != nil {
		t.Fatal(err)
	}
	if boundary == "after-finalize" {
		os.Exit(87)
	}
	t.Fatalf("unknown crash boundary %q", boundary)
}

func CheckRestart(t *testing.T, store RestartStore, host *Host) {
	t.Helper()
	ctx := context.Background()
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("origin markers after restart: %+v error=%v", markers, err)
	}
	marker := markers[0]
	inspection, err := store.InspectSuccessor(ctx, marker)
	if errors.Is(err, sql.ErrNoRows) {
		binding := PrepareBinding(t, store, BindingFor(marker))
		inspection = transition.SuccessorInspection[string, string]{Binding: binding, Phase: transition.SuccessorPrepared}
	} else if err != nil {
		t.Fatal(err)
	}
	recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Reconcile(ctx, marker, true); err != nil {
		t.Fatalf("recover successor: %v", err)
	}
	final, err := store.InspectSuccessor(ctx, marker)
	if err != nil || final.Phase != transition.SuccessorAccepted || final.Binding.Execution.ID != inspection.Binding.Execution.ID {
		t.Fatalf("recovered successor: %+v error=%v", final, err)
	}
	ids, err := store.ListExecutionIDs(ctx)
	if err != nil || len(ids) != 2 {
		t.Fatalf("expected origin and exactly one successor, ids=%v error=%v", ids, err)
	}
	result, err := host.LookupSuccessor(ctx, final.Binding)
	if err != nil || result.Outcome != transition.SuccessorHostAccepted {
		t.Fatalf("host receipt not retained: %+v error=%v", result, err)
	}
	if err := host.Write("restart", "outside", "revision-2"); err == nil {
		t.Fatal("direct writer bypassed fence after restart")
	}
}
