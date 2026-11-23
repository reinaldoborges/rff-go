package continuationcontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type pagedHost struct {
	markers      []transition.ContinuationMarker[string, string]
	acknowledged map[string]bool
	failPage     bool
	failFirstAck bool
}

func (h *pagedHost) ListContinuationMarkers(_ context.Context, cursor string) ([]transition.ContinuationMarker[string, string], string, error) {
	if cursor == "second" && h.failPage {
		h.failPage = false
		return nil, "", errors.New("page unavailable")
	}
	index, next := 0, "second"
	if cursor == "second" {
		index, next = 1, ""
	}
	marker := h.markers[index]
	if h.acknowledged[marker.ExecutionID] {
		return nil, next, nil
	}
	return []transition.ContinuationMarker[string, string]{marker}, next, nil
}

func (h *pagedHost) AcknowledgeContinuationMarker(_ context.Context, marker transition.ContinuationMarker[string, string]) error {
	if h.failFirstAck {
		h.failFirstAck = false
		return errors.New("acknowledgement unavailable")
	}
	h.acknowledged[marker.ExecutionID] = true
	return nil
}

func RunPagination(t *testing.T, store interface {
	transition.ContinuationMarkerStore[string, string]
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error)
}) {
	t.Helper()
	ctx := context.Background()
	policy := fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute}
	first := transition.ContinuationMarker[string, string]{SchemaVersion: 1, ExecutionID: "first", OperationID: "commit-1", Entity: "one", Target: "due", MachineID: "plan", MachineVersion: "1", Policy: policy}
	second := first
	second.ExecutionID, second.OperationID, second.Entity = "second", "commit-2", "two"
	host := &pagedHost{markers: []transition.ContinuationMarker[string, string]{first, second}, acknowledged: make(map[string]bool), failPage: true, failFirstAck: true}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(ctx); err == nil {
		t.Fatal("expected page and acknowledgement failures")
	}
	markers, err := store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 1 || host.acknowledged["first"] {
		t.Fatalf("failed page or acknowledgement lost progress: markers=%+v acknowledgements=%v err=%v", markers, host.acknowledged, err)
	}
	if err := discovery.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	markers, err = store.ContinuationMarkers(ctx)
	if err != nil || len(markers) != 2 || !host.acknowledged["first"] || !host.acknowledged["second"] {
		t.Fatalf("replay did not fill gap: markers=%+v acknowledgements=%v err=%v", markers, host.acknowledged, err)
	}
}
