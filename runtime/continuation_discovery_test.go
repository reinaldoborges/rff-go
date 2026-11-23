package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func continuationMarker(id string) transition.ContinuationMarker[string, string] {
	return transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: id, OperationID: "commit-" + id,
		Entity: "entity-" + id, Target: "due", MachineID: "machine", MachineVersion: "v1",
		Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
	}
}

type continuationDiscoveryHost struct {
	markers []transition.ContinuationMarker[string, string]
	acked   map[string]bool
	failAck bool
}

func (h *continuationDiscoveryHost) ListContinuationMarkers(_ context.Context, cursor string) ([]transition.ContinuationMarker[string, string], string, error) {
	switch cursor {
	case "":
		return h.markers[:1], "second", nil
	case "second":
		return h.markers[1:], "", nil
	default:
		return nil, "", errors.New("unknown cursor")
	}
}

func (h *continuationDiscoveryHost) AcknowledgeContinuationMarker(_ context.Context, marker transition.ContinuationMarker[string, string]) error {
	if h.failAck {
		return errors.New("injected acknowledgement failure")
	}
	h.acked[marker.ExecutionID] = true
	return nil
}

func TestContinuationDiscoveryReplaysAfterLostAcknowledgement(t *testing.T) {
	host := &continuationDiscoveryHost{
		markers: []transition.ContinuationMarker[string, string]{
			continuationMarker("first"),
			continuationMarker("second"),
		},
		acked: make(map[string]bool), failAck: true,
	}
	store := &continuationDiscoveryStore{records: make(map[string]transition.ContinuationMarker[string, string])}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(context.Background()); err == nil || len(store.records) != 2 || len(host.acked) != 0 {
		t.Fatalf("failed acknowledgement must leave durable records replayable: err=%v records=%v", err, store.records)
	}
	host.failAck = false
	if err := discovery.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.records) != 2 || len(host.acked) != 2 {
		t.Fatalf("replay did not acknowledge existing records: acked=%v records=%v", host.acked, store.records)
	}
}

type continuationDiscoveryStore struct {
	records map[string]transition.ContinuationMarker[string, string]
	fail    bool
	mutate  bool
}

func (s *continuationDiscoveryStore) RecordContinuationMarker(_ context.Context, marker transition.ContinuationMarker[string, string]) error {
	if marker.ExecutionID == "first" && s.fail {
		return errors.New("injected persistence failure")
	}
	if prior, ok := s.records[marker.ExecutionID]; ok {
		return transition.MatchContinuationMarkers(prior, marker)
	}
	s.records[marker.ExecutionID] = marker
	if s.mutate {
		marker.Policy.RetryDelays[0] = time.Hour
	}
	return nil
}

func TestContinuationDiscoveryDoesNotLoseFailedPageGap(t *testing.T) {
	host := &continuationDiscoveryHost{
		markers: []transition.ContinuationMarker[string, string]{
			continuationMarker("first"),
			continuationMarker("second"),
		},
		acked: make(map[string]bool),
	}

	store := &continuationDiscoveryStore{records: make(map[string]transition.ContinuationMarker[string, string]), fail: true}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}

	if err := discovery.Reconcile(context.Background()); err == nil {
		t.Fatal("expected the first marker's persistence failure")
	}
	if host.acked["first"] || !host.acked["second"] || len(store.records) != 1 {
		t.Fatalf("failed marker was acknowledged or unrelated marker lost: acked=%v records=%v", host.acked, store.records)
	}
	store.fail = false
	if err := discovery.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !host.acked["first"] || len(store.records) != 2 {
		t.Fatalf("failed gap was not retried: acked=%v records=%v", host.acked, store.records)
	}
}

func TestContinuationDiscoveryDoesNotAcknowledgeUnsupportedMarker(t *testing.T) {
	first := continuationMarker("first")
	first.SchemaVersion = 2
	host := &continuationDiscoveryHost{
		markers: []transition.ContinuationMarker[string, string]{first, continuationMarker("second")},
		acked:   make(map[string]bool),
	}
	store := &continuationDiscoveryStore{records: make(map[string]transition.ContinuationMarker[string, string])}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(context.Background()); err == nil {
		t.Fatal("expected an incompatible marker error")
	}
	if host.acked["first"] || !host.acked["second"] || len(store.records) != 1 {
		t.Fatalf("invalid marker was persisted or acknowledged: acked=%v records=%v", host.acked, store.records)
	}
}

func TestContinuationDiscoveryDoesNotMutateSourceMarker(t *testing.T) {
	first := continuationMarker("first")
	first.Policy.RetryDelays = []time.Duration{time.Second}
	second := continuationMarker("second")
	second.Policy.RetryDelays = []time.Duration{time.Second}
	host := &continuationDiscoveryHost{
		markers: []transition.ContinuationMarker[string, string]{first, second},
		acked:   make(map[string]bool),
	}
	store := &continuationDiscoveryStore{records: make(map[string]transition.ContinuationMarker[string, string]), mutate: true}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if host.markers[0].Policy.RetryDelays[0] != time.Second || host.markers[1].Policy.RetryDelays[0] != time.Second {
		t.Fatal("store mutated source marker before acknowledgement")
	}
}

type loopingMarkerSource struct{}

func (loopingMarkerSource) ListContinuationMarkers(context.Context, string) ([]transition.ContinuationMarker[string, string], string, error) {
	return nil, "repeated", nil
}

func (loopingMarkerSource) AcknowledgeContinuationMarker(context.Context, transition.ContinuationMarker[string, string]) error {
	return nil
}

func TestContinuationDiscoveryRejectsCyclicPagination(t *testing.T) {
	store := &continuationDiscoveryStore{records: make(map[string]transition.ContinuationMarker[string, string])}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, loopingMarkerSource{})
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(context.Background()); err == nil {
		t.Fatal("expected repeated pagination cursor to fail")
	}
}

func TestContinuationDiscoveryLeavesConflictingMarkerUnacknowledged(t *testing.T) {
	first := continuationMarker("first")
	first.Target = "changed"
	host := &continuationDiscoveryHost{
		markers: []transition.ContinuationMarker[string, string]{first, continuationMarker("second")},
		acked:   make(map[string]bool),
	}
	store := &continuationDiscoveryStore{records: map[string]transition.ContinuationMarker[string, string]{"first": continuationMarker("first")}}
	discovery, err := transition.NewContinuationDiscovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Reconcile(context.Background()); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("expected conflicting marker: %v", err)
	}
	if host.acked["first"] || !host.acked["second"] || store.records["first"].Target != "due" {
		t.Fatalf("conflict overwrote or acknowledged the original: acked=%v records=%v", host.acked, store.records)
	}
}
