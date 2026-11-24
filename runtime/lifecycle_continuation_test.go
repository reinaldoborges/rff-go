package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLifecycleContinuationRequiresCompleteConfiguration(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.EnableContinuation(nil, nil, transition.ContinuationDriverConfig[string, string, string, int]{}, nil, ""); err == nil {
		t.Fatal("incomplete continuation configuration accepted")
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	for _, path := range l.Status().Paths {
		if path.Name == "continuation" {
			t.Fatal("failed configuration enabled continuation")
		}
	}
}

type lifecycleContinuationStore struct {
	transition.ContinuationLifecycleStore[string, string]
	available  atomic.Bool
	failRecord atomic.Bool
	scans      atomic.Int64
	drives     atomic.Int64
	block      <-chan struct{}
	entered    chan<- struct{}
}

var errMarkerPersistence = transition.ErrContinuationOriginMissing

func (s *lifecycleContinuationStore) RecordContinuationMarker(context.Context, transition.ContinuationMarker[string, string]) error {
	if s.failRecord.Load() {
		return errMarkerPersistence
	}
	s.available.Store(true)
	return nil
}

func (s *lifecycleContinuationStore) ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, string], error) {
	s.scans.Add(1)
	if !s.available.Load() {
		return nil, nil
	}
	return []transition.ContinuationMarker[string, string]{{ExecutionID: "origin"}}, nil
}

func (s *lifecycleContinuationStore) ReadContinuation(context.Context, string) (transition.ContinuationProgress, error) {
	s.drives.Add(1)
	if s.block != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-s.block
	}
	return transition.ContinuationProgress{Phase: transition.ContinuationAccepted}, nil
}

type lifecycleContinuationHost struct {
	transition.ContinuationDeliveryHost[string, string]
}

type lifecycleContinuationCommitHost struct{ lifecycleTestHost }

func (lifecycleContinuationCommitHost) ContinuationCommitSupported() bool { return true }

type lifecycleMarkerSource struct {
	acked atomic.Bool
}

func (s *lifecycleMarkerSource) ListContinuationMarkers(context.Context, string) ([]transition.ContinuationMarker[string, string], string, error) {
	if s.acked.Load() {
		return nil, "", nil
	}
	return []transition.ContinuationMarker[string, string]{continuationMarker("origin")}, "", nil
}

func (s *lifecycleMarkerSource) AcknowledgeContinuationMarker(context.Context, transition.ContinuationMarker[string, string]) error {
	s.acked.Store(true)
	return nil
}

func TestLifecycleRetriesMarkerPersistenceBeforeAcknowledging(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleContinuationStore{}
	store.failRecord.Store(true)
	source := &lifecycleMarkerSource{}
	var registry fsm.Registry[string, string, string, int]
	input := func(transition.CodecVersion) (transition.Codec[int], error) { return intCodec{}, nil }
	event := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor[string, string, string, int](newMemoryStore(), &registry, input, event, func() (string, error) { return "next", nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	config := transition.ContinuationDriverConfig[string, string, string, int]{
		Clock: time.Now, LeaseDuration: time.Second,
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
		ResolveEvent: event, ResolveInput: input, Acceptor: acceptor, Host: lifecycleContinuationHost{},
		OriginRevision: func(context.Context, transition.ContinuationMarker[string, string]) ([]byte, error) {
			return []byte("revision"), nil
		},
	}
	noSupport, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := noSupport.EnableContinuation(store, &registry, config, source, "continuation-worker"); !errors.Is(err, transition.ErrContinuationUnavailable) {
		t.Fatalf("commit host without durable intent support accepted: %v", err)
	}
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleContinuationCommitHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.EnableContinuation(store, &registry, config, source, "continuation-worker"); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); !errors.Is(err, errMarkerPersistence) {
		t.Fatalf("initial failure not surfaced: %v", err)
	}
	defer l.Shutdown(context.Background())
	if source.acked.Load() {
		t.Fatal("marker acknowledged before persistence")
	}
	store.failRecord.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for (!source.acked.Load() || store.drives.Load() == 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !source.acked.Load() || store.drives.Load() == 0 {
		t.Fatal("replay did not persist, acknowledge, and drive marker")
	}
}

func TestLifecyclePollsContinuationWithoutWakeupAndAfterRestart(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleContinuationStore{}
	var registry fsm.Registry[string, string, string, int]
	input := func(transition.CodecVersion) (transition.Codec[int], error) { return intCodec{}, nil }
	event := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	acceptor, err := transition.NewAcceptor[string, string, string, int](newMemoryStore(), &registry, input, event, func() (string, error) { return "next", nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	newLifecycle := func() *transition.Lifecycle[string, string, string, int] {
		l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleContinuationCommitHost{}, transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
				RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = l.EnableContinuation(store, &registry, transition.ContinuationDriverConfig[string, string, string, int]{
			Clock: time.Now, LeaseDuration: time.Second,
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"}, InputCodec: transition.CodecVersion{Name: "input", Version: "1"},
			ResolveEvent: event, ResolveInput: input, Acceptor: acceptor, Host: lifecycleContinuationHost{},
			OriginRevision: func(context.Context, transition.ContinuationMarker[string, string]) ([]byte, error) {
				return []byte("revision"), nil
			},
		}, nil, "continuation-worker")
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	l := newLifecycle()
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.scans.Load() < 1 {
		t.Fatalf("initial inventory missing: %d", store.scans.Load())
	}
	store.available.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for store.drives.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.drives.Load() == 0 {
		t.Fatal("durable poll did not discover work")
	}
	if err := l.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := store.drives.Load()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	store.block, store.entered = release, entered
	l = newLifecycle()
	started := make(chan error, 1)
	go func() { started <- l.Start(context.Background()) }()
	select {
	case err := <-started:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("continuation driver blocked lifecycle startup")
	}
	defer l.Shutdown(context.Background())
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("startup did not start continuation driver")
	}
	close(release)
	if store.drives.Load() <= before {
		t.Fatal("restart did not scan durable work")
	}
	if err := l.EnableContinuation(nil, nil, transition.ContinuationDriverConfig[string, string, string, int]{}, nil, ""); !errors.Is(err, transition.ErrInvalidRequest) {
		t.Fatalf("invalid late configuration: %v", err)
	}
}
