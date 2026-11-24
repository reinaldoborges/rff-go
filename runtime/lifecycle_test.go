package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLifecycleRejectsMissingDependencies(t *testing.T) {
	_, err := transition.NewLifecycle[string, string, string, int](nil, nil, nil, nil, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: time.Millisecond,
	})
	if err == nil {
		t.Fatal("missing dependencies were accepted")
	}
}

func TestLifecycleReportsInitialReadinessAndCapacity(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleTestStore{}
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if l.Status().Ready {
		t.Fatal("unstarted lifecycle is ready")
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	status := l.Status()
	if !status.Ready || status.Capacity.Available != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestLifecycleRejectsInvalidTimingAndCapacity(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleTestStore{}
	valid := transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	}
	for _, invalid := range []transition.LifecycleConfig{
		func() transition.LifecycleConfig { c := valid; c.PollInterval = 0; return c }(),
		func() transition.LifecycleConfig { c := valid; c.PollInterval = time.Second; return c }(),
		func() transition.LifecycleConfig { c := valid; c.Worker.Capacity = 0; return c }(),
		func() transition.LifecycleConfig { c := valid; c.Worker.RenewalInterval = c.Worker.LeaseTTL; return c }(),
	} {
		if _, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, invalid); err == nil {
			t.Fatalf("invalid lifecycle configuration accepted: %+v", invalid)
		}
	}
}

type lifecycleTestStore struct {
	transition.LifecycleStore[string, string]
	fail       atomic.Bool
	claimFails atomic.Bool
	scans      atomic.Int64
	claims     atomic.Int64
}

var errLifecycleScan = errors.New("scan failed")
var errLifecycleClaim = errors.New("claim failed")

func (s *lifecycleTestStore) ActiveExecutionIDs(context.Context) ([]string, error) {
	s.scans.Add(1)
	if s.fail.Load() {
		return nil, errLifecycleScan
	}
	return nil, nil
}

func TestLifecycleRetriesRecoverableInitialScans(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleTestStore{}
	store.fail.Store(true)
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 30 * time.Millisecond,
		ClassifyError: func(err error) transition.PathErrorClass {
			if errors.Is(err, errLifecycleScan) {
				return transition.PathErrorRecoverable
			}
			return transition.PathErrorUnclassified
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); !errors.Is(err, errLifecycleScan) {
		t.Fatalf("initial failure not surfaced: %v", err)
	}
	defer l.Shutdown(context.Background())
	if l.Status().Ready {
		t.Fatal("failed initial scan marked ready")
	}
	initial := store.scans.Load()
	for i := 0; i < 10; i++ {
		l.Wakeup()
	}
	time.Sleep(10 * time.Millisecond)
	if store.scans.Load() != initial {
		t.Fatal("retry ignored backoff")
	}
	store.fail.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("recovered scans never became ready: %+v", l.Status())
	}
}

func (s *lifecycleTestStore) ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error) {
	s.claims.Add(1)
	if s.claimFails.Load() {
		return transition.HookLease{}, errLifecycleClaim
	}
	return transition.HookLease{}, transition.ErrNoHookActivity
}

type lifecycleTestHost struct{}

func (lifecycleTestHost) Commit(context.Context, transition.TransitionLease[string], transition.Execution[string, string]) (transition.CommitReceipt, error) {
	panic("no executions")
}

func TestLifecycleExposesStartupAndBackgroundScanFailures(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleTestStore{}
	config := transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	}
	newLifecycle := func() *transition.Lifecycle[string, string, string, int] {
		l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, config)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	store.fail.Store(true)
	startup := newLifecycle()
	if err := startup.Start(context.Background()); !errors.Is(err, errLifecycleScan) {
		t.Fatalf("initial scan error not returned: %v", err)
	}
	if err := startup.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(false)
	l := newLifecycle()
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(true)
	l.Wakeup()
	select {
	case <-l.Errors():
		if !errors.Is(l.Err(), errLifecycleScan) {
			t.Fatalf("scan failure not retained: %v", l.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background scan failure was not signaled")
	}
	if err := l.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(false)
	store.claimFails.Store(true)
	l = newLifecycle()
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Errors():
		if !errors.Is(l.Err(), errLifecycleClaim) {
			t.Fatalf("claim failure not retained: %v", l.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker failure was not signaled")
	}
	if err := l.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
