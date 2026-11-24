package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type occupiedHookStore struct {
	lifecycleTestStore
	activity  *oneActivityStore
	claims    atomic.Int64
	checks    atomic.Int64
	checkFail atomic.Bool
}

var errHookInventory = errors.New("hook inventory unavailable")

func (s *occupiedHookStore) ClaimHook(ctx context.Context, owner string, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	s.claims.Add(1)
	return s.activity.ClaimHook(ctx, owner, clock, ttl)
}

func (s *occupiedHookStore) InspectHook(ctx context.Context, lease transition.HookLease, clock func() time.Time) (transition.Execution[string, string], transition.HookCall, error) {
	return s.activity.InspectHook(ctx, lease, clock)
}

func (s *occupiedHookStore) StartHook(ctx context.Context, lease transition.HookLease, clock func() time.Time) (transition.StartedHook, error) {
	return s.activity.StartHook(ctx, lease, clock)
}

func (s *occupiedHookStore) RenewHook(ctx context.Context, lease transition.HookLease, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	return s.activity.RenewHook(ctx, lease, clock, ttl)
}

func (*occupiedHookStore) ReportHook(context.Context, transition.HookLease, transition.HookResult, func() time.Time) error {
	return errors.New("report failed")
}

func (s *occupiedHookStore) BlockHookCompatibility(ctx context.Context, lease transition.HookLease, diagnostic transition.CompatibilityDiagnostic, clock func() time.Time) error {
	return s.activity.BlockHookCompatibility(ctx, lease, diagnostic, clock)
}

func (s *occupiedHookStore) CheckHookActivities(context.Context) error {
	s.checks.Add(1)
	if s.checkFail.Load() {
		return errHookInventory
	}
	return nil
}

func TestLifecycleRestartWithOccupiedHookChecksStoreWithoutClaiming(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		close(started)
		<-release
		return nil
	}})
	record.Policy.AttemptTimeout = 30 * time.Millisecond
	record.Deadline = time.Now().Add(time.Minute)
	store := &occupiedHookStore{activity: &oneActivityStore{record: record, call: call}}
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
		ClassifyError: func(err error) transition.PathErrorClass {
			if errors.Is(err, errHookInventory) {
				return transition.PathErrorRecoverable
			}
			return transition.PathErrorUnclassified
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_ = l.Shutdown(ctx)
		cancel()
		close(release)
		_ = l.Shutdown(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("hook did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		path := l.Status().Paths[1]
		if !path.Running && path.Classification == transition.PathErrorUnclassified {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("report failure did not stop worker: %+v", l.Status())
		}
		time.Sleep(time.Millisecond)
	}
	store.checkFail.Store(true)
	if err := l.RestartPath("hook-worker-0"); err != nil {
		t.Fatal(err)
	}
	for {
		path := l.Status().Paths[1]
		if path.Running && path.Classification == transition.PathErrorRecoverable && store.checks.Load() > 0 {
			if path.Checked || l.Status().Ready || !errors.Is(l.Err(), errHookInventory) {
				t.Fatalf("failed inventory check reported healthy: %+v", l.Status())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capacity blocked inventory check: %+v", l.Status())
		}
		time.Sleep(time.Millisecond)
	}
	store.checkFail.Store(false)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	status := l.Status()
	if !status.Ready || status.Paths[1].Classification != "" || status.Capacity.Available != 0 ||
		store.checks.Load() < 2 || store.claims.Load() != 1 || !errors.Is(l.Err(), errHookInventory) {
		t.Fatalf("occupied hook prevented readiness or caused another claim: %+v", status)
	}
}

type joinedFailureStore struct {
	lifecycleTestStore
}

type invalidHookStore struct {
	lifecycleTestStore
}

type joinedEmptyStore struct {
	lifecycleTestStore
}

func (*joinedEmptyStore) ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error) {
	return transition.HookLease{}, errors.Join(transition.ErrNoHookActivity, errors.New("private failure"))
}

func TestLifecycleDoesNotHideJoinedClaimFailure(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&joinedEmptyStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		path := l.Status().Paths[1]
		if !path.Running {
			if path.Classification != transition.PathErrorUnclassified {
				t.Fatalf("unexpected classification: %+v", path)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("joined claim failure was hidden")
}

func (*invalidHookStore) ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error) {
	return transition.HookLease{}, transition.ErrInvalidHook
}

func TestLifecycleFrameworkFatalClassificationCannotBeOverridden(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&invalidHookStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:   transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:        transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval:  10 * time.Millisecond,
		ClassifyError: func(error) transition.PathErrorClass { return transition.PathErrorRecoverable },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		path := l.Status().Paths[1]
		if !path.Running {
			if path.Classification != transition.PathErrorFatal {
				t.Fatalf("framework error misclassified: %+v", path)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("framework fatal error was retried")
}

func TestLifecycleStopsFatalWorkerAndRestartsWithoutStoppingScans(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleTestStore{}
	store.claimFails.Store(true)
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:   transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:        transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval:  10 * time.Millisecond,
		ClassifyError: func(error) transition.PathErrorClass { return transition.PathErrorFatal },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		status := l.Status()
		if !status.Paths[1].Running {
			if status.Ready || status.Paths[1].Classification != transition.PathErrorFatal || status.Capacity.Available != 0 {
				t.Fatalf("unexpected stopped status: %+v", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fatal worker failure was not stopped")
		}
		time.Sleep(time.Millisecond)
	}
	scans := store.scans.Load()
	l.Wakeup()
	for store.scans.Load() == scans && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.scans.Load() == scans {
		t.Fatal("independent transition path stopped")
	}
	store.claimFails.Store(false)
	if err := l.RestartPath("hook-worker-0"); err != nil {
		t.Fatal(err)
	}
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("worker restart did not restore readiness: %+v", l.Status())
	}
}

func (s *joinedFailureStore) ClaimHook(context.Context, string, func() time.Time, time.Duration) (transition.HookLease, error) {
	return transition.HookLease{}, errors.Join(transition.ErrStaleActivityLease, errors.New("private failure"))
}

func TestLifecycleDoesNotRetryUnclassifiedJoinedError(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &joinedFailureStore{}
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := l.Status()
		for _, path := range status.Paths {
			if path.Name == "hook-worker-0" && !path.Running {
				if path.Classification != transition.PathErrorUnclassified || status.Ready {
					t.Fatalf("unexpected status: %+v", status)
				}
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("joined error did not stop path: %+v", l.Status())
}

func TestLifecycleIsolatesInitialFailureAndRestartsStoppedPaths(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	store := &lifecycleTestStore{}
	store.fail.Store(true)
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); !errors.Is(err, errLifecycleScan) {
		t.Fatalf("initial failure not surfaced: %v", err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for store.claims.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.claims.Load() < 2 {
		t.Fatal("independent worker stopped")
	}
	scans := store.scans.Load()
	time.Sleep(30 * time.Millisecond)
	if store.scans.Load() != scans {
		t.Fatal("unclassified failures retried without explicit restart")
	}
	status := l.Status()
	if status.Ready || strings.Contains(fmt.Sprintf("%+v", status), errLifecycleScan.Error()) {
		t.Fatalf("unsafe or ready status after failure: %+v", status)
	}
	store.fail.Store(false)
	for _, name := range []string{"transition"} {
		if err := l.RestartPath(name); err != nil {
			t.Fatalf("restart %s: %v", name, err)
		}
	}
	deadline = time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("restart did not restore readiness: %+v", l.Status())
	}
}
