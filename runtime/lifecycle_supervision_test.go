package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

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
