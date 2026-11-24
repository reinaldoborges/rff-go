package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLifecycleNotificationConfigurationIsExplicitAndValidated(t *testing.T) {
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
	if err := l.EnableNotifications(nil, nil, nil, nil, transition.LifecycleNotificationConfig{}); err == nil {
		t.Fatal("incomplete notification configuration accepted")
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	for _, path := range l.Status().Paths {
		if path.Name == "notification-handoff" || path.Name == "notification-worker-0" {
			t.Fatalf("disabled notification path started: %+v", path)
		}
	}
	if err := l.EnableNotifications(nil, nil, nil, nil, transition.LifecycleNotificationConfig{}); err == nil {
		t.Fatal("notification configuration accepted after startup")
	}
}

type lifecycleNotificationStore struct {
	transition.NotificationStore[string, string]
	transition.NotificationActivityStore[string, string]
	fail atomic.Bool
}

func (*lifecycleNotificationStore) ActiveExecutionIDs(context.Context) ([]string, error) {
	return nil, nil
}

var errNotificationClaim = errors.New("private notification claim failure")

func (s *lifecycleNotificationStore) ClaimNotification(context.Context, string, func() time.Time, time.Duration) (transition.NotificationDelivery[string, string], error) {
	if s.fail.Load() {
		return transition.NotificationDelivery[string, string]{}, errNotificationClaim
	}
	return transition.NotificationDelivery[string, string]{}, transition.ErrNoNotificationActivity
}

type lifecycleNotificationHandoff struct{}

func (lifecycleNotificationHandoff) Handoff(context.Context, transition.TransitionLease[string], transition.NotificationIntent[string, string]) error {
	panic("no executions")
}

func TestLifecycleNotificationFailureIsIsolatedAndRestartable(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	transitions := &lifecycleTestStore{}
	notifications := &lifecycleNotificationStore{}
	notifications.fail.Store(true)
	l, err := transition.NewLifecycle[string, string, string, int](transitions, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	config := transition.LifecycleNotificationConfig{Capacity: 1, Worker: transition.NotificationWorkerConfig{
		ID: "notification", LeaseTTL: time.Second, RenewalInterval: time.Millisecond,
		AttemptTimeout: time.Second, RetryDelay: time.Millisecond, Clock: transition.SystemWorkerClock{},
	}}
	deliver := func(context.Context, transition.NotificationDelivery[string, string]) error { return nil }
	for _, invalid := range []transition.LifecycleNotificationConfig{
		func() transition.LifecycleNotificationConfig { c := config; c.Capacity = 0; return c }(),
		func() transition.LifecycleNotificationConfig { c := config; c.Worker.RetryDelay = 0; return c }(),
	} {
		if err := l.EnableNotifications(notifications, lifecycleNotificationHandoff{}, notifications, deliver, invalid); err == nil {
			t.Fatal("invalid notification settings accepted")
		}
	}
	if err := l.EnableNotifications(notifications, lifecycleNotificationHandoff{}, notifications, deliver, config); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		var stopped bool
		for _, path := range l.Status().Paths {
			if path.Name == "notification-worker-0" && !path.Running && path.Classification == transition.PathErrorUnclassified {
				stopped = true
			}
		}
		if stopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification failure did not stop path: %+v", l.Status())
		}
		time.Sleep(time.Millisecond)
	}
	if l.Status().Ready || !errors.Is(l.Err(), errNotificationClaim) {
		t.Fatalf("notification failure was not surfaced: %+v %v", l.Status(), l.Err())
	}
	before := transitions.scans.Load()
	l.Wakeup()
	for transitions.scans.Load() == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if transitions.scans.Load() == before {
		t.Fatal("notification failure stopped transition scans")
	}
	notifications.fail.Store(false)
	if err := l.RestartPath("notification-worker-0"); err != nil {
		t.Fatal(err)
	}
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("restart did not restore readiness: %+v", l.Status())
	}
}
