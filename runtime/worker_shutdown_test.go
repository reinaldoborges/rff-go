package runtime_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type manualTicker struct{ ticks chan time.Time }

func (t *manualTicker) Ticks() <-chan time.Time { return t.ticks }
func (*manualTicker) Stop()                     {}

type manualWorkerClock struct {
	now    time.Time
	ticker *manualTicker
}

func (c *manualWorkerClock) Now() time.Time { return c.now }
func (c *manualWorkerClock) NewTicker(time.Duration) transition.WorkerTicker {
	return c.ticker
}
func (*manualWorkerClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

type renewingStore struct {
	oneActivityStore
	renewed chan struct{}
}

func (s *renewingStore) RenewHook(ctx context.Context, lease transition.HookLease, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	renewed, err := s.oneActivityStore.RenewHook(ctx, lease, clock, ttl)
	s.renewed <- struct{}{}
	return renewed, err
}

func TestWorkerDrainsAndRenewsUntilShutdown(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now.Add(time.Hour)
	clock := &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}}
	store := &renewingStore{oneActivityStore: oneActivityStore{record: record, call: call}, renewed: make(chan struct{}, 1)}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	workDone := make(chan error, 1)
	go func() { workDone <- worker.RunOne(context.Background()) }()
	<-started
	if status := worker.Status(); !status.Ready || status.Active != 1 || status.Capacity != 1 {
		t.Fatalf("unexpected active worker status: %+v", status)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrWorkerAtCapacity) {
		t.Fatalf("worker exceeded configured capacity: %v", err)
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- worker.Shutdown(context.Background()) }()
	deadline := time.After(5 * time.Second)
	for !worker.Status().Draining {
		select {
		case <-deadline:
			t.Fatal("worker did not begin draining")
		default:
			runtime.Gosched()
		}
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrWorkerDraining) {
		t.Fatalf("draining worker accepted a claim: %v", err)
	}
	clock.ticker.ticks <- now
	select {
	case <-store.renewed:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not renew during drain")
	}
	close(release)
	if err := <-workDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if !store.reported || !store.result.Success {
		t.Fatal("drained successful hook was not reported")
	}
}

func TestWorkerShutdownCancellationDoesNotReportSuccess(t *testing.T) {
	started := make(chan struct{})
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now.Add(time.Hour)
	clock := &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}}
	store := &renewingStore{oneActivityStore: oneActivityStore{record: record, call: call}, renewed: make(chan struct{}, 1)}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	workDone := make(chan error, 1)
	go func() { workDone <- worker.RunOne(context.Background()) }()
	<-started
	shutdownCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := worker.Shutdown(shutdownCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown did not return its deadline error: %v", err)
	}
	if err := <-workDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker did not cancel its hook: %v", err)
	}
	if store.reported {
		t.Fatal("shutdown cancellation fabricated an outcome")
	}
}
