package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type expiredAttemptClock struct{ *manualWorkerClock }

func (*expiredAttemptClock) WithTimeout(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, time.Unix(1, 0))
}

type controlledDeadline struct {
	context.Context
	done chan struct{}
}

func (c *controlledDeadline) Done() <-chan struct{} { return c.done }
func (c *controlledDeadline) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

type controlledTimeoutClock struct {
	*manualWorkerClock
	deadline *controlledDeadline
}

func (c *controlledTimeoutClock) WithTimeout(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	c.deadline.Context = ctx
	return c.deadline, func() {}
}

func TestWorkerTimeoutDoesNotWaitForUncooperativeHook(t *testing.T) {
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		close(started)
		<-release
		close(exited)
		return nil
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now.Add(time.Hour)
	clock := &controlledTimeoutClock{
		manualWorkerClock: &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}},
		deadline:          &controlledDeadline{done: make(chan struct{})},
	}
	store := &oneActivityStore{record: record, call: call}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	workDone := make(chan error, 1)
	go func() { workDone <- worker.RunOne(context.Background()) }()
	<-started
	close(clock.deadline.done)
	select {
	case err := <-workDone:
		if err != nil || !store.reported || store.result.Success || store.result.Failure.Code != "safe" {
			t.Errorf("timeout was not durably classified: result=%+v error=%v", store.result, err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		<-workDone
		t.Fatal("worker remained blocked and kept renewing after attempt timeout")
	}
	close(release)
	<-exited
	if store.result.Success {
		t.Fatal("late hook success overwrote the timeout")
	}
}

func TestWorkerKeepsCapacityUntilTimedOutHookActuallyExits(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		close(started)
		<-release
		return nil
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now.Add(time.Hour)
	clock := &controlledTimeoutClock{
		manualWorkerClock: &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}},
		deadline:          &controlledDeadline{done: make(chan struct{})},
	}
	store := &oneActivityStore{record: record, call: call}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.RunOne(context.Background()) }()
	<-started
	close(clock.deadline.done)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrWorkerAtCapacity) {
		t.Errorf("uncooperative hook did not retain capacity: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := worker.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("unfinished hook not reported at shutdown: %v", err)
	}
	close(release)
	if err := worker.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerReportsClassifiedAttemptTimeout(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		t.Fatal("expired attempt ran its hook")
		return nil
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now.Add(time.Hour)
	clock := &expiredAttemptClock{&manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}}}
	store := &oneActivityStore{record: record, call: call}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.started || !store.reported || store.result.Success || store.result.Failure.Code != "safe" {
		t.Fatalf("timeout was not durably reported as a failure: %+v", store.result)
	}
}

func TestWorkerDoesNotStartAfterOverallDeadline(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		t.Fatal("expired execution ran its hook")
		return nil
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now
	clock := &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}}
	store := &oneActivityStore{record: record, call: call}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrTransitionDeadline) {
		t.Fatalf("overall deadline not reported: %v", err)
	}
	if store.started || store.reported {
		t.Fatal("expired execution consumed an attempt or reported an outcome")
	}
}
