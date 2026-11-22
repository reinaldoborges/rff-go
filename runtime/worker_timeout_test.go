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
