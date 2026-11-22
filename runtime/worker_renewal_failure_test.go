package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

var errRenewalFailed = errors.New("lease renewal failed")

type failingRenewalStore struct{ oneActivityStore }

func (*failingRenewalStore) RenewHook(context.Context, transition.HookLease, func() time.Time, time.Duration) (transition.HookLease, error) {
	return transition.HookLease{}, errRenewalFailed
}

func TestWorkerCancelsWithoutReportingWhenRenewalFails(t *testing.T) {
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
	store := &failingRenewalStore{oneActivityStore{record: record, call: call}}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.RunOne(context.Background()) }()
	<-started
	clock.ticker.ticks <- now
	if err := <-done; !errors.Is(err, errRenewalFailed) {
		t.Fatalf("renewal error was not surfaced: %v", err)
	}
	if store.reported {
		t.Fatal("worker reported an outcome after losing renewal")
	}
}
