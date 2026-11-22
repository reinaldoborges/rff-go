package runtime_test

import (
	"context"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type blockingReportStore struct {
	renewingStore
	reporting chan struct{}
	release   chan struct{}
}

func (s *blockingReportStore) ReportHook(ctx context.Context, lease transition.HookLease, result transition.HookResult, clock func() time.Time) error {
	close(s.reporting)
	select {
	case <-s.release:
		return s.oneActivityStore.ReportHook(ctx, lease, result, clock)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkerRenewsUntilOutcomeIsDurable(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = now.Add(time.Hour)
	clock := &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}}
	store := &blockingReportStore{
		renewingStore: renewingStore{oneActivityStore: oneActivityStore{record: record, call: call}, renewed: make(chan struct{}, 1)},
		reporting:     make(chan struct{}), release: make(chan struct{}),
	}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.RunOne(ctx) }()
	<-store.reporting
	clock.ticker.ticks <- now
	select {
	case <-store.renewed:
	case <-time.After(5 * time.Second):
		t.Fatal("worker stopped renewal before its report was durable")
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
