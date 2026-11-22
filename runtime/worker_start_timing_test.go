package runtime_test

import (
	"context"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type delayedStartStore struct {
	oneActivityStore
	clock *manualWorkerClock
}

func (s *delayedStartStore) StartHook(ctx context.Context, lease transition.HookLease, clock func() time.Time) (transition.StartedHook, error) {
	attempt, err := s.oneActivityStore.StartHook(ctx, lease, clock)
	s.clock.now = s.clock.now.Add(2 * time.Second)
	return attempt, err
}

func TestWorkerMeasuresAttemptTimeoutFromDurableStart(t *testing.T) {
	calls := 0
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		calls++
		return nil
	}})
	now := time.Now()
	record.Policy.AttemptTimeout = time.Second
	record.Deadline = now.Add(time.Hour)
	clock := &manualWorkerClock{now: now, ticker: &manualTicker{ticks: make(chan time.Time, 1)}}
	store := &delayedStartStore{oneActivityStore: oneActivityStore{record: record, call: call}, clock: clock}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || !store.reported || store.result.Success || store.result.Failure.Code != "safe" {
		t.Fatalf("delayed start extended the attempt timeout: calls=%d result=%+v", calls, store.result)
	}
}
