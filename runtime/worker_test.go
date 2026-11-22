package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type oneActivityStore struct {
	record   transition.Execution[string, string]
	call     transition.HookCall
	reported bool
	started  bool
	result   transition.HookResult
}

func (s *oneActivityStore) ClaimHook(_ context.Context, worker string, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	return transition.HookLease{
		ExecutionID: s.record.ID, ActivityID: s.call.Attempt.ActivityID,
		Owner: worker, Generation: s.call.Attempt.LeaseGeneration, ExpiresAt: clock().Add(ttl),
	}, nil
}

func (s *oneActivityStore) InspectHook(_ context.Context, _ transition.HookLease, _ func() time.Time) (transition.Execution[string, string], transition.HookCall, error) {
	return s.record, s.call, nil
}

func (s *oneActivityStore) StartHook(_ context.Context, _ transition.HookLease, clock func() time.Time) (transition.StartedHook, error) {
	s.started = true
	return transition.StartedHook{Attempt: s.call.Attempt, StartedAt: clock()}, nil
}

func (s *oneActivityStore) RenewHook(_ context.Context, lease transition.HookLease, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	lease.ExpiresAt = clock().Add(ttl)
	return lease, nil
}

func (s *oneActivityStore) ReportHook(_ context.Context, _ transition.HookLease, result transition.HookResult, _ func() time.Time) error {
	s.reported = true
	s.result = result
	return nil
}

func TestWorkerRunsClaimedHookAndReportsSuccess(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = time.Now().Add(time.Hour)
	store := &oneActivityStore{record: record, call: call}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second,
		Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.reported || !store.result.Success {
		t.Fatalf("worker did not report success: %+v", store.result)
	}
	status := worker.Status()
	if !status.Ready || status.Draining || status.Capacity != 1 || status.Active != 0 || status.Heartbeat.IsZero() {
		t.Fatalf("unexpected worker status: %+v", status)
	}
	if err := worker.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrWorkerDraining) {
		t.Fatalf("draining worker accepted work: %v", err)
	}
}
