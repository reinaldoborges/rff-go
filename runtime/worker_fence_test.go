package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type wrongOwnerStore struct{ oneActivityStore }

func (s *wrongOwnerStore) ClaimHook(ctx context.Context, worker string, clock func() time.Time, ttl time.Duration) (transition.HookLease, error) {
	lease, err := s.oneActivityStore.ClaimHook(ctx, worker, clock, ttl)
	lease.Owner = "another-worker"
	return lease, err
}

func TestWorkerRejectsAnotherWorkersClaim(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		t.Fatal("wrong worker ran the hook")
		return nil
	}})
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = time.Now().Add(time.Hour)
	store := &wrongOwnerStore{oneActivityStore{record: record, call: call}}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrStaleActivityLease) {
		t.Fatalf("wrong worker lease not rejected: %v", err)
	}
	if store.started || store.reported {
		t.Fatal("invalid claim started work or reported an outcome")
	}
}
