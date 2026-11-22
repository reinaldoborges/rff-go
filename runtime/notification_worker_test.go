package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type deliveryStore struct {
	delivery transition.NotificationDelivery[string, string]
	retried  bool
	done     bool
}

func (s *deliveryStore) ClaimNotification(_ context.Context, owner string, clock func() time.Time, ttl time.Duration) (transition.NotificationDelivery[string, string], error) {
	s.delivery.Attempt++
	s.delivery.Lease.Owner = owner
	s.delivery.Lease.Generation = s.delivery.Attempt
	s.delivery.Lease.ExpiresAt = clock().Add(ttl)
	return s.delivery, nil
}

func (s *deliveryStore) RenewNotification(_ context.Context, lease transition.NotificationLease, clock func() time.Time, ttl time.Duration) (transition.NotificationLease, error) {
	lease.ExpiresAt = clock().Add(ttl)
	return lease, nil
}

func (s *deliveryStore) RetryNotificationDelivery(_ context.Context, _ transition.NotificationLease, clock func() time.Time, due time.Time) error {
	if due.Before(clock()) {
		return errors.New("retry is in the past")
	}
	s.retried = true
	return nil
}

func (s *deliveryStore) CompleteNotification(context.Context, transition.NotificationLease, func() time.Time) error {
	s.done = true
	return nil
}

func TestNotificationWorkerRetriesWithStableIntent(t *testing.T) {
	unavailable := errors.New("delivery unavailable")
	store := &deliveryStore{delivery: transition.NotificationDelivery[string, string]{
		Intent: transition.NotificationIntent[string, string]{ID: "intent", ExecutionID: "execution"},
		Lease:  transition.NotificationLease{IntentID: "intent", ExecutionID: "execution"},
	}}
	var seen []transition.NotificationDelivery[string, string]
	worker, err := transition.NewNotificationWorker[string, string](
		store, func(_ context.Context, delivery transition.NotificationDelivery[string, string]) error {
			seen = append(seen, delivery)
			if len(seen) == 1 {
				return unavailable
			}
			return nil
		}, transition.NotificationWorkerConfig{
			ID: "worker", LeaseTTL: time.Minute, RenewalInterval: time.Second,
			AttemptTimeout: time.Minute, RetryDelay: time.Second, Clock: transition.SystemWorkerClock{},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, unavailable) || !store.retried || store.done {
		t.Fatalf("delivery failure was not scheduled for retry: %v", err)
	}
	if err := worker.RunOne(context.Background()); err != nil || !store.done {
		t.Fatalf("delivery retry did not complete: %v", err)
	}
	if len(seen) != 2 || seen[0].Intent.ID != seen[1].Intent.ID || seen[0].Attempt != 1 || seen[1].Attempt != 2 {
		t.Fatalf("delivery identity or attempt changed incorrectly: %+v", seen)
	}
}
