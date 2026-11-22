package runtimecontract

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// RunTimerDelivery checks durable timer identity and late delivery recovery.
func RunTimerDelivery(t *testing.T, newStore func(*testing.T) TimerStore) {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	request := transition.TimerRequest[string]{
		ID: "expiry", WorkflowID: "membership", Deadline: deadline, Event: "expired",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}
	first, created, err := scheduler.Schedule(ctx, request)
	if err != nil || !created || !first.Deadline.Equal(deadline) {
		t.Fatalf("timer was not scheduled: %+v created=%v error=%v", first, created, err)
	}
	replayed, created, err := scheduler.Schedule(ctx, request)
	if err != nil || created || replayed != first {
		t.Fatalf("timer did not replay: %+v created=%v error=%v", replayed, created, err)
	}
	changed := request
	changed.Event = "different"
	if _, _, err := scheduler.Schedule(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed timer event did not conflict: %v", err)
	}
	changed = request
	changed.Deadline = deadline.Add(time.Second)
	if _, _, err := scheduler.Schedule(ctx, changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed timer deadline did not conflict: %v", err)
	}
	clock := &timerClock{now: deadline.Add(-time.Nanosecond)}
	lost := errors.New("delivery response lost")
	var deliveries []transition.TimerDelivery[string]
	worker, err := transition.NewTimerWorker[string](store, resolve, func(_ context.Context, delivery transition.TimerDelivery[string]) error {
		deliveries = append(deliveries, delivery)
		if len(deliveries) == 1 {
			return lost
		}
		return nil
	}, transition.TimerWorkerConfig{ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrNoDueTimer) || len(deliveries) != 0 {
		t.Fatalf("timer fired before deadline: deliveries=%+v error=%v", deliveries, err)
	}
	clock.advance(time.Nanosecond)
	if err := worker.RunOne(ctx); !errors.Is(err, lost) || len(deliveries) != 1 {
		t.Fatalf("unknown timer outcome was not reported: deliveries=%+v error=%v", deliveries, err)
	}
	clock.advance(10 * time.Minute)
	if err := worker.RunOne(ctx); err != nil || len(deliveries) != 2 ||
		deliveries[0].ID != "expiry" || deliveries[1].ID != deliveries[0].ID ||
		deliveries[1].Event != "expired" || !deliveries[1].Deadline.Equal(deadline) {
		t.Fatalf("late timer recovery changed identity or deadline: deliveries=%+v error=%v", deliveries, err)
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("completed timer was delivered again: %v", err)
	}
	if _, created, err := scheduler.Schedule(ctx, request); err != nil || created {
		t.Fatalf("completed timer lost its scheduling identity: created=%v error=%v", created, err)
	}
}
