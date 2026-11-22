package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerSchedulePersistsAndDeduplicatesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "timers.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version != (transition.CodecVersion{Name: "event", Version: "1"}) {
			return nil, errors.New("unknown codec")
		}
		return stringValueCodec{}, nil
	}
	newScheduler := func() *transition.TimerScheduler[string] {
		t.Helper()
		scheduler, err := transition.NewTimerScheduler[string](store, resolve)
		if err != nil {
			t.Fatal(err)
		}
		return scheduler
	}
	request := transition.TimerRequest[string]{
		ID: "expiry-1", WorkflowID: "membership-1", Deadline: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Event: "expired", EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}
	first, created, err := newScheduler().Schedule(ctx, request)
	if err != nil || !created || first.Deadline != request.Deadline {
		t.Fatalf("schedule timer: timer=%+v created=%v error=%v", first, created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	replayed, created, err := newScheduler().Schedule(ctx, request)
	if err != nil || created || replayed != first {
		t.Fatalf("timer replay did not preserve original outcome: timer=%+v created=%v error=%v", replayed, created, err)
	}
	request.Event = "different"
	if _, _, err := newScheduler().Schedule(ctx, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("different timer payload did not conflict: %v", err)
	}
	request.Event = "expired"
	request.Deadline = request.Deadline.Add(time.Second)
	if _, _, err := newScheduler().Schedule(ctx, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("different timer deadline did not conflict: %v", err)
	}
}

func TestTimerDeliveryUsesPersistedDeadlineAndRecoversUnknownOutcome(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "delivery.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	request := transition.TimerRequest[string]{
		ID: "expiry", WorkflowID: "membership", Deadline: deadline, Event: "expired",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}
	if _, _, err := scheduler.Schedule(ctx, request); err != nil {
		t.Fatal(err)
	}
	clock := &timerTestClock{now: deadline.Add(-time.Nanosecond), ticks: make(chan time.Time, 1)}
	calls := 0
	delivered := make(map[string]transition.TimerDelivery[string])
	unknown := errors.New("response lost")
	handler := func(_ context.Context, delivery transition.TimerDelivery[string]) error {
		calls++
		delivered[delivery.ID] = delivery
		if calls == 1 {
			return unknown
		}
		return nil
	}
	newWorker := func() *transition.TimerWorker[string] {
		t.Helper()
		worker, err := transition.NewTimerWorker[string](store, resolve, handler, transition.TimerWorkerConfig{
			ID: "timer-worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}
	worker := newWorker()
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrNoDueTimer) || calls != 0 {
		t.Fatalf("timer fired before its deadline: calls=%d error=%v", calls, err)
	}
	clock.advance(time.Nanosecond)
	if err := worker.RunOne(ctx); !errors.Is(err, unknown) || calls != 1 {
		t.Fatalf("timer did not fire at deadline: calls=%d error=%v", calls, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	clock.advance(10 * time.Minute)
	if err := newWorker().RunOne(ctx); err != nil {
		t.Fatalf("recover timer delivery: %v", err)
	}
	if calls != 2 || len(delivered) != 1 || delivered["expiry"].Event != "expired" || !delivered["expiry"].Deadline.Equal(deadline) {
		t.Fatalf("delivery changed its identity or deadline: calls=%d deliveries=%+v", calls, delivered)
	}
	if err := newWorker().RunOne(ctx); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("completed timer was delivered again: %v", err)
	}
	scheduler, err = transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := scheduler.Schedule(ctx, request); err != nil || created {
		t.Fatalf("completed timer scheduling did not replay: created=%v error=%v", created, err)
	}
}
