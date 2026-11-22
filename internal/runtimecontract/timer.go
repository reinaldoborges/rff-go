package runtimecontract

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// TimerStore combines the runtime ports needed by the timer compatibility contract.
type TimerStore interface {
	transition.TimerScheduleStore
	transition.TimerDeliveryStore
}

type timerClock struct {
	transition.SystemWorkerClock
	mu  sync.RWMutex
	now time.Time
}

func (c *timerClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *timerClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// RunTimerCompatibility checks isolation and restoration through timer ports.
func RunTimerCompatibility(t *testing.T, newStore func(*testing.T) TimerStore) {
	t.Helper()
	t.Run("timer delivery", func(t *testing.T) {
		RunTimerDelivery(t, newStore)
	})
	store := newStore(t)
	ctx := context.Background()
	available := true
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version.Name != "event" || (version.Version != "1" && version.Version != "2") ||
			version.Version == "1" && !available {
			return nil, transition.ErrCodecNotFound
		}
		return stringCodec{}, nil
	}
	clock := &timerClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ id, version string }{{"a", "1"}, {"b", "2"}} {
		if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
			ID: item.id, WorkflowID: item.id, Deadline: clock.Now(), Event: "event",
			EventCodec: transition.CodecVersion{Name: "event", Version: item.version},
		}); err != nil {
			t.Fatal(err)
		}
	}
	available = false
	var delivered []transition.TimerDelivery[string]
	worker, err := transition.NewTimerWorker[string](store, resolve, func(_ context.Context, event transition.TimerDelivery[string]) error {
		delivered = append(delivered, event)
		return nil
	}, transition.TimerWorkerConfig{ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable codec was not reported: %v", err)
	}
	blocked, err := store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 1 || blocked[0].ID != "a" || blocked[0].Compatibility == nil ||
		blocked[0].Compatibility.Component != transition.CompatibilityEventCodec {
		t.Fatalf("timer block was not inspectable: %+v, %v", blocked, err)
	}
	clock.advance(2 * time.Minute)
	if err := worker.RunOne(ctx); err != nil || len(delivered) != 1 || delivered[0].ID != "b" {
		t.Fatalf("blocked timer starved compatible work: %+v, %v", delivered, err)
	}
	available = true
	if err := worker.RunOne(ctx); err != nil || len(delivered) != 2 || delivered[1].ID != "a" ||
		!delivered[1].Deadline.Equal(delivered[0].Deadline) {
		t.Fatalf("restored timer lost its original identity or deadline: %+v, %v", delivered, err)
	}
}
