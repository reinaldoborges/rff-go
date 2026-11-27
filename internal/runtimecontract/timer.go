package runtimecontract

import (
	"context"
	"errors"
	"strings"
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

// RunIndexedTimerSelection verifies timer ordering and inclusive eligibility at
// the scheduler, worker, and adapter claim boundary.
func RunIndexedTimerSelection(t *testing.T, newStore func(*testing.T) TimerStore) {
	t.Helper()
	t.Run("worker selection", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		blockedUnavailable := false
		resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
			if version.Version == "2" && blockedUnavailable {
				return nil, transition.ErrCodecNotFound
			}
			return stringCodec{}, nil
		}
		scheduler, err := transition.NewTimerScheduler[string](store, resolve)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Date(2026, 1, 1, 12, 0, 0, 123456789, time.UTC)
		clock := &timerClock{now: now}
		schedule := func(id, version string, deadline time.Time) {
			t.Helper()
			if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
				ID: id, WorkflowID: "workflow", Deadline: deadline, Event: "event",
				EventCodec: transition.CodecVersion{Name: "event", Version: version},
			}); err != nil {
				t.Fatal(err)
			}
		}

		schedule("completed", "2", now)
		_, completedLease, err := store.ClaimDueTimer(ctx, "setup", clock.Now, time.Minute)
		if err != nil || completedLease.ID != "completed" {
			t.Fatalf("claim timer for completion: lease=%+v error=%v", completedLease, err)
		}
		if err := store.CompleteTimer(ctx, completedLease, clock.Now); err != nil {
			t.Fatal(err)
		}

		schedule("blocked", "2", now)
		_, blockedLease, err := store.ClaimDueTimer(ctx, "setup", clock.Now, time.Minute)
		if err != nil || blockedLease.ID != "blocked" {
			t.Fatalf("claim timer for compatibility block: lease=%+v error=%v", blockedLease, err)
		}
		diagnostic := transition.CompatibilityDiagnostic{
			Component: transition.CompatibilityEventCodec, Name: "event", Version: "2",
		}
		if err := store.BlockTimerCompatibility(ctx, blockedLease, clock.Now, diagnostic); err != nil {
			t.Fatal(err)
		}
		blockedUnavailable = true

		schedule("actively-leased", "1", now)
		_, activeLease, err := store.ClaimDueTimer(ctx, "setup", clock.Now, time.Minute)
		if err != nil || activeLease.ID != "actively-leased" {
			t.Fatalf("claim timer to hold lease: lease=%+v error=%v", activeLease, err)
		}

		prefix := strings.Repeat("x", 255)
		firstTieID, secondTieID := prefix+"a", prefix+"z"
		schedule(firstTieID, "1", now)
		schedule(secondTieID, "1", now)
		schedule("later", "1", now.Add(time.Second))
		schedule("future", "1", now.Add(time.Second+time.Nanosecond))
		schedule("maximum-supported-time", "1", time.Date(9999, 12, 30, 23, 59, 59, 999999999, time.UTC))

		var delivered []transition.TimerDelivery[string]
		worker, err := transition.NewTimerWorker[string](store, resolve, func(_ context.Context, item transition.TimerDelivery[string]) error {
			delivered = append(delivered, item)
			return nil
		}, transition.TimerWorkerConfig{
			ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{firstTieID, secondTieID} {
			if err := worker.RunOne(ctx); err != nil {
				t.Fatalf("deliver %q: %v", expected, err)
			}
			if got := delivered[len(delivered)-1]; got.ID != expected || !got.Deadline.Equal(now) {
				t.Fatalf("delivery = %+v, want %q at %s", got, expected, now)
			}
		}
		if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("unavailable timer was not surfaced while no work was eligible: %v", err)
		}
		if _, _, err := store.ClaimDueTimer(ctx, "direct-check", clock.Now, time.Minute); !errors.Is(err, transition.ErrNoDueTimer) {
			t.Fatalf("completed, blocked, leased, or future timer was eligible: %v", err)
		}
		clock.advance(time.Second)
		if err := worker.RunOne(ctx); err != nil || delivered[len(delivered)-1].ID != "later" {
			t.Fatalf("timer due at equality was not delivered: deliveries=%+v error=%v", delivered, err)
		}
		if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("future timer fired early or blocked work was hidden: %v", err)
		}
		if _, _, err := store.ClaimDueTimer(ctx, "direct-check", clock.Now, time.Minute); !errors.Is(err, transition.ErrNoDueTimer) {
			t.Fatalf("future timer was eligible before deadline: %v", err)
		}
		clock.advance(time.Nanosecond)
		if err := worker.RunOne(ctx); err != nil || delivered[len(delivered)-1].ID != "future" {
			t.Fatalf("future timer was not delivered at equality: deliveries=%+v error=%v", delivered, err)
		}
	})

	t.Run("lease expiry equality fences the prior claim", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		scheduler, err := transition.NewTimerScheduler[string](store,
			func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil })
		if err != nil {
			t.Fatal(err)
		}
		now := time.Date(1960, 1, 1, 0, 0, 0, 987654321, time.UTC)
		clock := &timerClock{now: now}
		if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
			ID: "negative-unix-time", WorkflowID: "workflow", Deadline: now, Event: "event",
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		}); err != nil {
			t.Fatal(err)
		}
		_, first, err := store.ClaimDueTimer(ctx, "first", clock.Now, time.Minute)
		if err != nil || first.ID != "negative-unix-time" || first.Generation != 1 {
			t.Fatalf("first claim = %+v, %v", first, err)
		}
		clock.advance(time.Minute)
		_, second, err := store.ClaimDueTimer(ctx, "second", clock.Now, time.Minute)
		if err != nil || second.ID != first.ID || second.Generation != first.Generation+1 {
			t.Fatalf("claim at lease-expiry equality = %+v, %v", second, err)
		}
		if err := store.CompleteTimer(ctx, first, clock.Now); !errors.Is(err, transition.ErrStaleLease) {
			t.Fatalf("expired generation completed timer: %v", err)
		}
		if err := store.CompleteTimer(ctx, second, clock.Now); err != nil {
			t.Fatalf("current generation failed to complete timer: %v", err)
		}
		if _, _, err := store.ClaimDueTimer(ctx, "third", clock.Now, time.Minute); !errors.Is(err, transition.ErrNoDueTimer) {
			t.Fatalf("completed timer remained eligible: %v", err)
		}
	})

	t.Run("competing claims are atomic", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)
		scheduler, err := transition.NewTimerScheduler[string](store,
			func(transition.CodecVersion) (transition.Codec[string], error) { return stringCodec{}, nil })
		if err != nil {
			t.Fatal(err)
		}
		now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
			ID: "single-claim", WorkflowID: "workflow", Deadline: now, Event: "event",
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		}); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		for _, owner := range []string{"worker-a", "worker-b"} {
			go func(owner string) {
				<-start
				_, lease, err := store.ClaimDueTimer(ctx, owner, func() time.Time { return now }, time.Minute)
				if err == nil && (lease.ID != "single-claim" || lease.Owner != owner || lease.Generation != 1) {
					err = errors.New("claim returned an invalid lease")
				}
				results <- err
			}(owner)
		}
		close(start)
		successes, empty := 0, 0
		for i := 0; i < 2; i++ {
			err := <-results
			switch {
			case err == nil:
				successes++
			case errors.Is(err, transition.ErrNoDueTimer):
				empty++
			default:
				t.Fatalf("competing claim failed unexpectedly: %v", err)
			}
		}
		if successes != 1 || empty != 1 {
			t.Fatalf("competing claims yielded %d winners and %d empty results", successes, empty)
		}
	})
}
