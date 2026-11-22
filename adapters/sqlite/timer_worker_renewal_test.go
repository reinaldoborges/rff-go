package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type timerTestClock struct {
	mu    sync.Mutex
	now   time.Time
	ticks chan time.Time
}

func (c *timerTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *timerTestClock) NewTicker(time.Duration) transition.WorkerTicker {
	return timerTestTicker{c.ticks}
}
func (c *timerTestClock) WithTimeout(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}
func (c *timerTestClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type timerTestTicker struct{ ticks chan time.Time }

func (t timerTestTicker) Ticks() <-chan time.Time { return t.ticks }
func (timerTestTicker) Stop()                     {}

type observedTimerStore struct {
	*sqlite.Store[string, string]
	renewed chan struct{}
}

func (s observedTimerStore) RenewTimer(ctx context.Context, lease transition.TimerLease, clock func() time.Time, ttl time.Duration) (transition.TimerLease, error) {
	result, err := s.Store.RenewTimer(ctx, lease, clock, ttl)
	close(s.renewed)
	return result, err
}

func TestTimerWorkerRenewsWhileDelivering(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "worker.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	clock := &timerTestClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), ticks: make(chan time.Time, 1)}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "expiry", WorkflowID: "membership", Deadline: clock.Now(), Event: "expired",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	started, release, renewed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	worker, err := transition.NewTimerWorker[string](observedTimerStore{Store: store, renewed: renewed}, resolve,
		func(ctx context.Context, _ transition.TimerDelivery[string]) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, transition.TimerWorkerConfig{ID: "one", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- worker.RunOne(ctx) }()
	<-started
	clock.advance(30 * time.Second)
	clock.ticks <- clock.Now()
	<-renewed
	clock.advance(31 * time.Second)
	if _, _, err := store.ClaimDueTimer(ctx, "two", clock.Now, time.Minute); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("delivery was reclaimed despite renewal: %v", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("renewed delivery did not complete: %v", err)
	}
}
