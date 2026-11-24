package runtime_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLifecycleDeliversTimerDueBeforeStartup(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "timers.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(-time.Minute).UTC()
	_, _, err = scheduler.Schedule(context.Background(), transition.TimerRequest[string]{
		ID: "expiry", WorkflowID: "membership", Deadline: deadline, Event: "expired",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	deliveries := make(chan transition.TimerDelivery[string], 1)
	err = l.EnableTimerDelivery(store, resolve, func(_ context.Context, d transition.TimerDelivery[string]) error {
		deliveries <- d
		return nil
	}, transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
		ID: "timer", LeaseTTL: time.Second, RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	select {
	case d := <-deliveries:
		if d.ID != "expiry" || d.WorkflowID != "membership" || d.Event != "expired" || !d.Deadline.Equal(deadline) {
			t.Fatalf("unexpected delivery: %+v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not deliver due timer")
	}
}

type lifecycleTimerCodec struct{}

type failingLifecycleTimerStore struct {
	transition.TimerDeliveryStore
	fail atomic.Bool
}

type lifecycleStoreWithTimers struct {
	*lifecycleTestStore
	transition.TimerDeliveryStore
}

var errTimerScan = errors.New("private timer scan failure")

func (s *failingLifecycleTimerStore) ClaimDueTimer(ctx context.Context, id string, now func() time.Time, ttl time.Duration) (transition.TimerRecord, transition.TimerLease, error) {
	if s.fail.Load() {
		return transition.TimerRecord{}, transition.TimerLease{}, errTimerScan
	}
	return s.TimerDeliveryStore.ClaimDueTimer(ctx, id, now, ttl)
}

func (lifecycleTimerCodec) Encode(_ context.Context, value string) ([]byte, error) {
	return []byte(value), nil
}
func (lifecycleTimerCodec) Decode(_ context.Context, value []byte) (string, error) {
	return string(value), nil
}
func (lifecycleTimerCodec) Equal(_ context.Context, data []byte, value string) (bool, error) {
	return string(data) == value, nil
}

func TestLifecycleConcurrentTimerRegistrationAndShutdown(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "registration.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	for i := 0; i < 20; i++ {
		l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
			Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker:       transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		registered := make(chan error, 1)
		go func() {
			registered <- l.EnableTimerDelivery(store,
				func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil },
				func(context.Context, transition.TimerDelivery[string]) error { return nil },
				transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
					ID: "timer", LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{},
				}})
		}()
		if err := l.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = l.Shutdown(ctx)
		<-registered // Registration may win or lose the race with Start.
		if err := l.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLifecycleTimerConfigurationIsExplicitAndValidated(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	timerStore, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "config.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer timerStore.Close()
	store := &lifecycleStoreWithTimers{lifecycleTestStore: &lifecycleTestStore{}, TimerDeliveryStore: timerStore}
	l, err := transition.NewLifecycle[string, string, string, int](store, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil }
	handler := func(context.Context, transition.TimerDelivery[string]) error { return nil }
	valid := transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
		ID: "timer", LeaseTTL: time.Second, RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{},
	}}
	for _, invalid := range []transition.TimerLifecycleConfig{
		{}, {Capacity: 0, Worker: valid.Worker},
		{Capacity: 1, Worker: transition.TimerWorkerConfig{ID: "timer", LeaseTTL: time.Second}},
		{Capacity: 1, Worker: transition.TimerWorkerConfig{ID: "timer", LeaseTTL: 10 * time.Millisecond, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}}},
	} {
		if err := l.EnableTimerDelivery(timerStore, resolve, handler, invalid); err == nil {
			t.Fatalf("accepted invalid timer configuration: %+v", invalid)
		}
	}
	if err := l.EnableTimerDelivery(nil, resolve, handler, valid); err == nil {
		t.Fatal("accepted missing timer store")
	}
	if err := l.EnableTimerDelivery(timerStore, nil, handler, valid); err == nil {
		t.Fatal("accepted missing timer codec resolver")
	}
	if err := l.EnableTimerDelivery(timerStore, resolve, nil, valid); err == nil {
		t.Fatal("accepted missing timer handler")
	}
	for _, path := range l.Status().Paths {
		if path.Name == "timer-worker-0" {
			t.Fatal("invalid configuration activated a timer worker")
		}
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	if err := l.EnableTimerDelivery(timerStore, resolve, handler, valid); err == nil {
		t.Fatal("enabled timer delivery after start")
	}
}

func TestLifecycleTimerWorkersDeliverIndependentlyAndDrain(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "parallel.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		_, _, err = scheduler.Schedule(context.Background(), transition.TimerRequest[string]{
			ID: id, WorkflowID: "membership", Deadline: time.Now().Add(-time.Second), Event: "expired",
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan string, 2)
	release := make(chan struct{})
	if err := l.EnableTimerDelivery(store, resolve, func(_ context.Context, d transition.TimerDelivery[string]) error {
		delivered <- d.ID
		if d.ID == "a" {
			<-release
		}
		return nil
	}, transition.TimerLifecycleConfig{Capacity: 2, Worker: transition.TimerWorkerConfig{
		ID: "timer", LeaseTTL: 150 * time.Millisecond, RenewalInterval: 20 * time.Millisecond, Clock: transition.SystemWorkerClock{},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-delivered:
		case <-time.After(5 * time.Second):
			t.Fatal("blocked timer prevented an independent delivery")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("active handler prevented readiness: %+v", l.Status())
	}
	done := make(chan error, 1)
	go func() { done <- l.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("shutdown did not drain the active handler: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after handler returned")
	}
	worker, err := transition.NewTimerWorker(store, resolve, func(context.Context, transition.TimerDelivery[string]) error {
		t.Error("drained timer was delivered again")
		return nil
	}, transition.TimerWorkerConfig{ID: "other", LeaseTTL: time.Second, RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("drained deliveries were not completed: %v", err)
	}
}

func TestLifecycleTimerFailureIsIsolatedAndRestartable(t *testing.T) {
	timerStore, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "failure.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer timerStore.Close()
	failing := &failingLifecycleTimerStore{TimerDeliveryStore: timerStore}
	failing.fail.Store(true)
	scans := &lifecycleTestStore{}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](scans, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
			RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil }
	if err := l.EnableTimerDelivery(failing, resolve, func(context.Context, transition.TimerDelivery[string]) error { return nil },
		transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
			ID: "timer", LeaseTTL: time.Second, RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{},
		}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(context.Background())
	select {
	case <-l.Errors():
		if !errors.Is(l.Err(), errTimerScan) {
			t.Fatalf("timer error not surfaced: %v", l.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timer failure not signaled")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stopped := false
		for _, path := range l.Status().Paths {
			if path.Name == "timer-worker-0" && !path.Running && path.Classification == transition.PathErrorUnclassified {
				stopped = true
			}
		}
		if stopped {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if l.Status().Ready {
		t.Fatal("failed timer path did not affect readiness")
	}
	before := scans.scans.Load()
	time.Sleep(40 * time.Millisecond)
	if scans.scans.Load() <= before {
		t.Fatal("timer failure stopped transition path")
	}
	failing.fail.Store(false)
	if err := l.RestartPath("timer-worker-0"); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("restarted timer path did not recover: %+v", l.Status())
	}
}

func TestLifecycleTimerRecoveryAndMissedWakeup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(80 * time.Millisecond).UTC()
	_, _, err = scheduler.Schedule(context.Background(), transition.TimerRequest[string]{
		ID: "expiry", WorkflowID: "membership", Deadline: deadline, Event: "expired",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	lost := errors.New("host response lost")
	var mu sync.Mutex
	effects := map[string]int{}
	deliveries := make(chan transition.TimerDelivery[string], 2)
	handler := func(_ context.Context, d transition.TimerDelivery[string]) error {
		mu.Lock()
		first := effects[d.ID] == 0
		if first {
			effects[d.ID]++
		}
		mu.Unlock()
		deliveries <- d
		if first {
			return lost
		}
		return nil
	}
	newLifecycle := func() *transition.Lifecycle[string, string, string, int] {
		t.Helper()
		runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
		l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
				RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
			ClassifyError: func(err error) transition.PathErrorClass {
				if errors.Is(err, lost) {
					return transition.PathErrorRecoverable
				}
				return transition.PathErrorUnclassified
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = l.EnableTimerDelivery(store, resolve, handler, transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
			ID: "timer", LeaseTTL: 150 * time.Millisecond, RenewalInterval: 30 * time.Millisecond, Clock: transition.SystemWorkerClock{},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	first := newLifecycle()
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-deliveries:
		if time.Now().Before(deadline) || !d.Deadline.Equal(deadline) {
			t.Fatalf("timer delivered before its persisted deadline: %+v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("polling did not recover missed wakeup")
	}
	select {
	case <-first.Errors():
		if !errors.Is(first.Err(), lost) {
			t.Fatalf("delivery failure was not surfaced: %v", first.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delivery failure was not signaled")
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	second := newLifecycle()
	if err := second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer second.Shutdown(context.Background())
	select {
	case d := <-deliveries:
		if d.ID != "expiry" || d.Event != "expired" || !d.Deadline.Equal(deadline) {
			t.Fatalf("replayed delivery changed: %+v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expired delivery was not recovered after restart")
	}
	mu.Lock()
	defer mu.Unlock()
	if effects["expiry"] != 1 {
		t.Fatalf("host did not deduplicate effect: %+v", effects)
	}
}
func TestLifecycleTimerShutdownDeadlineLeavesClaimsRecoverable(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "shutdown.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleTimerCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		_, _, err = scheduler.Schedule(context.Background(), transition.TimerRequest[string]{
			ID: id, WorkflowID: "membership", Deadline: time.Now().Add(-time.Second), Event: "expired",
			EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	scans := &lifecycleTestStore{}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	handler := func(ctx context.Context, _ transition.TimerDelivery[string]) error {
		entered <- ctx
		<-release
		return nil
	}
	newLifecycle := func(handler func(context.Context, transition.TimerDelivery[string]) error) *transition.Lifecycle[string, string, string, int] {
		t.Helper()
		runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
		l, err := transition.NewLifecycle[string, string, string, int](scans, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
			Coordinator: transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
			Worker: transition.WorkerConfig{ID: "hook", Capacity: 1, LeaseTTL: time.Second,
				RenewalInterval: 100 * time.Millisecond, Clock: transition.SystemWorkerClock{}},
			PollInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := l.EnableTimerDelivery(store, resolve, handler, transition.TimerLifecycleConfig{Capacity: 1, Worker: transition.TimerWorkerConfig{
			ID: "timer", LeaseTTL: 150 * time.Millisecond, RenewalInterval: 20 * time.Millisecond, Clock: transition.SystemWorkerClock{},
		}}); err != nil {
			t.Fatal(err)
		}
		return l
	}
	first := newLifecycle(handler)
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var handlerCtx context.Context
	select {
	case handlerCtx = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timer handler did not start")
	}
	before := scans.scans.Load()
	time.Sleep(40 * time.Millisecond)
	if scans.scans.Load() <= before {
		t.Fatal("blocked timer handler stopped independent transition scans")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := first.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown did not respect deadline: %v", err)
	}
	select {
	case <-handlerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel unfinished timer delivery")
	}
	close(release)
	delivered := make(chan string, 2)
	second := newLifecycle(func(_ context.Context, d transition.TimerDelivery[string]) error {
		delivered <- d.ID
		return nil
	})
	if err := second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer second.Shutdown(context.Background())
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-delivered:
			seen[id] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("unfinished deliveries were not recovered: %v", seen)
		}
	}
}
