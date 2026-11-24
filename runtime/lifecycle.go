package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// LifecycleStore combines the existing durable coordinator, commit and worker ports.
type LifecycleStore[EntityID comparable, StateID comparable] interface {
	CoordinatorStore[EntityID, StateID]
	CommitStore[EntityID, StateID]
	HookActivityStore[EntityID, StateID]
}

// LifecycleConfig explicitly enables transition execution with local capacity.
type LifecycleConfig struct {
	Coordinator  CoordinatorConfig
	Worker       WorkerConfig
	PollInterval time.Duration
}

// Lifecycle runs a single local transition coordinator and bounded hook workers.
// The host must ensure only one logical lifecycle coordinates a store.
type Lifecycle[EntityID comparable, StateID comparable, Event, Input any] struct {
	coordinator *Coordinator[EntityID, StateID]
	commits     *CommitCoordinator[EntityID, StateID]
	worker      *HookWorker[EntityID, StateID, Event, Input]
	interval    time.Duration
	wakes       []chan struct{}
	errors      chan struct{}

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
}

// NewLifecycle validates all dependencies and timing before any work starts.
func NewLifecycle[EntityID comparable, StateID comparable, Event, Input any](
	store LifecycleStore[EntityID, StateID], classifier HookFailureClassifier[EntityID, StateID],
	runner *HookRunner[EntityID, StateID, Event, Input], host CommitHost[EntityID, StateID], config LifecycleConfig,
) (*Lifecycle[EntityID, StateID, Event, Input], error) {
	if config.PollInterval <= 0 || config.PollInterval >= config.Coordinator.LeaseTTL {
		return nil, errors.New("poll interval must be positive and shorter than the coordinator lease")
	}
	coordinator, err := NewCoordinator(store, classifier, config.Coordinator)
	if err != nil {
		return nil, err
	}
	commits, err := NewCommitCoordinator(store, host, config.Coordinator)
	if err != nil {
		return nil, err
	}
	worker, err := NewHookWorker(store, runner, config.Worker)
	if err != nil {
		return nil, err
	}
	l := &Lifecycle[EntityID, StateID, Event, Input]{
		coordinator: coordinator, commits: commits, worker: worker, interval: config.PollInterval,
		errors: make(chan struct{}, 1), wakes: make([]chan struct{}, config.Worker.Capacity+1),
	}
	for i := range l.wakes {
		l.wakes[i] = make(chan struct{}, 1)
	}
	return l, nil
}

// Start checks durable work synchronously before launching background progress.
// The context bounds startup only; Shutdown controls the running lifecycle.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return errors.New("lifecycle already started")
	}
	l.started = true
	if err := l.coordinator.Reconcile(ctx); err != nil {
		return fmt.Errorf("initial transition scan: %w", err)
	}
	if err := l.commits.Reconcile(ctx); err != nil {
		return fmt.Errorf("initial commit scan: %w", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(l.wakes))
	go func() {
		defer wg.Done()
		l.coordinate(runCtx)
	}()
	for i := 1; i < len(l.wakes); i++ {
		go func(wake <-chan struct{}) {
			defer wg.Done()
			l.work(runCtx, wake)
		}(l.wakes[i])
	}
	go func() { wg.Wait(); close(l.done) }()
	return nil
}

// Wakeup is a best-effort hint; periodic durable scans remain authoritative.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Wakeup() {
	for _, wake := range l.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// Errors signals that Err has changed. Errors remain available even if a signal
// is coalesced or the host is not listening.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Errors() <-chan struct{} { return l.errors }

// Err returns all background progress errors observed by this lifecycle.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) fail(path string, err error) {
	l.mu.Lock()
	l.err = errors.Join(l.err, fmt.Errorf("%s: %w", path, err))
	l.mu.Unlock()
	select {
	case l.errors <- struct{}{}:
	default:
	}
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) coordinate(ctx context.Context) {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-l.wakes[0]:
		}
		if ctx.Err() != nil {
			return
		}
		if err := l.coordinator.Reconcile(ctx); err != nil {
			if ctx.Err() == nil {
				l.fail("transition scan", err)
			}
			return
		}
		if err := l.commits.Reconcile(ctx); err != nil {
			if ctx.Err() == nil {
				l.fail("commit scan", err)
			}
			return
		}
		for _, wake := range l.wakes[1:] {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) work(ctx context.Context, wake <-chan struct{}) {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		err := l.worker.RunOne(context.Background())
		switch {
		case err == nil:
			select {
			case l.wakes[0] <- struct{}{}:
			default:
			}
			continue
		case errors.Is(err, ErrWorkerDraining):
			return
		case errors.Is(err, ErrNoHookActivity), errors.Is(err, ErrWorkerAtCapacity):
		default:
			if ctx.Err() == nil {
				l.fail("hook worker", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
	}
}

// Shutdown stops scans and claims, then drains hooks with lease renewal until
// ctx expires. A deadline error means work may still be running externally.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel == nil {
		return errors.New("lifecycle is not running")
	}
	cancel()
	err := l.worker.Shutdown(ctx)
	select {
	case <-done:
		return err
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}
