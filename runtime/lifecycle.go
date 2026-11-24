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

// PathErrorClass is a safe description of a progress-path failure.
type PathErrorClass string

const (
	PathErrorUnclassified PathErrorClass = "unclassified"
	PathErrorRecoverable  PathErrorClass = "recoverable"
	PathErrorFatal        PathErrorClass = "fatal"
)

// LifecycleConfig explicitly enables transition execution with local capacity.
type LifecycleConfig struct {
	Coordinator  CoordinatorConfig
	Worker       WorkerConfig
	PollInterval time.Duration
	// Inputs explicitly enables independently supervised typed input channels.
	Inputs []LifecycleInput
	// ClassifyError classifies errors not recognized by the framework. It may
	// be called concurrently and must not expose error text in diagnostics.
	ClassifyError func(error) PathErrorClass
}

// PathStatus contains only safe, non-persisted diagnostics.
type PathStatus struct {
	Name           string
	Checked        bool
	Running        bool
	Classification PathErrorClass
}

// LifecycleStatus separates durable progress readiness from local capacity.
type LifecycleStatus struct {
	Ready    bool
	Capacity ExecutionCapacity
	Paths    []PathStatus
}

type lifecyclePath struct {
	name       string
	wake       chan struct{}
	done       chan struct{}
	status     PathStatus
	step       func(context.Context, func()) error
	worker     bool
	hook       bool
	empty      error
	stop       func()
	cancelWork func()
	err        error
}

// Lifecycle runs a single local transition coordinator and bounded hook workers.
// The host must ensure only one logical lifecycle coordinates a store.
type Lifecycle[EntityID comparable, StateID comparable, Event, Input any] struct {
	worker      *HookWorker[EntityID, StateID, Event, Input]
	commitHost  CommitHost[EntityID, StateID]
	coordinator CoordinatorConfig
	interval    time.Duration
	classify    func(error) PathErrorClass
	errors      chan struct{}

	mu            sync.Mutex
	started       bool
	stopping      bool
	cancel        context.CancelFunc
	ctx           context.Context
	startup       chan struct{}
	paths         []*lifecyclePath
	notifications *lifecycleNotifications[EntityID, StateID]
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
		worker: worker, commitHost: host, coordinator: config.Coordinator, interval: config.PollInterval,
		classify: config.ClassifyError, errors: make(chan struct{}, 1),
	}
	add := func(name string, worker bool, step func(context.Context, func()) error) {
		l.paths = append(l.paths, &lifecyclePath{name: name, wake: make(chan struct{}, 1),
			status: PathStatus{Name: name}, step: step, worker: worker})
	}
	add("transition", false, func(ctx context.Context, _ func()) error {
		if err := coordinator.Reconcile(ctx); err != nil {
			return err
		}
		return commits.Reconcile(ctx)
	})
	for i := 0; i < config.Worker.Capacity; i++ {
		add(fmt.Sprintf("hook-worker-%d", i), true, func(_ context.Context, checked func()) error {
			return worker.runOne(context.Background(), checked)
		})
		l.paths[len(l.paths)-1].hook = true
		l.paths[len(l.paths)-1].empty = ErrNoHookActivity
	}
	if err := l.addInputs(config.Inputs, add); err != nil {
		return nil, err
	}
	return l, nil
}

// Start checks coordinator paths synchronously. A failed initial check is
// returned, while independent paths and recoverable retries continue running.
// The context bounds startup only; Shutdown controls the running lifecycle.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return errors.New("lifecycle already started")
	}
	l.started = true
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.startup = make(chan struct{})
	startup := l.startup
	runCtx := l.ctx
	l.mu.Unlock()
	defer close(startup)

	checkCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(runCtx, cancel)
	defer func() { stop(); cancel() }()
	stopStartup := context.AfterFunc(ctx, func() {
		l.mu.Lock()
		l.stopping = true
		l.cancel()
		l.mu.Unlock()
		_ = l.worker.Shutdown(ctx)
	})
	defer stopStartup()
	results := make(chan error, len(l.paths))
	var checks sync.WaitGroup
	for _, path := range l.paths {
		if path.worker {
			l.mu.Lock()
			if !l.stopping && checkCtx.Err() == nil {
				l.launchLocked(path, false)
			}
			l.mu.Unlock()
			continue
		}
		checks.Add(1)
		go func(path *lifecyclePath) {
			defer checks.Done()
			if err := checkCtx.Err(); err != nil {
				results <- err
				return
			}
			err := path.step(checkCtx, nil)
			if err != nil {
				results <- fmt.Errorf("initial %s scan: %w", path.name, err)
				if checkCtx.Err() == nil {
					l.recordFailure(path, err)
				}
			} else {
				l.recordSuccess(path)
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			if !l.stopping && checkCtx.Err() == nil && path.status.Classification != PathErrorFatal && path.status.Classification != PathErrorUnclassified {
				l.launchLocked(path, true)
			}
		}(path)
	}
	checks.Wait()
	close(results)
	var initial error
	for err := range results {
		initial = errors.Join(initial, err)
	}
	l.mu.Lock()
	if l.stopping {
		l.mu.Unlock()
		return errors.Join(initial, ctx.Err(), context.Canceled)
	}
	if ctx.Err() != nil {
		l.stopping = true
		l.cancel()
		l.mu.Unlock()
		_ = l.worker.Shutdown(ctx)
		return errors.Join(initial, ctx.Err())
	}
	l.mu.Unlock()
	return initial
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) launchLocked(path *lifecyclePath, wait bool) {
	ctx, cancel := context.WithCancel(l.ctx)
	path.done = make(chan struct{})
	path.status.Running = true
	go func() {
		defer cancel()
		l.runPath(ctx, path, wait)
	}()
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) runPath(ctx context.Context, path *lifecyclePath, wait bool) {
	defer func() {
		l.mu.Lock()
		path.status.Running = false
		close(path.done)
		l.mu.Unlock()
	}()
	for {
		if wait {
			timer := time.NewTimer(l.interval)
			if l.pathRecovering(path) {
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			} else {
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-path.wake:
					timer.Stop()
				case <-timer.C:
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		err := path.step(ctx, func() { l.recordSuccess(path) })
		if ctx.Err() != nil {
			return
		}
		if path.worker && path.empty != nil && onlyPathSignal(err, path.empty) {
			l.recordSuccess(path)
			wait = true
			continue
		}
		if path.worker && onlyPathSignal(err, ErrWorkerAtCapacity) {
			wait = true
			continue
		}
		if err != nil {
			if l.recordFailure(path, err) != PathErrorRecoverable {
				return
			}
			wait = true
			continue
		}
		l.recordSuccess(path)
		if path.worker {
			l.Wakeup()
			wait = false
		} else {
			l.wakeWorkers()
			wait = true
		}
	}
}

func onlyPathSignal(err, signal error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*notificationDeliveryFailure); ok {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyPathSignal(child, signal) {
				return false
			}
		}
		return true
	}
	if child := errors.Unwrap(err); child != nil {
		return onlyPathSignal(child, signal)
	}
	return errors.Is(err, signal)
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) pathRecovering(path *lifecyclePath) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return path.status.Classification == PathErrorRecoverable
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) recordSuccess(path *lifecyclePath) {
	l.mu.Lock()
	path.status.Checked = true
	path.status.Classification = ""
	l.mu.Unlock()
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) recordFailure(path *lifecyclePath, err error) PathErrorClass {
	class := classifyPathError(err, l.classify)
	l.mu.Lock()
	path.status.Classification = class
	path.err = fmt.Errorf("%s: %w", path.name, err)
	l.mu.Unlock()
	select {
	case l.errors <- struct{}{}:
	default:
	}
	return class
}

func classifyPathError(err error, host func(error) PathErrorClass) PathErrorClass {
	if _, ok := err.(*notificationDeliveryFailure); ok {
		return PathErrorRecoverable
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		result := PathErrorRecoverable
		for _, child := range joined.Unwrap() {
			class := classifyPathError(child, host)
			if class == PathErrorFatal {
				return class
			}
			if class != PathErrorRecoverable {
				result = PathErrorUnclassified
			}
		}
		return result
	}
	if child := errors.Unwrap(err); child != nil && containsJoinedError(child) {
		return classifyPathError(child, host)
	}
	if errors.Is(err, ErrLeaseHeld) || errors.Is(err, ErrStaleLease) ||
		errors.Is(err, ErrStaleActivityLease) || errors.Is(err, ErrActivityLeaseHeld) ||
		errors.Is(err, ErrStaleNotificationLease) ||
		errors.Is(err, ErrExecutionNotActive) || errors.Is(err, ErrTransitionDeadline) ||
		errors.Is(err, ErrContinuationOriginMissing) || errors.Is(err, ErrContinuationIntervention) ||
		errors.Is(err, ErrSuccessorDenied) {
		return PathErrorRecoverable
	}
	if errors.Is(err, ErrInvalidHook) || errors.Is(err, ErrInvalidLease) ||
		errors.Is(err, ErrCompatibility) || errors.Is(err, ErrInvalidRequest) ||
		errors.Is(err, ErrInvalidDecision) || errors.Is(err, ErrContinuationUnavailable) {
		return PathErrorFatal
	}
	if host != nil {
		switch class := host(err); class {
		case PathErrorRecoverable, PathErrorFatal:
			return class
		}
	}
	return PathErrorUnclassified
}

func containsJoinedError(err error) bool {
	if _, ok := err.(interface{ Unwrap() []error }); ok {
		return true
	}
	if child := errors.Unwrap(err); child != nil {
		return containsJoinedError(child)
	}
	return false
}

// Status returns safe path diagnostics and separately reported local capacity.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Status() LifecycleStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	status := LifecycleStatus{Ready: l.started && !l.stopping}
	runningWorkers := 0
	for _, path := range l.paths {
		status.Paths = append(status.Paths, path.status)
		if !path.status.Running || !path.status.Checked || path.status.Classification != "" {
			status.Ready = false
		}
		if path.hook && path.status.Running && path.status.Checked && path.status.Classification == "" {
			runningWorkers++
		}
	}
	worker := l.worker.Status()
	available := runningWorkers - worker.Active
	if available < 0 {
		available = 0
	}
	if l.stopping || !worker.Ready {
		available = 0
	}
	status.Capacity = ExecutionCapacity{Available: available, Degraded: available == 0}
	return status
}

// RestartPath explicitly restarts a stopped path after its cause is corrected.
func (l *Lifecycle[EntityID, StateID, Event, Input]) RestartPath(name string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.started || l.stopping {
		return errors.New("lifecycle is not running")
	}
	select {
	case <-l.startup:
	default:
		return errors.New("lifecycle is starting")
	}
	for _, path := range l.paths {
		if path.name == name {
			if path.status.Running {
				return errors.New("path is not stopped")
			}
			path.status.Checked = false
			path.status.Classification = ""
			l.launchLocked(path, false)
			return nil
		}
	}
	return errors.New("unknown lifecycle path")
}

func (l *Lifecycle[EntityID, StateID, Event, Input]) wakeWorkers() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, path := range l.paths {
		if path.worker {
			select {
			case path.wake <- struct{}{}:
			default:
			}
		}
	}
}

// Wakeup is a best-effort hint; periodic durable scans remain authoritative.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Wakeup() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, path := range l.paths {
		select {
		case path.wake <- struct{}{}:
		default:
		}
	}
}

// Errors signals that Err has changed. Signals may be coalesced.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Errors() <-chan struct{} { return l.errors }

// Err returns the latest error per path for the host; it may contain sensitive data.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var result error
	for _, path := range l.paths {
		result = errors.Join(result, path.err)
	}
	return result
}

// Shutdown stops scans and claims, then drains hooks with lease renewal until
// ctx expires. A deadline error means work may still be running externally.
func (l *Lifecycle[EntityID, StateID, Event, Input]) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	if !l.started {
		l.mu.Unlock()
		return errors.New("lifecycle is not running")
	}
	l.stopping = true
	if l.notifications != nil {
		l.notifications.stopClaims()
	}
	l.cancel()
	for _, path := range l.paths {
		if path.stop != nil {
			path.stop()
		}
	}
	startup := l.startup
	l.mu.Unlock()
	defer func() {
		if ctx.Err() != nil {
			for _, path := range l.paths {
				if path.cancelWork != nil {
					path.cancelWork()
				}
			}
		}
	}()
	err := l.worker.Shutdown(ctx)
	if l.notifications != nil {
		err = errors.Join(err, l.notifications.shutdown(ctx))
	}
	select {
	case <-startup:
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
	l.mu.Lock()
	var done []<-chan struct{}
	for _, path := range l.paths {
		if path.done != nil {
			done = append(done, path.done)
		}
	}
	l.mu.Unlock()
	for _, ch := range done {
		select {
		case <-ch:
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
	}
	return err
}
