package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var (
	ErrWorkerDraining     = errors.New("worker is draining")
	ErrWorkerAtCapacity   = errors.New("worker is at capacity")
	ErrNoHookActivity     = errors.New("no hook activity is ready")
	ErrTransitionDeadline = errors.New("transition deadline elapsed")
	ErrStaleActivityLease = errors.New("hook activity lease is stale")
)

// HookLease identifies one fenced worker claim.
type HookLease struct {
	ExecutionID string
	ActivityID  string
	Owner       string
	Generation  uint64
	ExpiresAt   time.Time
}

// StartedHook is the durable start time and identity of one hook attempt.
type StartedHook struct {
	Attempt   fsm.AttemptMetadata
	StartedAt time.Time
}

// HookActivityStore is the durable worker side of the activity seam.
// Implementations atomically fence claims, starts, renewals, and reports.
// InspectHook returns the immutable snapshot bound to the claim; migration
// must fence that claim before modifying it. A started attempt is counted
// before StartHook returns. A claim not started before expiry does not consume
// an attempt. Renewals and reports validate the persisted expiry and fence;
// they must not treat the expiry copied into HookLease as authoritative.
type HookActivityStore[EntityID comparable, StateID comparable] interface {
	ClaimHook(context.Context, string, func() time.Time, time.Duration) (HookLease, error)
	InspectHook(context.Context, HookLease, func() time.Time) (Execution[EntityID, StateID], HookCall, error)
	StartHook(context.Context, HookLease, func() time.Time) (StartedHook, error)
	RenewHook(context.Context, HookLease, func() time.Time, time.Duration) (HookLease, error)
	ReportHook(context.Context, HookLease, HookResult, func() time.Time) error
	BlockHookCompatibility(context.Context, HookLease, CompatibilityDiagnostic, func() time.Time) error
}

// WorkerTicker is a stoppable ticker driven by a worker clock.
type WorkerTicker interface {
	Ticks() <-chan time.Time
	Stop()
}

// WorkerClock supplies time and timers for worker scheduling and tests.
// Its methods must be concurrency-safe, nonblocking, and not re-enter the worker.
type WorkerClock interface {
	Now() time.Time
	NewTicker(time.Duration) WorkerTicker
	WithTimeout(context.Context, time.Duration) (context.Context, context.CancelFunc)
}

type systemTicker struct{ *time.Ticker }

func (t systemTicker) Ticks() <-chan time.Time { return t.C }

// SystemWorkerClock uses the system clock and Go timers.
type SystemWorkerClock struct{}

func (SystemWorkerClock) Now() time.Time { return time.Now() }
func (SystemWorkerClock) NewTicker(d time.Duration) WorkerTicker {
	return systemTicker{time.NewTicker(d)}
}
func (SystemWorkerClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

// WorkerConfig contains explicit deployment choices.
type WorkerConfig struct {
	ID              string
	Capacity        int
	LeaseTTL        time.Duration
	RenewalInterval time.Duration
	Clock           WorkerClock
}

// WorkerStatus reports execution capacity independently of persistence health.
type WorkerStatus struct {
	Ready     bool
	Draining  bool
	Capacity  int
	Active    int
	Heartbeat time.Time
}

type workerSlot struct {
	claimCancel context.CancelFunc
	workCancel  context.CancelFunc
}

// HookWorker claims and executes state hooks; it never commits domain state.
type HookWorker[EntityID comparable, StateID comparable, Event, Input any] struct {
	store  HookActivityStore[EntityID, StateID]
	runner *HookRunner[EntityID, StateID, Event, Input]
	config WorkerConfig

	mu       sync.Mutex
	draining bool
	slots    map[*workerSlot]struct{}
	drained  chan struct{}
}

// NewHookWorker constructs a worker with explicit capacity and lease settings.
func NewHookWorker[EntityID comparable, StateID comparable, Event, Input any](
	store HookActivityStore[EntityID, StateID], runner *HookRunner[EntityID, StateID, Event, Input], config WorkerConfig,
) (*HookWorker[EntityID, StateID, Event, Input], error) {
	if nilcheck.IsNil(store) || runner == nil || config.ID == "" || config.Capacity <= 0 ||
		config.LeaseTTL <= 0 || config.RenewalInterval <= 0 || config.RenewalInterval >= config.LeaseTTL || nilcheck.IsNil(config.Clock) {
		return nil, errors.New("worker dependencies and valid capacity and lease settings are required")
	}
	return &HookWorker[EntityID, StateID, Event, Input]{store: store, runner: runner, config: config, slots: make(map[*workerSlot]struct{})}, nil
}

// Status returns a worker heartbeat and current readiness and capacity.
func (w *HookWorker[EntityID, StateID, Event, Input]) Status() WorkerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WorkerStatus{
		Ready: !w.draining, Draining: w.draining, Capacity: w.config.Capacity,
		Active: len(w.slots), Heartbeat: w.config.Clock.Now(),
	}
}

// RunOne claims and executes at most one hook activity.
func (w *HookWorker[EntityID, StateID, Event, Input]) RunOne(ctx context.Context) error {
	w.mu.Lock()
	if w.draining {
		w.mu.Unlock()
		return ErrWorkerDraining
	}
	if len(w.slots) >= w.config.Capacity {
		w.mu.Unlock()
		return ErrWorkerAtCapacity
	}
	claimCtx, claimCancel := context.WithCancel(ctx)
	slot := &workerSlot{claimCancel: claimCancel}
	w.slots[slot] = struct{}{}
	w.mu.Unlock()
	defer func() {
		claimCancel()
		w.mu.Lock()
		delete(w.slots, slot)
		if w.draining && len(w.slots) == 0 {
			close(w.drained)
		}
		w.mu.Unlock()
	}()

	lease, err := w.store.ClaimHook(claimCtx, w.config.ID, w.config.Clock.Now, w.config.LeaseTTL)
	if err != nil {
		return err
	}
	if err := claimCtx.Err(); err != nil {
		return err
	}
	if lease.ExecutionID == "" || lease.ActivityID == "" || lease.Owner != w.config.ID ||
		lease.Generation == 0 || !w.config.Clock.Now().Before(lease.ExpiresAt) {
		return ErrStaleActivityLease
	}
	execution, call, err := w.store.InspectHook(claimCtx, lease, w.config.Clock.Now)
	if err != nil {
		return w.recordCompatibility(claimCtx, lease, err)
	}
	if execution.ID != lease.ExecutionID {
		return fmt.Errorf("%w: execution does not match claim", ErrInvalidHook)
	}
	prepared, err := w.runner.prepare(claimCtx, execution, call)
	if err != nil {
		return w.recordCompatibility(claimCtx, lease, err)
	}
	deadline := execution.EffectiveDeadline()
	if execution.Policy.AttemptTimeout <= 0 || deadline.IsZero() {
		return fmt.Errorf("%w: invalid execution timing", ErrCompatibility)
	}
	if !w.config.Clock.Now().Before(deadline) {
		return ErrTransitionDeadline
	}
	started, err := w.store.StartHook(claimCtx, lease, w.config.Clock.Now)
	if err != nil {
		return err
	}
	attempt := started.Attempt
	if attempt.ExecutionID != lease.ExecutionID || attempt.ActivityID != lease.ActivityID || attempt.LeaseGeneration != lease.Generation {
		return fmt.Errorf("%w: started attempt does not match claim", ErrInvalidHook)
	}
	w.mu.Lock()
	if w.draining || claimCtx.Err() != nil {
		w.mu.Unlock()
		return ErrWorkerDraining
	}
	workCtx, workCancel := context.WithCancel(ctx)
	slot.claimCancel = nil
	slot.workCancel = workCancel
	w.mu.Unlock()
	defer workCancel()

	now := w.config.Clock.Now()
	if started.StartedAt.IsZero() || now.IsZero() || started.StartedAt.After(now) {
		return fmt.Errorf("%w: invalid attempt start time", ErrInvalidHook)
	}
	if !now.Before(lease.ExpiresAt) {
		return ErrStaleActivityLease
	}
	timeout := execution.Policy.AttemptTimeout - now.Sub(started.StartedAt)
	remaining := deadline.Sub(now)
	if remaining < timeout {
		timeout = remaining
	}
	attemptCtx, cancelAttempt := w.config.Clock.WithTimeout(workCtx, timeout)
	if nilcheck.IsNil(attemptCtx) || cancelAttempt == nil {
		if cancelAttempt != nil {
			cancelAttempt()
		}
		return errors.New("worker clock returned an invalid timeout")
	}
	defer cancelAttempt()
	ticker := w.config.Clock.NewTicker(w.config.RenewalInterval)
	if nilcheck.IsNil(ticker) {
		return errors.New("worker clock returned a nil ticker")
	}
	ticks := ticker.Ticks()
	if ticks == nil {
		ticker.Stop()
		return errors.New("worker clock returned a nil tick channel")
	}
	renewCtx, stopRenewal := context.WithCancel(workCtx)
	renewed := make(chan error, 1)
	go func() {
		defer ticker.Stop()
		current := lease
		for {
			select {
			case <-renewCtx.Done():
				renewed <- nil
				return
			case _, ok := <-ticks:
				if !ok {
					workCancel()
					renewed <- errors.New("worker clock closed the tick channel")
					return
				}
				if renewCtx.Err() != nil {
					renewed <- nil
					return
				}
				next, err := w.store.RenewHook(renewCtx, current, w.config.Clock.Now, w.config.LeaseTTL)
				if err != nil {
					if renewCtx.Err() != nil {
						renewed <- nil
						return
					}
					workCancel()
					renewed <- err
					return
				}
				current = next
			}
		}
	}()
	result, err := prepared.run(attemptCtx, attempt)
	var reportErr error
	reported := false
	if err == nil && workCtx.Err() == nil {
		reportErr = w.store.ReportHook(workCtx, lease, result, w.config.Clock.Now)
		reported = reportErr == nil
	} else if workCtx.Err() == nil {
		var compatibility *CompatibilityError
		if errors.As(err, &compatibility) {
			diagnostic := compatibility.Diagnostic
			diagnostic.Check = CompatibilityUnknownAttempt
			reportErr = w.store.BlockHookCompatibility(workCtx, lease, diagnostic, w.config.Clock.Now)
			reported = reportErr == nil
		}
	}
	stopRenewal()
	renewalErr := <-renewed
	if reported {
		return err
	}
	if renewalErr != nil {
		return fmt.Errorf("renew hook lease: %w", renewalErr)
	}
	if reportErr != nil {
		return errors.Join(err, fmt.Errorf("report hook outcome: %w", reportErr))
	}
	if err != nil {
		return err
	}
	if err := workCtx.Err(); err != nil {
		return err
	}
	return reportErr
}

func (w *HookWorker[EntityID, StateID, Event, Input]) recordCompatibility(ctx context.Context, lease HookLease, err error) error {
	var compatibility *CompatibilityError
	if errors.As(err, &compatibility) {
		if blockErr := w.store.BlockHookCompatibility(ctx, lease, compatibility.Diagnostic, w.config.Clock.Now); blockErr != nil {
			return errors.Join(err, fmt.Errorf("record compatibility block: %w", blockErr))
		}
	}
	return err
}

// Shutdown stops claims and drains work until ctx ends, then cancels work.
func (w *HookWorker[EntityID, StateID, Event, Input]) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	if !w.draining {
		w.draining = true
		w.drained = make(chan struct{})
		for slot := range w.slots {
			if slot.claimCancel != nil {
				slot.claimCancel()
			}
		}
		if len(w.slots) == 0 {
			close(w.drained)
		}
	}
	drained := w.drained
	w.mu.Unlock()
	select {
	case <-drained:
		return nil
	default:
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		w.mu.Lock()
		for slot := range w.slots {
			if slot.workCancel != nil {
				slot.workCancel()
			}
		}
		w.mu.Unlock()
		return ctx.Err()
	}
}
