package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// TimerLifecycleConfig enables a bounded pool of durable timer deliveries.
type TimerLifecycleConfig struct {
	Capacity int
	Worker   TimerWorkerConfig
}

type timerLifecycleWork struct {
	mu       sync.Mutex
	stopping bool
	active   map[*timerLifecycleSlot]struct{}
}

type timerLifecycleSlot struct {
	cancel context.CancelFunc
}

func (w *timerLifecycleWork) start(claim context.Context, slot *timerLifecycleSlot) (context.Context, context.CancelFunc, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping || claim.Err() != nil {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	slot.cancel = cancel
	return ctx, cancel, true
}

func (w *timerLifecycleWork) stop() {
	w.mu.Lock()
	w.stopping = true
	w.mu.Unlock()
}

func (w *timerLifecycleWork) cancelWork() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for slot := range w.active {
		if slot.cancel != nil {
			slot.cancel()
		}
	}
}

// EnableTimerDelivery enables timer delivery before Start. The handler owns
// authorization and must durably deduplicate its effects by timer ID.
func (l *Lifecycle[EntityID, StateID, Event, Input]) EnableTimerDelivery(
	store TimerDeliveryStore, resolve CodecResolver[Event],
	handle func(context.Context, TimerDelivery[Event]) error, config TimerLifecycleConfig,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return errors.New("timer delivery must be enabled before lifecycle start")
	}
	for _, path := range l.paths {
		if path.name == "timer-worker-0" {
			return errors.New("timer delivery already enabled")
		}
	}
	if config.Capacity <= 0 || l.interval >= config.Worker.LeaseTTL {
		return errors.New("timer delivery requires positive capacity and poll interval shorter than lease")
	}
	worker, err := NewTimerWorker(store, resolve, handle, config.Worker)
	if err != nil {
		return err
	}
	work := &timerLifecycleWork{active: make(map[*timerLifecycleSlot]struct{})}
	for i := 0; i < config.Capacity; i++ {
		name := fmt.Sprintf("timer-worker-%d", i)
		path := &lifecyclePath{
			name: name, wake: make(chan struct{}, 1), status: PathStatus{Name: name},
			worker: true, empty: ErrNoDueTimer, stop: work.stop, cancelWork: work.cancelWork,
		}
		path.step = func(ctx context.Context, checked func()) error {
			slot := &timerLifecycleSlot{}
			work.mu.Lock()
			if work.stopping {
				work.mu.Unlock()
				return context.Canceled
			}
			work.active[slot] = struct{}{}
			work.mu.Unlock()
			defer func() {
				work.mu.Lock()
				delete(work.active, slot)
				work.mu.Unlock()
			}()
			return worker.runOne(ctx, checked, func() (context.Context, context.CancelFunc, bool) {
				return work.start(ctx, slot)
			})
		}
		l.paths = append(l.paths, path)
	}
	return nil
}
