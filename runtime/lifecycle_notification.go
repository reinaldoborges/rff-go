package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// LifecycleNotificationConfig enables a bounded pool of independent delivery workers.
type LifecycleNotificationConfig struct {
	Capacity int
	Worker   NotificationWorkerConfig
}

type notificationSlot struct {
	claimCancel context.CancelFunc
	workCancel  context.CancelFunc
}

type lifecycleNotifications[EntityID comparable, StateID comparable] struct {
	worker   *NotificationWorker[EntityID, StateID]
	capacity int
	mu       sync.Mutex
	draining bool
	drained  chan struct{}
	slots    map[*notificationSlot]struct{}
}

// EnableNotifications configures handoff and delivery before Start. Handoff and
// delivery stores may be distinct; the host must deduplicate downstream effects
// using the stable notification intent ID.
func (l *Lifecycle[EntityID, StateID, Event, Input]) EnableNotifications(
	store NotificationStore[EntityID, StateID], handoff NotificationHandoff[EntityID, StateID],
	outbox NotificationActivityStore[EntityID, StateID],
	deliver func(context.Context, NotificationDelivery[EntityID, StateID]) error,
	config LifecycleNotificationConfig,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started || l.notifications != nil {
		return errors.New("notification paths already configured or lifecycle started")
	}
	if config.Capacity <= 0 {
		return errors.New("notification worker capacity must be positive")
	}
	coordinator, err := NewNotificationCoordinator(store, handoff, l.coordinator)
	if err != nil {
		return err
	}
	worker, err := NewNotificationWorker(outbox, deliver, config.Worker)
	if err != nil {
		return err
	}
	n := &lifecycleNotifications[EntityID, StateID]{worker: worker, capacity: config.Capacity, slots: make(map[*notificationSlot]struct{})}
	l.notifications = n
	l.paths = append(l.paths, &lifecyclePath{
		name: "notification-handoff", wake: make(chan struct{}, 1),
		status: PathStatus{Name: "notification-handoff"},
		step:   func(ctx context.Context, _ func()) error { return coordinator.Reconcile(ctx) },
	})
	for i := 0; i < config.Capacity; i++ {
		name := fmt.Sprintf("notification-worker-%d", i)
		l.paths = append(l.paths, &lifecyclePath{
			name: name, wake: make(chan struct{}, 1), status: PathStatus{Name: name},
			worker: true, empty: ErrNoNotificationActivity, step: n.runOne,
		})
	}
	return nil
}

func (n *lifecycleNotifications[EntityID, StateID]) runOne(ctx context.Context, checked func()) error {
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return context.Canceled
	}
	if len(n.slots) >= n.capacity {
		n.mu.Unlock()
		return ErrWorkerAtCapacity
	}
	claimCtx, cancel := context.WithCancel(ctx)
	slot := &notificationSlot{claimCancel: cancel}
	n.slots[slot] = struct{}{}
	n.mu.Unlock()
	var exited <-chan struct{}
	defer func() {
		cancel()
		release := func() {
			n.mu.Lock()
			if slot.workCancel != nil {
				slot.workCancel()
			}
			delete(n.slots, slot)
			if n.draining && len(n.slots) == 0 {
				close(n.drained)
			}
			n.mu.Unlock()
		}
		if exited != nil {
			select {
			case <-exited:
			default:
				go func() { <-exited; release() }()
				return
			}
		}
		release()
	}()
	return n.worker.runOne(claimCtx, checked, func() (context.Context, bool) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.draining || claimCtx.Err() != nil {
			return nil, false
		}
		workCtx, workCancel := context.WithCancel(context.Background())
		slot.claimCancel = nil
		slot.workCancel = workCancel
		return workCtx, true
	}, func(ch <-chan struct{}) { exited = ch })
}

func (n *lifecycleNotifications[EntityID, StateID]) stopClaims() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.draining {
		return
	}
	n.draining = true
	n.drained = make(chan struct{})
	for slot := range n.slots {
		if slot.claimCancel != nil {
			slot.claimCancel()
		}
	}
	if len(n.slots) == 0 {
		close(n.drained)
	}
}

func (n *lifecycleNotifications[EntityID, StateID]) shutdown(ctx context.Context) error {
	n.mu.Lock()
	drained := n.drained
	n.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		n.mu.Lock()
		for slot := range n.slots {
			if slot.workCancel != nil {
				slot.workCancel()
			}
		}
		n.mu.Unlock()
		return ctx.Err()
	}
}
