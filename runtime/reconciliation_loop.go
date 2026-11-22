package runtime

import (
	"context"
	"errors"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

// WakeupQueue coalesces transient notifications. Durable state remains the
// source of truth; notifications may be lost on process restart.
type WakeupQueue struct {
	wake chan struct{}
}

// NewWakeupQueue creates a bounded queue for reconciliation notifications.
func NewWakeupQueue() *WakeupQueue {
	return &WakeupQueue{wake: make(chan struct{}, 1)}
}

// Notify requests a scan without blocking, coalescing any pending request.
func (q *WakeupQueue) Notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// RunReconciliation scans immediately, then after notifications or polling
// ticks. It stops the supplied ticker and returns scan or context errors to the
// caller. The host must restart it after an error and choose a polling interval
// short enough to renew leases.
func RunReconciliation(ctx context.Context, queue *WakeupQueue, ticker WorkerTicker, reconcile func(context.Context) error) error {
	if nilcheck.IsNil(ctx) || queue == nil || queue.wake == nil || nilcheck.IsNil(ticker) || reconcile == nil {
		return errors.New("context, wakeup queue, ticker, and reconcile function are required")
	}
	defer ticker.Stop()
	ticks := ticker.Ticks()
	if ticks == nil {
		return errors.New("reconciliation ticker returned a nil channel")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := reconcile(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-queue.wake:
		case _, ok := <-ticks:
			if !ok {
				return errors.New("reconciliation ticker closed")
			}
		}
	}
}
