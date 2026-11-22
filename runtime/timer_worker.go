package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var ErrNoDueTimer = errors.New("no due timer available")

// TimerLease identifies one fenced delivery claim.
type TimerLease struct {
	ID         string
	Owner      string
	Generation uint64
	ExpiresAt  time.Time
}

// TimerDelivery carries the original deadline, even after delayed delivery.
type TimerDelivery[Event any] struct {
	TimerReceipt
	Event Event
}

// TimerDeliveryStore claims due timers and completes only the current claim.
type TimerDeliveryStore interface {
	ClaimDueTimer(context.Context, string, func() time.Time, time.Duration) (TimerRecord, TimerLease, error)
	RenewTimer(context.Context, TimerLease, func() time.Time, time.Duration) (TimerLease, error)
	CompleteTimer(context.Context, TimerLease, func() time.Time) error
}

// TimerWorkerConfig supplies explicit delivery lease timing and a worker clock.
type TimerWorkerConfig struct {
	ID              string
	LeaseTTL        time.Duration
	RenewalInterval time.Duration
	Clock           WorkerClock
}

// TimerWorker submits due typed events to an application-owned durable ingress.
// The host schedules RunOne. Delivery is at least once: the handler must
// durably deduplicate by timer ID before returning success.
type TimerWorker[Event any] struct {
	store   TimerDeliveryStore
	resolve CodecResolver[Event]
	handle  func(context.Context, TimerDelivery[Event]) error
	config  TimerWorkerConfig
}

func NewTimerWorker[Event any](
	store TimerDeliveryStore, resolve CodecResolver[Event], handle func(context.Context, TimerDelivery[Event]) error,
	config TimerWorkerConfig,
) (*TimerWorker[Event], error) {
	if nilcheck.IsNil(store) || resolve == nil || handle == nil || config.ID == "" || nilcheck.IsNil(config.Clock) ||
		config.LeaseTTL <= 0 || config.RenewalInterval <= 0 || config.RenewalInterval >= config.LeaseTTL {
		return nil, errors.New("timer store, codec resolver, handler, and valid lease settings are required")
	}
	return &TimerWorker[Event]{store: store, resolve: resolve, handle: handle, config: config}, nil
}

// RunOne processes one due timer. Errors leave the claim for expiry and retry.
func (w *TimerWorker[Event]) RunOne(ctx context.Context) error {
	record, lease, err := w.store.ClaimDueTimer(ctx, w.config.ID, w.config.Clock.Now, w.config.LeaseTTL)
	if err != nil {
		return err
	}
	if record.ID == "" || record.ID != lease.ID || lease.Owner != w.config.ID || lease.Generation == 0 ||
		!w.config.Clock.Now().Before(lease.ExpiresAt) {
		return ErrStaleLease
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
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
					cancelWork()
					renewed <- errors.New("worker clock closed the tick channel")
					return
				}
				if renewCtx.Err() != nil {
					renewed <- nil
					return
				}
				next, err := w.store.RenewTimer(renewCtx, current, w.config.Clock.Now, w.config.LeaseTTL)
				if err != nil {
					if renewCtx.Err() != nil {
						renewed <- nil
						return
					}
					cancelWork()
					renewed <- err
					return
				}
				current = next
			}
		}
	}()
	deliveryErr := w.deliver(workCtx, record)
	if deliveryErr == nil {
		deliveryErr = workCtx.Err()
	}
	var reportErr error
	reported := false
	if workCtx.Err() == nil && deliveryErr == nil {
		reportErr = w.store.CompleteTimer(workCtx, lease, w.config.Clock.Now)
		reported = reportErr == nil
	}
	stopRenewal()
	renewalErr := <-renewed
	if reported {
		return nil
	}
	if reportErr != nil {
		reportErr = fmt.Errorf("complete timer delivery: %w", reportErr)
	}
	if renewalErr != nil {
		renewalErr = fmt.Errorf("renew timer lease: %w", renewalErr)
	}
	return errors.Join(deliveryErr, reportErr, renewalErr, workCtx.Err())
}

func (w *TimerWorker[Event]) deliver(ctx context.Context, record TimerRecord) error {
	codec, err := resolveCodec(w.resolve, record.Event.Codec)
	if err != nil {
		return err
	}
	event, err := codec.Decode(ctx, slices.Clone(record.Event.Data))
	if err != nil {
		return compatibilityError(CompatibilityEventCodec, record.Event.Codec.Name, record.Event.Codec.Version, err)
	}
	if err := w.handle(ctx, TimerDelivery[Event]{TimerReceipt: record.TimerReceipt, Event: event}); err != nil {
		return fmt.Errorf("deliver timer %q: %w", record.ID, err)
	}
	return nil
}
