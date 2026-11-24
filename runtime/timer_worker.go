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
// Compatibility blocks fence a claim; clearing a block must compare both its
// diagnostic and generation so stale revalidation cannot clear a newer block.
// BlockedTimers returns encoded application events and requires host protection.
type TimerDeliveryStore interface {
	ClaimDueTimer(context.Context, string, func() time.Time, time.Duration) (TimerRecord, TimerLease, error)
	RenewTimer(context.Context, TimerLease, func() time.Time, time.Duration) (TimerLease, error)
	CompleteTimer(context.Context, TimerLease, func() time.Time) error
	BlockTimerCompatibility(context.Context, TimerLease, func() time.Time, CompatibilityDiagnostic) error
	BlockedTimers(context.Context) ([]TimerRecord, error)
	ClearTimerCompatibility(context.Context, string, uint64, CompatibilityDiagnostic) error
}

// TimerWorkerConfig supplies explicit delivery lease timing and a worker clock.
type TimerWorkerConfig struct {
	ID              string
	LeaseTTL        time.Duration
	RenewalInterval time.Duration
	Clock           WorkerClock
}

// TimerWorker submits due typed events to an application-owned durable ingress.
// The host schedules RunOne or enables lifecycle delivery. Delivery is at
// least once: the handler must durably deduplicate by timer ID before success.
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

// RunOne revalidates blocked codecs and processes one due timer. Handler errors
// leave the claim for expiry and retry; codec failures persist a fenced block.
func (w *TimerWorker[Event]) RunOne(ctx context.Context) error {
	return w.runOne(ctx, nil, nil)
}

func (w *TimerWorker[Event]) runOne(ctx context.Context, checked func(), start func() (context.Context, context.CancelFunc, bool)) error {
	unavailable, err := w.revalidateCompatibility(ctx)
	if err != nil {
		return err
	}
	record, lease, err := w.store.ClaimDueTimer(ctx, w.config.ID, w.config.Clock.Now, w.config.LeaseTTL)
	if err != nil {
		if errors.Is(err, ErrNoDueTimer) && unavailable != nil {
			return unavailable
		}
		return err
	}
	if record.ID == "" || record.ID != lease.ID || lease.Owner != w.config.ID || lease.Generation == 0 ||
		!w.config.Clock.Now().Before(lease.ExpiresAt) {
		return ErrStaleLease
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if checked != nil {
		checked()
	}
	if start != nil {
		work, cancel, ok := start()
		if !ok {
			return context.Canceled
		}
		defer cancel()
		ctx = work
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
	event, deliveryErr := w.decode(workCtx, record)
	var compatibility *CompatibilityError
	if deliveryErr == nil {
		deliveryErr = w.handle(workCtx, TimerDelivery[Event]{TimerReceipt: record.TimerReceipt, Event: event})
		if deliveryErr != nil {
			deliveryErr = fmt.Errorf("deliver timer %q: %w", record.ID, deliveryErr)
		}
	} else {
		errors.As(deliveryErr, &compatibility)
	}
	if deliveryErr == nil {
		deliveryErr = workCtx.Err()
	}
	if compatibility != nil {
		stopRenewal()
		renewalErr := <-renewed
		if renewalErr != nil {
			renewalErr = fmt.Errorf("renew timer lease: %w", renewalErr)
		}
		var blockErr error
		if workCtx.Err() == nil && renewalErr == nil {
			blockErr = w.store.BlockTimerCompatibility(workCtx, lease, w.config.Clock.Now, compatibility.Diagnostic)
			if blockErr != nil {
				blockErr = fmt.Errorf("block incompatible timer: %w", blockErr)
			}
		}
		return errors.Join(deliveryErr, blockErr, renewalErr, workCtx.Err())
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
		reportErr = fmt.Errorf("record timer delivery outcome: %w", reportErr)
	}
	if renewalErr != nil {
		renewalErr = fmt.Errorf("renew timer lease: %w", renewalErr)
	}
	return errors.Join(deliveryErr, reportErr, renewalErr, workCtx.Err())
}

func (w *TimerWorker[Event]) decode(ctx context.Context, record TimerRecord) (Event, error) {
	var zero Event
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	codec, err := resolveCodec(w.resolve, record.Event.Codec)
	if err != nil {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		return zero, compatibilityError(CompatibilityEventCodec, record.Event.Codec.Name, record.Event.Codec.Version, err)
	}
	event, err := codec.Decode(ctx, slices.Clone(record.Event.Data))
	if err != nil {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		return zero, compatibilityError(CompatibilityEventCodec, record.Event.Codec.Name, record.Event.Codec.Version, err)
	}
	return event, nil
}

func (w *TimerWorker[Event]) revalidateCompatibility(ctx context.Context) (unavailable error, err error) {
	records, err := w.store.BlockedTimers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list incompatible timers: %w", err)
	}
	for _, record := range records {
		if record.Compatibility == nil {
			return nil, ErrInvalidDecision
		}
		if _, err := w.decode(ctx, record); err != nil {
			if errors.Is(err, ErrCompatibility) && ctx.Err() == nil {
				unavailable = errors.Join(unavailable, fmt.Errorf("timer %q: %w", record.ID, err))
				continue
			}
			return nil, err
		}
		if err := w.store.ClearTimerCompatibility(ctx, record.ID, record.Generation, *record.Compatibility); err != nil && !errors.Is(err, ErrConflict) {
			return nil, fmt.Errorf("revalidate timer %q: %w", record.ID, err)
		}
	}
	return unavailable, nil
}
