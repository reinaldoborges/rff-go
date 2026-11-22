package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/internal/nilcheck"
)

var (
	ErrNoNotificationActivity = errors.New("no notification activity is ready")
	ErrStaleNotificationLease = errors.New("notification activity lease is stale")
)

// NotificationLease fences one delivery attempt independently of the transition.
type NotificationLease struct {
	ExecutionID string
	IntentID    string
	Owner       string
	Generation  uint64
	ExpiresAt   time.Time
}

// NotificationDelivery contains the stable intent and its current attempt.
type NotificationDelivery[EntityID comparable, StateID comparable] struct {
	Intent  NotificationIntent[EntityID, StateID]
	Lease   NotificationLease
	Attempt uint64
}

// NotificationActivityStore persists claims, renewals, retry scheduling and delivery.
// Claim counts an attempt before returning; expiry permits another claim. Renewals
// and reports must validate the persisted expiry and generation.
type NotificationActivityStore[EntityID comparable, StateID comparable] interface {
	ClaimNotification(context.Context, string, func() time.Time, time.Duration) (NotificationDelivery[EntityID, StateID], error)
	RenewNotification(context.Context, NotificationLease, func() time.Time, time.Duration) (NotificationLease, error)
	RetryNotificationDelivery(context.Context, NotificationLease, func() time.Time, time.Time) error
	CompleteNotification(context.Context, NotificationLease, func() time.Time) error
}

// NotificationWorkerConfig specifies a delivery policy independent of hook policy.
type NotificationWorkerConfig struct {
	ID              string
	LeaseTTL        time.Duration
	RenewalInterval time.Duration
	AttemptTimeout  time.Duration
	RetryDelay      time.Duration
	Clock           WorkerClock
}

// NotificationWorker executes one durable delivery attempt per RunOne call.
// The host schedules calls and must make downstream delivery idempotent by intent ID.
type NotificationWorker[EntityID comparable, StateID comparable] struct {
	store   NotificationActivityStore[EntityID, StateID]
	deliver func(context.Context, NotificationDelivery[EntityID, StateID]) error
	config  NotificationWorkerConfig
}

// NewNotificationWorker constructs an independently retried delivery worker.
func NewNotificationWorker[EntityID comparable, StateID comparable](
	store NotificationActivityStore[EntityID, StateID],
	deliver func(context.Context, NotificationDelivery[EntityID, StateID]) error,
	config NotificationWorkerConfig,
) (*NotificationWorker[EntityID, StateID], error) {
	if nilcheck.IsNil(store) || deliver == nil || config.ID == "" || config.LeaseTTL <= 0 ||
		config.RenewalInterval <= 0 || config.RenewalInterval >= config.LeaseTTL ||
		config.AttemptTimeout <= 0 || config.RetryDelay <= 0 || nilcheck.IsNil(config.Clock) {
		return nil, errors.New("notification worker dependencies and valid timing are required")
	}
	return &NotificationWorker[EntityID, StateID]{store: store, deliver: deliver, config: config}, nil
}

// RunOne claims, delivers, and reports one notification. Delivery failures are
// returned after a durable retry is scheduled; cancellation leaves an expiring claim.
func (w *NotificationWorker[EntityID, StateID]) RunOne(ctx context.Context) error {
	delivery, err := w.store.ClaimNotification(ctx, w.config.ID, w.config.Clock.Now, w.config.LeaseTTL)
	if err != nil {
		return err
	}
	lease := delivery.Lease
	if delivery.Intent.ID == "" || delivery.Intent.ID != lease.IntentID || delivery.Intent.ExecutionID != lease.ExecutionID ||
		lease.Owner != w.config.ID || lease.Generation == 0 || delivery.Attempt == 0 || !w.config.Clock.Now().Before(lease.ExpiresAt) {
		return ErrStaleNotificationLease
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	attemptCtx, cancelAttempt := w.config.Clock.WithTimeout(workCtx, w.config.AttemptTimeout)
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
					cancelWork()
					renewed <- errors.New("worker clock closed the tick channel")
					return
				}
				if renewCtx.Err() != nil {
					renewed <- nil
					return
				}
				next, err := w.store.RenewNotification(renewCtx, current, w.config.Clock.Now, w.config.LeaseTTL)
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
	deliveryErr := w.deliver(attemptCtx, delivery)
	if deliveryErr == nil {
		deliveryErr = attemptCtx.Err()
	}
	var reportErr error
	reported := false
	if workCtx.Err() == nil {
		if deliveryErr == nil {
			reportErr = w.store.CompleteNotification(workCtx, lease, w.config.Clock.Now)
		} else {
			due := w.config.Clock.Now().Add(w.config.RetryDelay)
			reportErr = w.store.RetryNotificationDelivery(workCtx, lease, w.config.Clock.Now, due)
		}
		reported = reportErr == nil
	}
	stopRenewal()
	renewalErr := <-renewed
	if reported {
		return deliveryErr
	}
	if reportErr != nil {
		reportErr = fmt.Errorf("report notification delivery: %w", reportErr)
	}
	if renewalErr != nil {
		renewalErr = fmt.Errorf("renew notification lease: %w", renewalErr)
	}
	return errors.Join(deliveryErr, reportErr, renewalErr, workCtx.Err())
}
