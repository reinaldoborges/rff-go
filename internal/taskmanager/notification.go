package taskmanager

import (
	"context"
	"errors"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// InvitationNotification is an at-least-once delivery of a durable intent.
type InvitationNotification struct {
	ID           string
	MembershipID string
	GroupID      string
	UserID       string
	Deadline     time.Time
}

// SetNotificationSink injects a transient test delivery adapter. It must
// deduplicate external effects by notification ID.
func (f *Fixture) SetNotificationSink(sink func(context.Context, InvitationNotification) error) error {
	if sink == nil {
		return ErrInvalidRequest
	}
	f.sinkMu.Lock()
	defer f.sinkMu.Unlock()
	f.sink = sink
	return nil
}

func (f *Fixture) driveNotifications(ctx context.Context) error {
	f.sinkMu.RLock()
	sink := f.sink
	f.sinkMu.RUnlock()
	if sink == nil {
		return nil
	}
	outbox, err := sqlite.NewNotificationOutbox(f.store, f.clock)
	if err != nil {
		return err
	}
	worker, err := transition.NewNotificationWorker[string, string](outbox,
		func(ctx context.Context, delivery transition.NotificationDelivery[string, string]) error {
			membership, _, _, err := f.readMembership(ctx, delivery.Intent.Entity)
			if err != nil {
				return err
			}
			return sink(ctx, InvitationNotification{
				ID: delivery.Intent.ID, MembershipID: membership.ID, GroupID: membership.GroupID,
				UserID: membership.UserID, Deadline: membership.Deadline,
			})
		},
		transition.NotificationWorkerConfig{
			ID: "taskmanager-notifications", LeaseTTL: time.Minute, RenewalInterval: 10 * time.Second,
			AttemptTimeout: time.Minute, RetryDelay: time.Minute, Clock: fixtureClock{now: f.clock},
		})
	if err != nil {
		return err
	}
	for {
		err := worker.RunOne(ctx)
		if errors.Is(err, transition.ErrNoNotificationActivity) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
