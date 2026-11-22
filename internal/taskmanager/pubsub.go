package taskmanager

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrNoSubscribers  = errors.New("notification bus has no subscribers")
	ErrSubscriberFull = errors.New("notification subscriber buffer is full")
)

// NotificationBus is a transient, in-memory fixture channel, not durable storage.
type NotificationBus struct {
	mu          sync.Mutex
	subscribers map[chan InvitationNotification]struct{}
}

func NewNotificationBus() *NotificationBus {
	return &NotificationBus{subscribers: make(map[chan InvitationNotification]struct{})}
}

// Subscribe registers a buffered subscriber; unsubscribe closes its channel.
func (b *NotificationBus) Subscribe(capacity int) (<-chan InvitationNotification, func(), error) {
	if capacity <= 0 {
		return nil, nil, ErrInvalidRequest
	}
	channel := make(chan InvitationNotification, capacity)
	b.mu.Lock()
	b.subscribers[channel] = struct{}{}
	b.mu.Unlock()
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subscribers, channel)
			close(channel)
			b.mu.Unlock()
		})
	}
	return channel, unsubscribe, nil
}

// Publish fails without delivery if any subscriber is absent or backpressured.
func (b *NotificationBus) Publish(ctx context.Context, notification InvitationNotification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subscribers) == 0 {
		return ErrNoSubscribers
	}
	for channel := range b.subscribers {
		if len(channel) == cap(channel) {
			return ErrSubscriberFull
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for channel := range b.subscribers {
		channel <- notification
	}
	return nil
}
