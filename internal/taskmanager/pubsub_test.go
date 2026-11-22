package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestTransientPubSubRecoversUndeliveredInvitationFromOutbox(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "pubsub.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "first", "second"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	bus := taskmanager.NewNotificationBus()
	messages, unsubscribe, err := bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.SetNotificationSink(bus.Publish); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "first", "first-membership", "first", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	var first taskmanager.InvitationNotification
	select {
	case first = <-messages:
	default:
		t.Fatal("invitation was not published")
	}
	if first.ID == "" || first.MembershipID != "first-membership" {
		t.Fatalf("incorrect published invitation: %+v", first)
	}
	unsubscribe()
	if _, err := fixture.Invite(ctx, "owner", "group", "second", "second-membership", "second", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); !errors.Is(err, taskmanager.ErrNoSubscribers) {
		t.Fatalf("missing subscriber was silently treated as delivery: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	fixture, err = taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	bus = taskmanager.NewNotificationBus()
	messages, unsubscribe, err = bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	if err := fixture.SetNotificationSink(bus.Publish); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	var second taskmanager.InvitationNotification
	select {
	case second = <-messages:
	default:
		t.Fatal("pending invitation was not published")
	}
	if second.ID == "" || second.ID == first.ID || second.MembershipID != "second-membership" {
		t.Fatalf("outbox did not recover the undelivered invitation: %+v", second)
	}
}

func TestNotificationBusBackpressureDoesNotPartiallyDeliver(t *testing.T) {
	ctx := context.Background()
	bus := taskmanager.NewNotificationBus()
	first, unsubscribeFirst, err := bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribeFirst()
	second, unsubscribeSecond, err := bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribeSecond()
	initial := taskmanager.InvitationNotification{ID: "initial"}
	if err := bus.Publish(ctx, initial); err != nil {
		t.Fatal(err)
	}
	<-first
	next := taskmanager.InvitationNotification{ID: "next"}
	if err := bus.Publish(ctx, next); !errors.Is(err, taskmanager.ErrSubscriberFull) {
		t.Fatalf("expected backpressure error, got %v", err)
	}
	select {
	case notification := <-first:
		t.Fatalf("backpressured publish partially delivered %+v", notification)
	default:
	}
	<-second
	if err := bus.Publish(ctx, next); err != nil {
		t.Fatal(err)
	}
	if got := <-first; got.ID != next.ID {
		t.Fatalf("first subscriber received %+v", got)
	}
	if got := <-second; got.ID != next.ID {
		t.Fatalf("second subscriber received %+v", got)
	}
	unsubscribeFirst()
	unsubscribeFirst()
	unsubscribeSecond()
	if err := bus.Publish(ctx, next); !errors.Is(err, taskmanager.ErrNoSubscribers) {
		t.Fatalf("expected no subscribers error, got %v", err)
	}
}
