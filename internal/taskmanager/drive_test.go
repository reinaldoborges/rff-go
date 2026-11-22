package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestIncompatibleExecutionDoesNotStopIndependentInvitation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "incompatible-execution.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "other-entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "000-unavailable", Entity: "other-entity", Key: "key",
			Operation: transition.Initialize, MachineID: "older-machine", MachineVersion: "1", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "older-input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable execution was not reported: %v", err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited {
		t.Fatalf("unavailable execution stopped independent invitation: membership=%+v error=%v", membership, err)
	}
}

func TestIncompatibleUnknownAttemptDoesNotStopIndependentInvitation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "incompatible-unknown.db")
	fixture, err := taskmanager.Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Accept(ctx, "other-entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "000-unavailable", Entity: "other-entity", Key: "key",
			Operation: transition.Initialize, MachineID: "older-machine", MachineVersion: "1", Target: "open",
			Input:  transition.EncodedValue{Codec: transition.CodecVersion{Name: "older-input", Version: "1"}, Data: []byte("payload")},
			Status: transition.Waiting, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTransitionLease(ctx, "other-entity", "000-unavailable", "taskmanager", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, lease, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimHook(ctx, "lost-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, claim, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if err := fixture.Drive(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable classifier was not reported: %v", err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited {
		t.Fatalf("unavailable classifier stopped independent invitation: membership=%+v error=%v", membership, err)
	}
}

func TestIncompatibleTimerDoesNotDelayIndependentExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "incompatible-timer.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", deadline); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.ScheduleTimer(ctx, "000-unavailable", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{
			SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt:  transition.TimerReceipt{ID: "000-unavailable", WorkflowID: "other", Deadline: deadline},
			Event:         transition.EncodedValue{Codec: transition.CodecVersion{Name: "older-event", Version: "1"}, Data: []byte("payload")},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	now = deadline
	for range 2 {
		if err := fixture.Drive(ctx); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("unavailable timer was not reported: %v", err)
		}
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Expired {
		t.Fatalf("unavailable timer delayed independent expiry: membership=%+v error=%v", membership, err)
	}
}

func TestIncompatibleTimerReplayDoesNotStopIndependentExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "incompatible-timer-replay.db")
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
	deadline := now.Add(time.Hour)
	for _, item := range []struct{ user, membership string }{{"first", "a"}, {"second", "b"}} {
		if _, err := fixture.Invite(ctx, "owner", "group", item.user, item.membership, "invite-"+item.membership, deadline); err != nil {
			t.Fatal(err)
		}
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.ScheduleTimer(ctx, "taskmanager-expiry:a", func(*transition.TimerRecord) (*transition.TimerRecord, error) {
		return &transition.TimerRecord{
			SchemaVersion: transition.CurrentTimerSchemaVersion,
			TimerReceipt:  transition.TimerReceipt{ID: "taskmanager-expiry:a", WorkflowID: "a", Deadline: deadline},
			Event:         transition.EncodedValue{Codec: transition.CodecVersion{Name: "older-event", Version: "1"}, Data: []byte("a")},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable timer replay was not reported: %v", err)
	}
	now = deadline
	for range 2 {
		if err := fixture.Drive(ctx); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("unavailable timer replay was not reported: %v", err)
		}
	}
	membership, err := fixture.Membership(ctx, "owner", "b")
	if err != nil || membership.Status != taskmanager.Expired {
		t.Fatalf("unavailable timer replay stopped independent expiry: membership=%+v error=%v", membership, err)
	}
}

func TestIncompatiblePendingInputDoesNotStopIndependentInvitation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "incompatible-input.db")
	fixture, err := taskmanager.Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for _, channel := range []string{"taskmanager-invitations", "taskmanager-tasks", "taskmanager-decisions"} {
		id := "unavailable-" + channel
		_, _, err = store.AcceptInput(ctx, id, func(*transition.InputRecord) (*transition.InputRecord, error) {
			return &transition.InputRecord{
				SchemaVersion: 1, ID: id, Channel: channel, WorkflowID: "other",
				Kind: transition.Command, Value: transition.EncodedValue{
					Codec: transition.CodecVersion{Name: "older-input", Version: "1"}, Data: []byte("payload"),
				},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Drive(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable input was not reported: %v", err)
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Invited {
		t.Fatalf("unavailable input stopped independent invitation: membership=%+v error=%v", membership, err)
	}
}
