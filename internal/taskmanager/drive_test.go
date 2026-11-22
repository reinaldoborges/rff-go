package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

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
