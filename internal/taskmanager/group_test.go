package taskmanager_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestGroupCreationRequiresSeededIdentityAndIsolatesAccess(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fixture.db")
	fixture, err := taskmanager.Open(path, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.SeedUsers(ctx, "owner", "outsider"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.CreateGroup(ctx, "unknown", "bad-group", "bad-membership"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("unknown identity created a group: %v", err)
	}
	if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
		t.Fatal(err)
	}
	group, err := fixture.Group(ctx, "owner", "group")
	if err != nil || group.Owner != "owner" || group.Closed {
		t.Fatalf("owner cannot inspect group: group=%+v error=%v", group, err)
	}
	if _, err := fixture.Group(ctx, "outsider", "group"); !errors.Is(err, taskmanager.ErrDenied) {
		t.Fatalf("unrelated seeded user gained group access: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err = taskmanager.Open(path, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	group, err = fixture.Group(ctx, "owner", "group")
	if err != nil || group.Owner != "owner" {
		t.Fatalf("group did not survive reopen: group=%+v error=%v", group, err)
	}
}
