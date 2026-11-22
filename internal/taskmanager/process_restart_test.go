package taskmanager_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestFixtureRecoversExpiryAndNotificationAcrossProcessRestart(t *testing.T) {
	const phaseEnv = "RFF_FIXTURE_RESTART_PHASE"
	const pathEnv = "RFF_FIXTURE_RESTART_PATH"
	phase := os.Getenv(phaseEnv)
	if phase == "" {
		path := filepath.Join(t.TempDir(), "restart.db")
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		for _, childPhase := range []string{"commit", "recover"} {
			command := exec.Command(executable, "-test.run=^TestFixtureRecoversExpiryAndNotificationAcrossProcessRestart$")
			command.Env = append(os.Environ(), phaseEnv+"="+childPhase, pathEnv+"="+path)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%s subprocess failed: %v\n%s", childPhase, err, output)
			}
		}
		return
	}
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := start
	if phase == "recover" {
		now = start.Add(61 * time.Minute)
	}
	fixture, err := taskmanager.Open(os.Getenv(pathEnv), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if phase == "commit" {
		if err := fixture.SeedUsers(ctx, "owner", "invitee"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.CreateGroup(ctx, "owner", "group", "owner-membership"); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Invite(ctx, "owner", "group", "invitee", "membership", "invite", start.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	defer func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	}()
	bus := taskmanager.NewNotificationBus()
	messages, unsubscribe, err := bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	if err := fixture.SetNotificationSink(bus.Publish); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := fixture.Drive(ctx); err != nil {
			t.Fatal(err)
		}
	}
	membership, err := fixture.Membership(ctx, "owner", "membership")
	if err != nil || membership.Status != taskmanager.Expired || !membership.Deadline.Equal(start.Add(time.Hour)) {
		t.Fatalf("expiry was not recovered after process exit: membership=%+v error=%v", membership, err)
	}
	select {
	case notification := <-messages:
		if notification.ID == "" || notification.MembershipID != membership.ID {
			t.Fatalf("incorrect recovered notification: %+v", notification)
		}
	default:
		t.Fatal("notification intent was lost after process exit")
	}
}
