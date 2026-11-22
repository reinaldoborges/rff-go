package taskmanager_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/fixturecontract"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestNotificationFixtureContract(t *testing.T) {
	fixturecontract.RunNotification(t, func(t *testing.T, clock func() time.Time) fixturecontract.NotificationFixture {
		t.Helper()
		fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "contract.db"), clock)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := fixture.Close(); err != nil {
				t.Error(err)
			}
		})
		return fixture
	})
}

func TestDenialIsolationFixtureContract(t *testing.T) {
	fixturecontract.RunDenialIsolation(t, func(t *testing.T, clock func() time.Time) fixturecontract.DenialFixture {
		t.Helper()
		fixture, err := taskmanager.Open(filepath.Join(t.TempDir(), "contract.db"), clock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := fixture.Close(); err != nil {
				t.Error(err)
			}
		})
		return fixture
	})
}
