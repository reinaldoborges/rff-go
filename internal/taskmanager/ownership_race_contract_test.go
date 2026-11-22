package taskmanager_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/internal/fixturecontract"
	"github.com/reinaldoborges/rff-go/internal/taskmanager"
)

func TestOwnershipRaceFixtureContract(t *testing.T) {
	fixturecontract.RunOwnershipRace(t, func(t *testing.T, clock func() time.Time) fixturecontract.GovernanceFixture {
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
