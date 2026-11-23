package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
)

func TestContinuationWaitsForReceiptAndReservationRelease(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "ready.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunReadiness(t, store)
}

func TestContinuationPreservesExecutionIdentities(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "identity.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunIdentitySafety(t, store)
}

func TestContinuationMigrationContract(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "migration.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunMigration(t, store)
}

func TestContinuationDiscoveryPaginationContract(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "pagination.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunPagination(t, store)
}
