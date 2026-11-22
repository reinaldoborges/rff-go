package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
)

func TestRuntimeCompatibilityContract(t *testing.T) {
	runtimecontract.RunCompatibility(t, func(t *testing.T) runtimecontract.CompatibilityStore {
		t.Helper()
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "contract.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
}
