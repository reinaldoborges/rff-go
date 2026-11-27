package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/durableidentitytest"
)

func TestDurableIdentityByteExactRoundTrip(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "identity.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	durableidentitytest.Check(t, store)
}
