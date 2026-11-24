package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestLifecycleContinuationLostAcceptanceAndRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunLifecycleContinuation(t, store, host)
}
