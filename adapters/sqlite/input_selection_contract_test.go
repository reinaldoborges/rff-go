package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/inputselectiontest"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestPendingInputUnknownChannelQuarantineIsIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input-integrity.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	inputselectiontest.CheckUnknownChannelQuarantine(t, store, db, func(authorize transition.IntegrityAuthorizer) (inputselectiontest.Maintenance, error) {
		return sqlite.OpenIntegrityMaintenance(path, authorize)
	})
}

func TestPendingInputSelectionContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input-selection.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inputselectiontest.CheckPendingInputSelection(t, store)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	inputselectiontest.CheckHiddenProjectionCorruptionMayOmitInput(t, store, db)
}

func TestPendingInputKnownChannelQuarantineAndRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input-quarantine.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	inputselectiontest.CheckKnownChannelQuarantine(t, store, db, func(authorize transition.IntegrityAuthorizer) (inputselectiontest.Maintenance, error) {
		return sqlite.OpenIntegrityMaintenance(path, authorize)
	})
}
