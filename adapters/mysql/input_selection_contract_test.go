package mysql_test

import (
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/inputselectiontest"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestPendingInputUnknownChannelQuarantineIsIncomplete(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inputselectiontest.CheckUnknownChannelQuarantine(t, store, db, func(authorize transition.IntegrityAuthorizer) (inputselectiontest.Maintenance, error) {
		return mysql.OpenIntegrityMaintenance(config.FormatDSN(), authorize)
	})
}

func TestPendingInputSelectionContract(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inputselectiontest.CheckPendingInputSelection(t, store)
	inputselectiontest.CheckHiddenProjectionCorruptionMayOmitInput(t, store, db)
}

func TestPendingInputKnownChannelQuarantineAndRepair(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inputselectiontest.CheckKnownChannelQuarantine(t, store, db, func(authorize transition.IntegrityAuthorizer) (inputselectiontest.Maintenance, error) {
		return mysql.OpenIntegrityMaintenance(config.FormatDSN(), authorize)
	})
}
