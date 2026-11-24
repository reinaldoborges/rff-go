package mysql_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestSuccessorSeparateHostContract(t *testing.T) {
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.Run(t, store, host)
}
