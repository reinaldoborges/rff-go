package mysql_test

import (
	"strings"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
)

func TestOpenRejectsNonStrictConnection(t *testing.T) {
	_, config := newMySQLDatabase(t)
	if config.Params == nil {
		config.Params = make(map[string]string)
	}
	config.Params["sql_mode"] = "''"
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err == nil {
		store.Close()
		t.Fatal("non-strict connection accepted")
	}
	if !strings.Contains(err.Error(), "strict") {
		t.Fatalf("unexpected error: %v", err)
	}
}
