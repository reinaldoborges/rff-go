package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/selectionevidence"
)

func TestSelectionHandoffPlans(t *testing.T) {
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	selectionevidence.Run(t, db, store, "mysql", func(id string, now time.Time) {
		createMySQLNotificationWork(t, context.Background(), store, id, "ready", now)
	})
}
