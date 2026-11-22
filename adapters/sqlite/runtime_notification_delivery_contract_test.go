package sqlite_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
)

func TestRuntimeNotificationDeliveryContract(t *testing.T) {
	runtimecontract.RunNotificationDelivery(t, func(t *testing.T, clock func() time.Time) runtimecontract.NotificationDeliveryFixture {
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
		outbox, err := sqlite.NewNotificationOutbox(store, clock)
		if err != nil {
			t.Fatal(err)
		}
		return runtimecontract.NotificationDeliveryFixture{Store: store, Handoff: outbox, Outbox: outbox}
	})
}
