package mysql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestRuntimeOperationContract(t *testing.T) {
	runtimecontract.RunOperation(t, func(t *testing.T) runtimecontract.OperationStore { return openMySQL(t) })
}

func TestRuntimeOperationSuccessRace(t *testing.T) {
	runtimecontract.RunOperationSuccessRace(t, func(t *testing.T) runtimecontract.OperationStore { return openMySQL(t) })
}

func TestRuntimeOperationAbort(t *testing.T) {
	runtimecontract.RunOperationAbort(t, func(t *testing.T) runtimecontract.OperationControlStore { return openMySQL(t) })
}

func TestRuntimeOperationAbortRace(t *testing.T) {
	runtimecontract.RunOperationAbortRace(t, func(t *testing.T) runtimecontract.OperationControlStore { return openMySQL(t) })
}

func TestRuntimeConcurrentOperations(t *testing.T) {
	runtimecontract.RunConcurrentOperations(t, func(t *testing.T) runtimecontract.OperationStore { return openMySQL(t) })
}

func TestRuntimeOperationMigration(t *testing.T) {
	runtimecontract.RunOperationMigration(t, func(t *testing.T) runtimecontract.OperationMigrationStore { return openMySQL(t) })
}

func TestOperationProcessRestart(t *testing.T) {
	open := func(dsn string) (runtimecontract.RestartOperationStore, error) {
		return mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	}
	if os.Getenv(runtimecontract.OperationRestartPhase) != "" {
		runtimecontract.OperationRestartChild(t, open)
		return
	}
	for _, phase := range []string{"intent", "dispatch", "result"} {
		t.Run(phase, func(t *testing.T) {
			_, config := newMySQLDatabase(t)
			runtimecontract.RunOperationRestart(t, config.FormatDSN(), phase, open)
		})
	}
}

func TestOperationRecordIsolation(t *testing.T) {
	db, config := newMySQLDatabase(t)
	open := func(dsn string) (runtimecontract.RestartOperationStore, error) {
		return mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	}
	runtimecontract.RunOperationIsolation(t, config.FormatDSN(), db, open, mysql.ErrUnsupportedSchema)
}

func openMySQL(t *testing.T) *mysql.Store[string, string] {
	t.Helper()
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func newMySQLDatabase(t *testing.T) (*sql.DB, *driver.Config) {
	t.Helper()
	dsn := os.Getenv("RFF_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RFF_MYSQL_DSN is not configured")
	}
	config, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	databaseName := "rff_contract_" + hex.EncodeToString(suffix[:])
	adminDB, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.ExecContext(t.Context(), "CREATE DATABASE `"+databaseName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
		adminDB.Close()
		t.Fatal(err)
	}
	config.DBName = databaseName
	databaseDB, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		_, dropErr := adminDB.ExecContext(t.Context(), "DROP DATABASE `"+databaseName+"`")
		adminDB.Close()
		t.Fatal(errors.Join(err, dropErr))
	}
	t.Cleanup(func() {
		if err := databaseDB.Close(); err != nil {
			t.Error(err)
		}
		if _, err := adminDB.ExecContext(context.Background(), "DROP DATABASE `"+databaseName+"`"); err != nil {
			t.Error(err)
		}
		if err := adminDB.Close(); err != nil {
			t.Error(err)
		}
	})
	return databaseDB, config
}

func TestRuntimeStoreContract(t *testing.T) {
	runtimecontract.Run(t, func(t *testing.T) runtimecontract.Store { return openMySQL(t) })
}

func TestRuntimeTimerCompatibilityContract(t *testing.T) {
	runtimecontract.RunTimerCompatibility(t, func(t *testing.T) runtimecontract.TimerStore { return openMySQL(t) })
}

func TestRuntimeInputIngressContract(t *testing.T) {
	runtimecontract.RunInputIngress(t, func(t *testing.T) transition.InputStore { return openMySQL(t) })
}

func TestRuntimeAuthorizedControlContract(t *testing.T) {
	runtimecontract.RunAuthorizedControl(t, func(t *testing.T) runtimecontract.ControlStore { return openMySQL(t) })
}

func TestRuntimeMigrationContract(t *testing.T) {
	runtimecontract.RunMigration(t, func(t *testing.T) runtimecontract.MigrationStore { return openMySQL(t) })
}

func TestRuntimeCapacityContract(t *testing.T) {
	runtimecontract.RunCapacity(t, func(t *testing.T) runtimecontract.Store { return openMySQL(t) })
}

func TestRuntimeRetryBudgetContract(t *testing.T) {
	runtimecontract.RunRetryBudget(t, func(t *testing.T) runtimecontract.ControlStore { return openMySQL(t) })
}

func TestRuntimeCompatibilityContract(t *testing.T) {
	runtimecontract.RunCompatibility(t, func(t *testing.T) runtimecontract.CompatibilityStore { return openMySQL(t) })
}

func TestRuntimeNotificationDeliveryContract(t *testing.T) {
	runtimecontract.RunNotificationDelivery(t, func(t *testing.T, clock func() time.Time) runtimecontract.NotificationDeliveryFixture {
		store := openMySQL(t)
		outbox, err := mysql.NewNotificationOutbox(store, clock)
		if err != nil {
			t.Fatal(err)
		}
		return runtimecontract.NotificationDeliveryFixture{Store: store, Handoff: outbox, Outbox: outbox}
	})
}

func TestRuntimeVersionsContract(t *testing.T) {
	runtimecontract.RunVersions(t, func(t *testing.T) runtimecontract.Store { return openMySQL(t) })
}

func TestRuntimeNotificationRepairContract(t *testing.T) {
	runtimecontract.RunNotificationRepair(t, func(t *testing.T) runtimecontract.NotificationRepairStore { return openMySQL(t) })
}
