package mysql_test

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestSuccessorSharedProcessRestart(t *testing.T) {
	dsn, hostPath := os.Getenv("RFF_SUCCESSOR_SHARED_MYSQL"), os.Getenv("RFF_SUCCESSOR_SHARED_HOST")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		host, err := successorcontract.OpenHost(hostPath)
		if err != nil {
			t.Fatal(err)
		}
		successorcontract.CrashAtomic(t, store, db, host, os.Getenv("RFF_SUCCESSOR_CRASH"))
		return
	}
	for _, boundary := range []string{"before-accept", "host-write", "after-accept"} {
		t.Run(boundary, func(t *testing.T) {
			db, config := newMySQLDatabase(t)
			hostPath := filepath.Join(t.TempDir(), "origin-host.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuccessorSharedProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_SUCCESSOR_SHARED_MYSQL="+config.FormatDSN(), "RFF_SUCCESSOR_SHARED_HOST="+hostPath, "RFF_SUCCESSOR_CRASH="+boundary)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child exit: %v: %s", err, output)
			}
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			successorcontract.CheckAtomicRestart(t, store, db, boundary)
		})
	}
}
