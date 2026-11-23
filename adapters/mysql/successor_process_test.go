package mysql_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestSuccessorSeparateHostProcessRestart(t *testing.T) {
	dsn, hostPath := os.Getenv("RFF_SUCCESSOR_MYSQL"), os.Getenv("RFF_SUCCESSOR_HOST")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		host, err := successorcontract.OpenHost(hostPath)
		if err != nil {
			t.Fatal(err)
		}
		successorcontract.Crash(t, store, host, os.Getenv("RFF_SUCCESSOR_CRASH"))
		return
	}
	for _, boundary := range []string{"before-prepare", "after-prepare", "before-host", "after-host", "before-finalize", "after-finalize"} {
		t.Run(boundary, func(t *testing.T) {
			_, config := newMySQLDatabase(t)
			hostPath := filepath.Join(t.TempDir(), "host.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuccessorSeparateHostProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_SUCCESSOR_MYSQL="+config.FormatDSN(), "RFF_SUCCESSOR_HOST="+hostPath, "RFF_SUCCESSOR_CRASH="+boundary)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child exit: %v: %s", err, output)
			}
			store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			host, err := successorcontract.OpenHost(hostPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
			successorcontract.CheckRestart(t, store, host)
		})
	}
}
