package sqlite_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestSuccessorCancellationProcessRestart(t *testing.T) {
	runtimePath, hostPath := os.Getenv("RFF_SUCCESSOR_CANCEL_SQLITE"), os.Getenv("RFF_SUCCESSOR_HOST")
	if runtimePath != "" {
		store, err := sqlite.Open[string, string](runtimePath, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		host, err := successorcontract.OpenHost(hostPath)
		if err != nil {
			t.Fatal(err)
		}
		successorcontract.CrashCancellation(t, store, host, os.Getenv("RFF_SUCCESSOR_CANCEL_CRASH"))
		return
	}
	for _, boundary := range []string{"before-host-cancel", "during-host-cancel", "after-host-cancel", "after-runtime-cancel"} {
		t.Run(boundary, func(t *testing.T) {
			dir := t.TempDir()
			runtimePath, hostPath := filepath.Join(dir, "runtime.db"), filepath.Join(dir, "host.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuccessorCancellationProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_SUCCESSOR_CANCEL_SQLITE="+runtimePath, "RFF_SUCCESSOR_HOST="+hostPath, "RFF_SUCCESSOR_CANCEL_CRASH="+boundary)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child exit: %v: %s", err, output)
			}
			store, err := sqlite.Open[string, string](runtimePath, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			host, err := successorcontract.OpenHost(hostPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
			successorcontract.CheckCancellationRestart(t, store, host, boundary)
		})
	}
}
