package sqlite_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
)

func TestContinuationCrossStoreProcessRestart(t *testing.T) {
	path, hostPath := os.Getenv("RFF_SQLITE_CROSS_RUNTIME"), os.Getenv("RFF_CROSS_HOST")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		host, err := continuationcontract.OpenCrossStoreHost(hostPath, os.Getenv("RFF_CROSS_CRASH"))
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunCrossStoreCrash(t, store, host)
		return
	}
	for _, boundary := range []string{"before-host", "host", "before-ack", "after-ack"} {
		t.Run(boundary, func(t *testing.T) {
			dir := t.TempDir()
			path, hostPath := filepath.Join(dir, "runtime.db"), filepath.Join(dir, "host.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationCrossStoreProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_SQLITE_CROSS_RUNTIME="+path, "RFF_CROSS_HOST="+hostPath, "RFF_CROSS_CRASH="+boundary)
			if err := cmd.Run(); err == nil {
				t.Fatal("expected process loss")
			} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child failure: %v", err)
			}
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			host, err := continuationcontract.OpenCrossStoreHost(hostPath, "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
			continuationcontract.CheckCrossStoreRestart(t, store, host)
		})
	}
}
