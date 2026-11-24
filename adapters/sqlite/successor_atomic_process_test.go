package sqlite_test

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestSuccessorSharedProcessRestart(t *testing.T) {
	path, hostPath := os.Getenv("RFF_SUCCESSOR_SHARED_SQLITE"), os.Getenv("RFF_SUCCESSOR_SHARED_HOST")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite3", path)
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
			dir := t.TempDir()
			path, hostPath := filepath.Join(dir, "shared.db"), filepath.Join(dir, "origin-host.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuccessorSharedProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_SUCCESSOR_SHARED_SQLITE="+path, "RFF_SUCCESSOR_SHARED_HOST="+hostPath, "RFF_SUCCESSOR_CRASH="+boundary)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child exit: %v: %s", err, output)
			}
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
			successorcontract.CheckAtomicRestart(t, store, db, boundary)
		})
	}
}
