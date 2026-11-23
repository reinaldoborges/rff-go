package sqlite_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestContinuationDriveProcessRestart(t *testing.T) {
	path := os.Getenv("RFF_SQLITE_DRIVE_RESTART")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunDriveBudget(t, store)
		t.Fatal("expected process loss")
	}
	path = filepath.Join(t.TempDir(), "drive-restart.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDriveProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_SQLITE_DRIVE_RESTART="+path, "RFF_CONTINUATION_DRIVE_CRASH=after-failure")
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v: %s", err, output)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckDriveRestart(t, store)
}

func TestContinuationDrivePauseProcessRestart(t *testing.T) {
	path := os.Getenv("RFF_SQLITE_DRIVE_PAUSE_RESTART")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunDrivePause(t, store)
		t.Fatal("expected process loss")
	}
	path = filepath.Join(t.TempDir(), "drive-pause-restart.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDrivePauseProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_SQLITE_DRIVE_PAUSE_RESTART="+path, "RFF_CONTINUATION_DRIVE_CRASH=after-park")
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v: %s", err, output)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckDrivePauseRestart(t, store)
}

func TestContinuationDriveStartedProcessRestart(t *testing.T) {
	path := os.Getenv("RFF_SQLITE_DRIVE_STARTED_RESTART")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunDriveStartedLeaseLoss(t, store)
		t.Fatal("expected process loss")
	}
	path = filepath.Join(t.TempDir(), "drive-started-restart.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDriveStartedProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_SQLITE_DRIVE_STARTED_RESTART="+path, "RFF_CONTINUATION_DRIVE_CRASH=after-start")
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v: %s", err, output)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckDriveStartedRestart(t, store)
}

func TestContinuationDriverAcceptanceProcessRestart(t *testing.T) {
	path, hostPath := os.Getenv("RFF_DRIVER_SQLITE"), os.Getenv("RFF_DRIVER_HOST")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		host, err := successorcontract.OpenHost(hostPath)
		if err != nil {
			t.Fatal(err)
		}
		successorcontract.RunDriverOneAttempt(t, store, host, os.Getenv("RFF_DRIVER_OUTCOME"))
		t.Fatal("expected process loss")
	}
	for _, scenario := range []string{"accepted-lost", "absent", "waive-absent", "waive-after-tombstone"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			path, hostPath := filepath.Join(dir, "runtime.db"), filepath.Join(dir, "host.db")
			outcome := scenario
			crash := "after-dispatch"
			if outcome == "waive-absent" {
				crash = "after-waiver"
			} else if outcome == "waive-after-tombstone" {
				outcome, crash = "waive-absent", "after-tombstone"
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDriverAcceptanceProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_DRIVER_SQLITE="+path, "RFF_DRIVER_HOST="+hostPath, "RFF_DRIVER_OUTCOME="+outcome, "RFF_DRIVER_CRASH="+crash)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatal("expected process loss")
			} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child failure: %v: %s", err, output)
			}
			store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			host, err := successorcontract.OpenHost(hostPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
			successorcontract.CheckDriverRestart(t, store, host, outcome)
		})
	}
}
