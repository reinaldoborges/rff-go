package mysql_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestContinuationDriveProcessRestart(t *testing.T) {
	dsn := os.Getenv("RFF_MYSQL_DRIVE_RESTART")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunDriveBudget(t, store)
		t.Fatal("expected process loss")
	}
	_, config := newMySQLDatabase(t)
	dsn = config.FormatDSN()
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDriveProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_MYSQL_DRIVE_RESTART="+dsn, "RFF_CONTINUATION_DRIVE_CRASH=after-failure")
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v: %s", err, output)
	}
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckDriveRestart(t, store)
}

func TestContinuationDrivePauseProcessRestart(t *testing.T) {
	dsn := os.Getenv("RFF_MYSQL_DRIVE_PAUSE_RESTART")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunDrivePause(t, store)
		t.Fatal("expected process loss")
	}
	_, config := newMySQLDatabase(t)
	dsn = config.FormatDSN()
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDrivePauseProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_MYSQL_DRIVE_PAUSE_RESTART="+dsn, "RFF_CONTINUATION_DRIVE_CRASH=after-park")
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v: %s", err, output)
	}
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckDrivePauseRestart(t, store)
}

func TestContinuationDriveStartedProcessRestart(t *testing.T) {
	dsn := os.Getenv("RFF_MYSQL_DRIVE_STARTED_RESTART")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		continuationcontract.RunDriveStartedLeaseLoss(t, store)
		t.Fatal("expected process loss")
	}
	_, config := newMySQLDatabase(t)
	dsn = config.FormatDSN()
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDriveStartedProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_MYSQL_DRIVE_STARTED_RESTART="+dsn, "RFF_CONTINUATION_DRIVE_CRASH=after-start")
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v: %s", err, output)
	}
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckDriveStartedRestart(t, store)
}

func TestContinuationDriverAcceptanceProcessRestart(t *testing.T) {
	dsn, hostPath := os.Getenv("RFF_DRIVER_MYSQL"), os.Getenv("RFF_DRIVER_HOST")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
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
			_, config := newMySQLDatabase(t)
			dsn, hostPath := config.FormatDSN(), filepath.Join(t.TempDir(), "host.db")
			outcome := scenario
			crash := "after-dispatch"
			if outcome == "waive-absent" {
				crash = "after-waiver"
			} else if outcome == "waive-after-tombstone" {
				outcome, crash = "waive-absent", "after-tombstone"
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationDriverAcceptanceProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_DRIVER_MYSQL="+dsn, "RFF_DRIVER_HOST="+hostPath, "RFF_DRIVER_OUTCOME="+outcome, "RFF_DRIVER_CRASH="+crash)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatal("expected process loss")
			} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child failure: %v: %s", err, output)
			}
			store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
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
