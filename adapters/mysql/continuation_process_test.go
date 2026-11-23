package mysql_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestContinuationMarkerProcessRestart(t *testing.T) {
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", OperationID: "commit", Entity: "entity", Target: "due",
		MachineID: "machine", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
	}

	dsn := os.Getenv("RFF_MYSQL_CONTINUATION_RESTART")
	if dsn != "" {
		store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
			t.Fatal(err)
		}
		os.Exit(87)
	}
	_, config := newMySQLDatabase(t)
	dsn = config.FormatDSN()
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationMarkerProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_MYSQL_CONTINUATION_RESTART="+dsn)
	if err := cmd.Run(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v", err)
	}
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
		t.Fatalf("replay after process loss: %v", err)
	}
	markers, err := store.ContinuationMarkers(context.Background())
	if err != nil || len(markers) != 1 {
		t.Fatalf("markers after restart: %+v, %v", markers, err)
	}
	if err := transition.MatchContinuationMarkers(marker, markers[0]); err != nil {
		t.Fatal(err)
	}
}

func TestContinuationAtomicProcessRestart(t *testing.T) {
	db, config := newMySQLDatabase(t)
	dsn := config.FormatDSN()
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationAtomicContract/initial_notification$")
	cmd.Env = append(os.Environ(), "RFF_MYSQL_ATOMIC_RESTART="+dsn, "RFF_CONTINUATION_ATOMIC_CRASH=1")
	if err := cmd.Run(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v", err)
	}
	store, err := mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.CheckAtomicAfterRestart(t, store, db)
}
