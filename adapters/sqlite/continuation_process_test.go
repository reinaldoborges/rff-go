package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
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

	path := os.Getenv("RFF_SQLITE_CONTINUATION_RESTART")
	if path != "" {
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		marker = continuationcontract.PrepareOrigin(t, store, marker)
		if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
			t.Fatal(err)
		}
		os.Exit(87)
	}
	path = filepath.Join(t.TempDir(), "process.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationMarkerProcessRestart$")
	cmd.Env = append(os.Environ(), "RFF_SQLITE_CONTINUATION_RESTART="+path)
	if err := cmd.Run(); err == nil {
		t.Fatal("expected process loss")
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
		t.Fatalf("unexpected child failure: %v", err)
	}
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	execution, err := store.ReadActiveExecution(context.Background(), marker.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	marker.OperationID = execution.CommitOperationID
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
	for _, boundary := range []string{"before-commit", "after-commit"} {
		t.Run(boundary, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "atomic.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestContinuationAtomicContract/initial_notification$")
			cmd.Env = append(os.Environ(), "RFF_SQLITE_ATOMIC_RESTART="+path, "RFF_CONTINUATION_ATOMIC_CRASH="+boundary)
			if err := cmd.Run(); err == nil {
				t.Fatal("expected process loss")
			} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("unexpected child failure: %v", err)
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
			if boundary == "before-commit" {
				continuationcontract.CheckAtomicRollbackAfterRestart(t, store, db)
			} else {
				continuationcontract.CheckAtomicAfterRestart(t, store, db)
			}
		})
	}
}
