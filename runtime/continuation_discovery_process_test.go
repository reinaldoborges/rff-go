package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type restartMarkerStore struct{ path string }

func (s restartMarkerStore) RecordContinuationMarker(_ context.Context, marker transition.ContinuationMarker[string, string]) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	prior, err := os.ReadFile(s.path)
	if err == nil {
		if string(prior) != string(data) {
			return transition.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.WriteFile(s.path, data, 0600)
}

type restartMarkerSource struct {
	ackPath string
	crash   string
}

func (s restartMarkerSource) ListContinuationMarkers(_ context.Context, _ string) ([]transition.ContinuationMarker[string, string], string, error) {
	if _, err := os.Stat(s.ackPath); err == nil {
		return nil, "", nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, "", err
	}
	return []transition.ContinuationMarker[string, string]{continuationMarker("origin")}, "", nil
}

func (s restartMarkerSource) AcknowledgeContinuationMarker(_ context.Context, _ transition.ContinuationMarker[string, string]) error {
	if s.crash == "before" {
		os.Exit(86)
	}
	if err := os.WriteFile(s.ackPath, []byte("acknowledged"), 0600); err != nil {
		return err
	}
	if s.crash == "after" {
		os.Exit(86)
	}
	return nil
}

func TestContinuationDiscoveryProcessRestart(t *testing.T) {
	dir := os.Getenv("RFF_DISCOVERY_RESTART_DIR")
	if dir != "" {
		store := restartMarkerStore{path: filepath.Join(dir, "record")}
		source := restartMarkerSource{ackPath: filepath.Join(dir, "ack"), crash: os.Getenv("RFF_DISCOVERY_CRASH")}
		discovery, err := transition.NewContinuationDiscovery[string, string](store, source)
		if err != nil {
			t.Fatal(err)
		}
		if err := discovery.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, boundary := range []string{"before", "after"} {
		t.Run(boundary+"-acknowledgement", func(t *testing.T) {
			dir := t.TempDir()
			run := func(crash string) error {
				command := exec.Command(os.Args[0], "-test.run=^TestContinuationDiscoveryProcessRestart$")
				command.Env = append(os.Environ(), "RFF_DISCOVERY_RESTART_DIR="+dir, "RFF_DISCOVERY_CRASH="+crash)
				return command.Run()
			}
			if err := run(boundary); err == nil {
				t.Fatal("expected process loss after marker persistence")
			} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("unexpected child failure: %v", err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "record"))
			if err != nil {
				t.Fatalf("record missing after process loss: %v", err)
			}
			var recorded transition.ContinuationMarker[string, string]
			if err := json.Unmarshal(data, &recorded); err != nil {
				t.Fatal(err)
			}
			if err := transition.MatchContinuationMarkers(recorded, continuationMarker("origin")); err != nil {
				t.Fatalf("unexpected persisted marker: %v", err)
			}
			if err := run(""); err != nil {
				t.Fatalf("reconcile after restart: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "ack")); err != nil {
				t.Fatalf("marker was not acknowledged after replay: %v", err)
			}
		})
	}
}
