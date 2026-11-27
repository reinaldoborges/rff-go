package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestContinuationMarkersSurviveReopenAndRejectConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "continuations.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "origin", OperationID: "commit", Entity: "entity",
		Target: "due", MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
	}
	marker = continuationcontract.PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
		t.Fatalf("identical replay: %v", err)
	}
	changed := marker
	changed.OperationID = "other-commit"
	if err := store.RecordContinuationMarker(context.Background(), changed); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("conflicting replay: %v", err)
	}
	markers, err := store.ContinuationMarkers(context.Background())
	if err != nil || len(markers) != 1 {
		t.Fatalf("list retained markers: %+v, %v", markers, err)
	}
	if err := transition.MatchContinuationMarkers(marker, markers[0]); err != nil {
		t.Fatal(err)
	}
}

func TestContinuationMarkersPreserveBinaryIdentities(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binary.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "é\x00a", OperationID: string([]byte{0xfe}),
		Entity: "entity", Target: "state", MachineID: string([]byte{0xfd}), MachineVersion: string([]byte{0xfc}),
		Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
	}
	marker = continuationcontract.PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
		t.Fatal(err)
	}
	markers, err := store.ContinuationMarkers(context.Background())
	if err != nil || len(markers) != 1 {
		t.Fatalf("list binary identity: %+v, %v", markers, err)
	}
	if err := transition.MatchContinuationMarkers(marker, markers[0]); err != nil {
		t.Fatal(err)
	}
}
