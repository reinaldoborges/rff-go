package mysql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestContinuationMarkersSurviveReopenAndRejectConflicts(t *testing.T) {
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	marker := transition.ContinuationMarker[string, string]{
		SchemaVersion: 1, ExecutionID: "é\x00a", OperationID: string([]byte{0xfe}),
		Entity: "entity", Target: "due", MachineID: "plan", MachineVersion: "1",
		Policy: fsm.Policy{MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
	}
	marker = continuationcontract.PrepareOrigin(t, store, marker)
	if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.RecordContinuationMarker(context.Background(), marker); err != nil {
		t.Fatal(err)
	}
	changed := marker
	changed.OperationID = "different"
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
