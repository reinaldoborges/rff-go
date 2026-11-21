package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestAcceptanceSurvivesStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transitions.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	original, created, err := store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing != nil || view.Active {
			t.Fatal("new entity unexpectedly has an execution")
		}
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion,
			ID:            "execution-1", Entity: "entity", Key: "key", Operation: transition.Initialize,
			MachineID: "task", MachineVersion: "1", Target: "open",
			Input:                transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("input")},
			NotificationOverride: transition.NotifyInherit, Notify: true,
			Policy:     fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Second, OverallTimeout: time.Minute},
			AcceptedAt: time.Unix(100, 0), Deadline: time.Unix(160, 0), Status: transition.Waiting,
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("accept: created=%v error=%v", created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	replayed, created, err := store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing == nil || !view.Active {
			t.Fatal("accepted execution or reservation was lost")
		}
		return nil, nil
	})
	if err != nil || created || replayed.ID != original.ID || replayed.Target != "open" ||
		string(replayed.Input.Data) != "input" || !replayed.Deadline.Equal(time.Unix(160, 0)) {
		t.Fatalf("reopened acceptance differs: record=%+v created=%v error=%v", replayed, created, err)
	}
}
