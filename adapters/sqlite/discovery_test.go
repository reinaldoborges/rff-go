package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestActiveExecutionDiscoverySurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discovery.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		_, _, err := store.Accept(ctx, id, "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: "execution-" + id,
				Entity: id, Key: "key", Target: "open", Status: transition.Waiting,
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
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
	ids, err := store.ActiveExecutionIDs(ctx)
	if err != nil || !slices.Equal(ids, []string{"execution-a", "execution-b"}) {
		t.Fatalf("active executions not discovered: ids=%v error=%v", ids, err)
	}
	execution, err := store.ReadActiveExecution(ctx, "execution-b")
	if err != nil || execution.Entity != "b" || execution.Target != "open" || execution.Status != transition.Waiting {
		t.Fatalf("unexpected active execution: %+v error=%v", execution, err)
	}
	if _, err := store.ReadActiveExecution(ctx, "missing"); !errors.Is(err, transition.ErrExecutionNotActive) {
		t.Fatalf("missing execution should be explicit: %v", err)
	}
}
