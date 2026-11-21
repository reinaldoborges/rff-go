package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type mutatingCodec struct{ sqlite.StringCodec }

func (mutatingCodec) Decode(data []byte) (string, error) {
	value := string(data)
	for i := range data {
		data[i] = 0
	}
	return value, nil
}

func TestIdentifierDecodingCannotMutatePersistedKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identifiers.db")
	store, err := sqlite.Open[string, string](path, mutatingCodec{}, mutatingCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	record, created, err := store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "id", Entity: "entity", Key: "key",
			Target: "open", Status: transition.Waiting,
		}, nil
	})
	if err != nil || !created || record.Entity != "entity" || record.Target != "open" {
		t.Fatalf("identifier encoding changed: record=%+v created=%v error=%v", record, created, err)
	}
	record, created, err = store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing == nil || !view.Active {
			t.Fatal("execution or reservation missing")
		}
		return nil, nil
	})
	if err != nil || created || record.Entity != "entity" || record.Target != "open" {
		t.Fatalf("replay changed identifiers: record=%+v created=%v error=%v", record, created, err)
	}
}
