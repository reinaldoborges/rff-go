package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type emptyCodec struct{}

func (emptyCodec) Encode(string) ([]byte, error) { return nil, nil }
func (emptyCodec) Decode([]byte) (string, error) { return "", nil }

func TestNilEncodingCanRepresentAnEmptyIdentifier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty-encoding.db")
	store, err := sqlite.Open[string, string](path, emptyCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	_, created, err := store.Accept(context.Background(), "", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "id", Entity: "", Key: "key", Target: "open", Status: transition.Waiting,
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("nil empty identifier encoding was rejected: created=%v error=%v", created, err)
	}
}
