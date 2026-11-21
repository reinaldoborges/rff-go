package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type selectiveStateCodec struct{ sqlite.StringCodec }

func (selectiveStateCodec) Decode(data []byte) (string, error) {
	if string(data) == "incompatible" {
		return "", errors.New("state is unavailable")
	}
	return string(data), nil
}

func TestIncompatibleStateBlocksOnlyAffectedExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compatibility.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"incompatible", "compatible"} {
		_, _, err := store.Accept(context.Background(), target, "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: target, Entity: target,
				Key: "key", Target: target, Status: transition.Waiting,
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, selectiveStateCodec{})
	if err != nil {
		t.Fatalf("one incompatible state must not prevent opening the store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	replay := func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing == nil || !view.Active {
			return nil, errors.New("missing execution or reservation")
		}
		return nil, nil
	}
	if _, _, err := store.Accept(context.Background(), "incompatible", "key", replay); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("expected compatibility error, got %v", err)
	}
	record, created, err := store.Accept(context.Background(), "compatible", "key", replay)
	if err != nil || created || record.Target != "compatible" {
		t.Fatalf("compatible execution could not be replayed: record=%+v created=%v error=%v", record, created, err)
	}
}
