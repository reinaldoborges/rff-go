package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestDirectAcceptanceRejectsInvalidExecutionID(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "execution.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, id := range []string{"", string([]byte{0xff}), strings.Repeat("x", 257), strings.Repeat("é", 129)} {
		_, _, err := store.Accept(context.Background(), "entity", "key", func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return &transition.Execution[string, string]{
				SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: "entity", Key: "key", Status: transition.Waiting,
			}, nil
		})
		if !errors.Is(err, transition.ErrInvalidDurableID) {
			t.Fatalf("execution ID %q: err=%v", id, err)
		}
	}
}
