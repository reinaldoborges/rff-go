package sqlite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type entityID struct {
	Tenant string
	Number int
}

type entityCodec struct{}

func (entityCodec) Encode(value entityID) ([]byte, error) { return json.Marshal(value) }
func (entityCodec) Decode(data []byte) (entityID, error) {
	var value entityID
	err := json.Unmarshal(data, &value)
	return value, err
}

type stateID int
type stateCodec struct{}

func (stateCodec) Encode(value stateID) ([]byte, error) { return []byte(strconv.Itoa(int(value))), nil }
func (stateCodec) Decode(data []byte) (stateID, error) {
	value, err := strconv.Atoi(string(data))
	return stateID(value), err
}

func TestApplicationIdentifierCodecsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "typed.db")
	store, err := sqlite.Open[entityID, stateID](path, entityCodec{}, stateCodec{})
	if err != nil {
		t.Fatal(err)
	}
	entity := entityID{Tenant: "tenant", Number: 5}
	_, _, err = store.Accept(context.Background(), entity, "key", func(view transition.AcceptanceView[entityID, stateID]) (*transition.Execution[entityID, stateID], error) {
		return &transition.Execution[entityID, stateID]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: "id", Entity: entity, Key: "key",
			Previous: 0, HasPrevious: true, Target: 42, Status: transition.Waiting,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[entityID, stateID](path, entityCodec{}, stateCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	record, _, err := store.Accept(context.Background(), entity, "key", func(view transition.AcceptanceView[entityID, stateID]) (*transition.Execution[entityID, stateID], error) {
		if view.Existing == nil || !view.Active {
			return nil, fmt.Errorf("missing persisted execution")
		}
		return nil, nil
	})
	if err != nil || record.Entity != entity || !record.HasPrevious || record.Previous != 0 || record.Target != 42 {
		t.Fatalf("identifier round trip failed: record=%+v error=%v", record, err)
	}
}

func TestEmptyComparableEntityIdentifier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
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
			SchemaVersion: transition.CurrentSchemaVersion, ID: "id", Entity: "", Key: "key", Target: "", Status: transition.Waiting,
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("empty entity ID should be valid: created=%v error=%v", created, err)
	}
}
