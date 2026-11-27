package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestInvalidGeneratedExecutionIDDoesNotCompleteInput(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "atomic.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	_, _, err = store.AcceptInput(ctx, "input", func(*transition.InputRecord) (*transition.InputRecord, error) {
		return &transition.InputRecord{SchemaVersion: 1, ID: "input", Channel: "channel", WorkflowID: "workflow",
			Kind: transition.Command, Value: transition.EncodedValue{Codec: transition.CodecVersion{Name: "input", Version: "1"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, outcome, err := store.AcceptInputAtomic(ctx, "entity", "input",
		func(context.Context, *sql.Tx, transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return nil, transition.ValidateDurableID("")
		}, func(error) *fsm.Failure { return &fsm.Failure{} })
	if !errors.Is(err, transition.ErrInvalidDurableID) || outcome != nil {
		t.Fatalf("invalid ID classified as durable rejection: outcome=%+v err=%v", outcome, err)
	}
	input, err := store.ReadInput(ctx, "input")
	if err != nil || input.Outcome != nil {
		t.Fatalf("input changed after invalid ID: %+v err=%v", input, err)
	}
}
