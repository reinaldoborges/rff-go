package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestExistingWorkWriteEntryPointsRejectInvalidIDs(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "ids.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := strings.Repeat("x", 257)
	clock := time.Now
	_, err = store.CompleteInput(context.Background(), id, transition.InputOutcome{Receipt: "receipt"})
	if !errors.Is(err, transition.ErrInvalidDurableID) {
		t.Fatalf("complete input: %v", err)
	}
	_, err = store.AcquireTransitionLease(context.Background(), "entity", id, "owner", clock, time.Second)
	if !errors.Is(err, transition.ErrInvalidDurableID) {
		t.Fatalf("acquire execution lease: %v", err)
	}
	err = store.CompleteTimer(context.Background(), transition.TimerLease{ID: id, Owner: "owner", Generation: 1, ExpiresAt: clock().Add(time.Second)}, clock)
	if !errors.Is(err, transition.ErrInvalidDurableID) {
		t.Fatalf("complete timer: %v", err)
	}
}
