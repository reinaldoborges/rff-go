package mysql_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestExistingWorkWriteEntryPointsRejectInvalidIDs(t *testing.T) {
	store := openMySQL(t)
	id := strings.Repeat("x", 257)
	clock := time.Now
	_, err := store.CompleteInput(context.Background(), id, transition.InputOutcome{Receipt: "receipt"})
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
