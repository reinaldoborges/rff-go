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

func TestExistingExecutionWritesRejectInvalidID(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "execution.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := strings.Repeat("x", 257)
	ctx := context.Background()
	if err := store.BlockUnreadableExecution(ctx, id); !errors.Is(err, transition.ErrInvalidDurableID) {
		t.Fatalf("block execution: %v", err)
	}
	_, err = store.RecordOperationResolution(ctx, transition.OperationIdentity{ExecutionID: id, Key: "key"}, "request", "principal", "reason", transition.OperationAssessment{}, time.Now)
	if !errors.Is(err, transition.ErrInvalidDurableID) {
		t.Fatalf("resolve operation: %v", err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	err = outbox.Handoff(ctx, transition.TransitionLease[string]{ExecutionID: id}, transition.NotificationIntent[string, string]{ID: "intent", ExecutionID: id})
	if !errors.Is(err, transition.ErrInvalidDurableID) {
		t.Fatalf("notification handoff: %v", err)
	}
}
