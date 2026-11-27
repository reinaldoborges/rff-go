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

func TestDirectWritesRejectInvalidDurableIDs(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "ids.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	defer store.Close()
	for _, id := range []string{"", string([]byte{0xff}), strings.Repeat("x", 257), strings.Repeat("é", 129)} {
		called := false
		_, _, err := store.ScheduleTimer(context.Background(), id, func(*transition.TimerRecord) (*transition.TimerRecord, error) {
			called = true
			return nil, nil
		})
		if !errors.Is(err, transition.ErrInvalidDurableID) || called {
			t.Fatalf("timer ID %q: err=%v called=%v", id, err, called)
		}
		_, _, err = store.AcceptInput(context.Background(), id, func(*transition.InputRecord) (*transition.InputRecord, error) {
			called = true
			return nil, nil
		})
		if !errors.Is(err, transition.ErrInvalidDurableID) || called {
			t.Fatalf("input ID %q: err=%v called=%v", id, err, called)
		}
	}
}
