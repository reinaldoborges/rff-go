package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestConcurrentAcceptanceAcrossStoreInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	first, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := errors.Join(first.Close(), second.Close()); err != nil {
			t.Error(err)
		}
	})
	stores := []*sqlite.Store[string, string]{first, second}
	const count = 20
	start := make(chan struct{})
	failures := make(chan error, count)
	var wg sync.WaitGroup
	var callbacks, created atomic.Int64
	for i := 0; i < count; i++ {
		wg.Add(1)
		store := stores[i%len(stores)]
		go func() {
			defer wg.Done()
			<-start
			record, isNew, err := store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
				if view.Existing != nil {
					return nil, nil
				}
				callbacks.Add(1)
				return &transition.Execution[string, string]{
					SchemaVersion: transition.CurrentSchemaVersion,
					ID:            "id", Entity: "entity", Key: "key", Target: "open", Status: transition.Waiting,
				}, nil
			})
			if err != nil {
				failures <- err
			} else if record.ID != "id" {
				failures <- errors.New("wrong execution")
			} else if isNew {
				created.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if callbacks.Load() != 1 || created.Load() != 1 {
		t.Fatalf("new decisions=%d created=%d", callbacks.Load(), created.Load())
	}
}
