// Package acceptancecontract checks acceptance-store implementations.
package acceptancecontract

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

// Fixture provides an isolated store and deterministic commit-failure injection.
type Fixture struct {
	Store           transition.AcceptanceStore[string, string]
	FailNextCommit  func()
	FailAfterCommit func()
}

// Run checks atomicity, isolation, and concurrent acceptance at the store seam.
func Run(t *testing.T, newFixture func(*testing.T) Fixture) {
	t.Helper()
	t.Run("rollback", func(t *testing.T) {
		f := newFixture(t)
		f.FailNextCommit()
		if _, _, err := f.Store.Accept(context.Background(), "entity", "key", create("first")); err == nil {
			t.Fatal("injected commit failure was not reported")
		}
		record, created, err := f.Store.Accept(context.Background(), "entity", "key", create("second"))
		if err != nil || !created || record.ID != "second" {
			t.Fatalf("failed acceptance left state behind: record=%+v created=%v error=%v", record, created, err)
		}
	})
	t.Run("unknown commit outcome", func(t *testing.T) {
		f := newFixture(t)
		f.FailAfterCommit()
		if _, _, err := f.Store.Accept(context.Background(), "entity", "key", create("id")); err == nil {
			t.Fatal("lost commit response was not reported")
		}
		record, created, err := f.Store.Accept(context.Background(), "entity", "key", replay)
		if err != nil || created || record.ID != "id" {
			t.Fatalf("committed acceptance could not be reconciled: record=%+v created=%v error=%v", record, created, err)
		}
	})
	t.Run("reservation", func(t *testing.T) {
		f := newFixture(t)
		if _, _, err := f.Store.Accept(context.Background(), "entity", "key", create("id")); err != nil {
			t.Fatal(err)
		}
		busy := errors.New("busy")
		_, _, err := f.Store.Accept(context.Background(), "entity", "other", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if view.Existing != nil || !view.Active {
				t.Fatal("active reservation was not visible to a different key")
			}
			return nil, busy
		})
		if !errors.Is(err, busy) {
			t.Fatalf("reservation decision was not returned: %v", err)
		}
		if _, created, err := f.Store.Accept(context.Background(), "entity", "key", replay); err != nil || created {
			t.Fatalf("original request cannot replay: created=%v error=%v", created, err)
		}
	})
	t.Run("invalid decisions", func(t *testing.T) {
		f := newFixture(t)
		if _, _, err := f.Store.Accept(context.Background(), "entity", "key", create("id")); err != nil {
			t.Fatal(err)
		}
		_, _, err := f.Store.Accept(context.Background(), "entity", "other", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			record := newRecord("other-id")
			record.Key = "other"
			return record, nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("active reservation allowed a new execution: %v", err)
		}
		_, _, err = f.Store.Accept(context.Background(), "entity", "key", create("replacement"))
		if err == nil {
			t.Fatal("existing execution was replaced")
		}
		_, _, err = f.Store.Accept(context.Background(), "other-entity", "other-key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			return newRecord("new-id"), nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("mismatched entity and key were accepted: %v", err)
		}
		_, _, err = f.Store.Accept(context.Background(), "other-entity", "other-key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			record := newRecord("id")
			record.Entity, record.Key = "other-entity", "other-key"
			return record, nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("duplicate execution ID was accepted: %v", err)
		}
		_, _, err = f.Store.Accept(context.Background(), "other-entity", "other-key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			record := newRecord("new-id")
			record.Entity, record.Key, record.Status = "other-entity", "other-key", transition.Complete
			return record, nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("terminal execution was accepted as new: %v", err)
		}
		_, _, err = f.Store.Accept(context.Background(), "other-entity", "other-key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			record := newRecord("new-id")
			record.Entity, record.Key, record.SchemaVersion = "other-entity", "other-key", transition.CurrentSchemaVersion+1
			return record, nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("unsupported execution schema was accepted: %v", err)
		}
		_, _, err = f.Store.Accept(context.Background(), "other-entity", "other-key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			record := newRecord("new-id")
			record.Entity, record.Key, record.CommitOperationID = "other-entity", "other-key", "fake-commit"
			return record, nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("pre-marked work was accepted: %v", err)
		}
		_, _, err = f.Store.Accept(context.Background(), "other-entity", "other-key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			record := newRecord("new-id")
			record.Entity, record.Key, record.Failure = "other-entity", "other-key", &fsm.Failure{Code: "fake"}
			return record, nil
		})
		if !errors.Is(err, transition.ErrInvalidDecision) {
			t.Fatalf("pre-failed work was accepted: %v", err)
		}
	})
	t.Run("isolation", func(t *testing.T) {
		f := newFixture(t)
		record, _, err := f.Store.Accept(context.Background(), "entity", "key", create("id"))
		if err != nil {
			t.Fatal(err)
		}
		record.Input.Data[0] = 'x'
		record.Policy.RetryDelays[0] = time.Hour
		stop := errors.New("abort callback")
		_, _, err = f.Store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			if view.Existing == nil {
				t.Fatal("missing existing execution")
			}
			view.Existing.Input.Data[0] = 'y'
			return nil, stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("callback error was not returned: %v", err)
		}
		replayed, created, err := f.Store.Accept(context.Background(), "entity", "key", replay)
		if err != nil || created || string(replayed.Input.Data) != "input" || replayed.Policy.RetryDelays[0] != time.Second {
			t.Fatalf("stored record was mutated: record=%+v created=%v error=%v", replayed, created, err)
		}
	})
	t.Run("concurrent acceptance", func(t *testing.T) {
		f := newFixture(t)
		const count = 16
		start := make(chan struct{})
		var wg sync.WaitGroup
		var creates atomic.Int64
		var createdCount atomic.Int64
		errorsFound := make(chan error, count)
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				record, created, err := f.Store.Accept(context.Background(), "entity", "key", func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
					if view.Existing != nil {
						return nil, nil
					}
					creates.Add(1)
					return newRecord("id"), nil
				})
				if err != nil {
					errorsFound <- err
				} else if record.ID != "id" {
					errorsFound <- errors.New("unexpected execution ID")
				} else if created {
					createdCount.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errorsFound)
		for err := range errorsFound {
			t.Error(err)
		}
		if creates.Load() != 1 || createdCount.Load() != 1 {
			t.Fatalf("selection ran %d times and %d executions were created", creates.Load(), createdCount.Load())
		}
	})
}

func newRecord(id string) *transition.Execution[string, string] {
	return &transition.Execution[string, string]{
		SchemaVersion: transition.CurrentSchemaVersion,
		ID:            id, Entity: "entity", Key: "key", Operation: transition.Initialize,
		MachineID: "machine", MachineVersion: "1", Target: "initial",
		Input: transition.EncodedValue{
			Codec: transition.CodecVersion{Name: "input", Version: "1"}, Data: []byte("input"),
		},
		NotificationOverride: transition.NotifyInherit,
		Policy: fsm.Policy{
			MaxAttempts: 2, AttemptTimeout: time.Second, OverallTimeout: time.Minute,
			RetryDelays: []time.Duration{time.Second},
		},
		AcceptedAt: time.Unix(100, 0), Deadline: time.Unix(160, 0), Status: transition.Waiting,
	}
}

func create(id string) transition.AcceptanceDecisionFunc[string, string] {
	return func(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if view.Existing != nil || view.Active {
			return nil, errors.New("unexpected existing execution or reservation")
		}
		return newRecord(id), nil
	}
}

func replay(view transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
	if view.Existing == nil || !view.Active {
		return nil, errors.New("execution or reservation missing")
	}
	return nil, nil
}
