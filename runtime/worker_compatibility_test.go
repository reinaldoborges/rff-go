package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestWorkerDoesNotStartAttemptWhenPinnedBehaviorIsUnavailable(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		t.Fatal("incompatible hook ran")
		return nil
	}})
	record.MachineVersion = "missing"
	record.Policy.AttemptTimeout = time.Minute
	record.Deadline = time.Now().Add(time.Hour)
	store := &oneActivityStore{record: record, call: call}
	worker, err := transition.NewHookWorker[string, string, string, int](store, runner, transition.WorkerConfig{
		ID: "worker", Capacity: 1, LeaseTTL: time.Minute, RenewalInterval: time.Second,
		Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(context.Background()); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("missing machine should be explicit: %v", err)
	}
	if store.started || store.reported {
		t.Fatal("incompatible execution consumed an attempt or reported an outcome")
	}
}
