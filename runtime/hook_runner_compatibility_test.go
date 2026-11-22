package runtime_test

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestHookRunnerBlocksUnavailablePinnedBehaviorBeforeWork(t *testing.T) {
	calls := 0
	runner, record, call := newFailingHookRunner(t, errorHandler{
		enter: func(context.Context) error { calls++; return nil },
	})
	record.MachineVersion = "missing"
	if _, err := runner.Run(context.Background(), record, call); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("missing machine version should block execution: %v", err)
	}
	record.MachineVersion = "1"
	record.Input.Data = []byte("not-an-integer")
	if _, err := runner.Run(context.Background(), record, call); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unreadable input should block execution: %v", err)
	}
	if calls != 0 {
		t.Fatal("incompatible behavior ran a hook")
	}
}
