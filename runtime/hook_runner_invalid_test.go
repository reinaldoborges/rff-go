package runtime_test

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestHookRunnerRejectsInvalidAttemptBeforeRehydration(t *testing.T) {
	runner, record, call := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error {
		t.Fatal("invalid attempt ran a hook")
		return nil
	}})
	call.Attempt.Number = 0
	record.Input.Data = []byte("unreadable")
	if _, err := runner.Run(context.Background(), record, call); !errors.Is(err, transition.ErrInvalidHook) {
		t.Fatalf("invalid attempt should be rejected before decoding: %v", err)
	}
}
