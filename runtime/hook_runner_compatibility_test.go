package runtime_test

import (
	"context"
	"errors"
	"strings"
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

func TestHookRunnerReportsSafePinnedCompatibilityDiagnostic(t *testing.T) {
	secret := errors.New("sensitive decoder details")
	runner, record, call := newHookRunnerWithCodec(t, errorHandler{enter: func(context.Context) error {
		t.Fatal("incompatible hook ran")
		return nil
	}}, func(transition.CodecVersion) (transition.Codec[int], error) {
		return nil, secret
	})
	record.Input.Codec = transition.CodecVersion{Name: "input", Version: "v2"}
	_, err := runner.Run(context.Background(), record, call)
	var compatibility *transition.CompatibilityError
	if !errors.As(err, &compatibility) || !errors.Is(err, secret) ||
		compatibility.Diagnostic.Component != transition.CompatibilityInputCodec ||
		compatibility.Diagnostic.Name != "input" || compatibility.Diagnostic.Version != "v2" {
		t.Fatalf("missing pinned compatibility diagnostic: %v", err)
	}
	if strings.Contains(err.Error(), secret.Error()) {
		t.Fatalf("diagnostic exposed codec error: %v", err)
	}
}
