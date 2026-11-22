package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type contextIntCodec struct{ intCodec }

func (contextIntCodec) Decode(ctx context.Context, data []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return intCodec{}.Decode(ctx, data)
}

func TestHookRunnerDistinguishesCodecCancellationFromIncompatibility(t *testing.T) {
	calls := 0
	runner, record, call := newHookRunnerWithCodec(t, errorHandler{
		enter: func(context.Context) error { calls++; return nil },
	}, func(transition.CodecVersion) (transition.Codec[int], error) { return contextIntCodec{}, nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runner.Run(ctx, record, call); !errors.Is(err, context.Canceled) || errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("cancellation was reported as incompatibility: %v", err)
	}

	ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	result, err := runner.Run(ctx, record, call)
	if err != nil || result.Success || result.Failure.Code != "safe" {
		t.Fatalf("attempt timeout was not classified: result=%+v error=%v", result, err)
	}
	if calls != 0 {
		t.Fatal("hook ran after its context was done")
	}
}

func TestHookRunnerCodecResolutionCancellationIsNotIncompatibility(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner, record, call := newHookRunnerWithCodec(t, errorHandler{
		enter: func(context.Context) error {
			t.Fatal("canceled hook ran")
			return nil
		},
	}, func(transition.CodecVersion) (transition.Codec[int], error) {
		cancel()
		return nil, context.Canceled
	})
	if _, err := runner.Run(ctx, record, call); !errors.Is(err, context.Canceled) || errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("canceled resolution was reported as incompatibility: %v", err)
	}
}
