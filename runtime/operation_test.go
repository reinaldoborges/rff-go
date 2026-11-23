package runtime_test

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTrackedOperationRequiresAttemptCapability(t *testing.T) {
	dispatched := false
	_, err := transition.TrackOperation(context.Background(), transition.OperationRequest{
		Key: "charge", Kind: "charge", BehaviorVersion: "1",
	}, func(context.Context, transition.OperationIdentity) (transition.OperationResult, error) {
		dispatched = true
		return transition.OperationResult{}, nil
	})
	if !errors.Is(err, transition.ErrOperationTrackingUnavailable) {
		t.Fatalf("expected unavailable tracking, got %v", err)
	}
	if dispatched {
		t.Fatal("downstream operation ran without tracking")
	}
}
