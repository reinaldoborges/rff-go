package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestRetryDecisionUsesSharedBudgetAndRepeatsLastDelay(t *testing.T) {
	policy := fsm.Policy{
		MaxAttempts: 4, AttemptTimeout: time.Second, OverallTimeout: time.Minute,
		RetryDelays: []time.Duration{time.Second, 5 * time.Second},
	}
	for _, tt := range []struct {
		name        string
		attempts    uint
		retries     uint
		disposition fsm.Disposition
		retry       bool
		delay       time.Duration
	}{
		{"first retry", 1, 0, fsm.DispositionRetry, true, time.Second},
		{"second retry", 2, 1, fsm.DispositionRetry, true, 5 * time.Second},
		{"repeat final delay", 3, 2, fsm.DispositionRetry, true, 5 * time.Second},
		{"shared attempts exhausted", 4, 3, fsm.DispositionRetry, false, 0},
		{"classifier requires intervention", 1, 0, fsm.DispositionIntervention, false, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			retry, delay, err := transition.DecideRetry(policy, tt.attempts, tt.retries, tt.disposition)
			if err != nil || retry != tt.retry || delay != tt.delay {
				t.Fatalf("retry=%v delay=%s error=%v", retry, delay, err)
			}
		})
	}
	policy.RetryDelays = nil
	if retry, _, err := transition.DecideRetry(policy, 1, 0, fsm.DispositionRetry); err != nil || retry {
		t.Fatalf("empty schedule must require intervention: retry=%v error=%v", retry, err)
	}
	if _, _, err := transition.DecideRetry(policy, 1, 0, "unknown"); !errors.Is(err, fsm.ErrInvalidClassification) {
		t.Fatalf("unknown disposition must be rejected: %v", err)
	}
	policy.RetryDelays = []time.Duration{-time.Second}
	if _, _, err := transition.DecideRetry(policy, 1, 0, fsm.DispositionRetry); !errors.Is(err, transition.ErrInvalidRequest) {
		t.Fatalf("negative delay must be rejected: %v", err)
	}
}
