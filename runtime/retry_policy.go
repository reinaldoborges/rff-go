package runtime

import (
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

func validatePolicy(policy fsm.Policy) error {
	if policy.MaxAttempts == 0 || policy.AttemptTimeout <= 0 || policy.OverallTimeout <= 0 {
		return fmt.Errorf("%w: invalid execution policy", ErrInvalidRequest)
	}
	for _, delay := range policy.RetryDelays {
		if delay < 0 {
			return fmt.Errorf("%w: negative retry delay", ErrInvalidRequest)
		}
	}
	return nil
}

// DecideRetry applies the pinned schedule and shared attempt budget to one
// classified failure. retries counts automatic retries already authorized in
// the current control cycle. Deadline handling remains the caller's duty.
func DecideRetry(policy fsm.Policy, startedAttempts, retries uint, disposition fsm.Disposition) (bool, time.Duration, error) {
	if err := validatePolicy(policy); err != nil {
		return false, 0, err
	}
	if disposition != fsm.DispositionRetry && disposition != fsm.DispositionIntervention {
		return false, 0, fsm.ErrInvalidClassification
	}
	if disposition == fsm.DispositionIntervention || startedAttempts >= policy.MaxAttempts || len(policy.RetryDelays) == 0 {
		return false, 0, nil
	}
	index := len(policy.RetryDelays) - 1
	if retries < uint(len(policy.RetryDelays)) {
		index = int(retries)
	}
	return true, policy.RetryDelays[index], nil
}
