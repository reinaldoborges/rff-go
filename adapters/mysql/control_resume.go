package mysql

import (
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func resumeFailedHook(executionID string, work *hookWork, now time.Time) error {
	last := work.Progress.Attempts[len(work.Progress.Attempts)-1]
	if last.Result == nil {
		return transition.ErrInvalidHook
	}

	if last.Result.Disposition == fsm.DispositionIntervention &&
		(last.Unknown || last.Kind == transition.HookRetry || last.Kind == transition.HookRecover) {
		work.AuthorizedRetries = append(work.AuthorizedRetries, hookAttemptRef{
			ActivityID: last.Metadata.ActivityID, Generation: last.Metadata.LeaseGeneration,
		})
	}
	switch work.Progress.Dispatch.Kind {
	case transition.HookRetry, transition.HookRecover:
		work.Redeliveries = append(work.Redeliveries, hookRedelivery{
			ActivityID: last.Metadata.ActivityID, Generation: last.Metadata.LeaseGeneration, AvailableAt: now.UTC(),
		})
		work.Progress.AvailableAt = now.UTC()
		work.Progress.Completed = false
		work.Progress.Result = transition.HookResult{}
		work.Started = false
		return nil
	case transition.HookEnter, transition.HookTransition:
	default:
		return transition.ErrInvalidHook
	}
	var cycle uint
	for _, attempt := range work.Progress.Attempts {
		if attempt.Kind == transition.HookRetry && attempt.Metadata.RetryCycle > cycle {
			cycle = attempt.Metadata.RetryCycle
		}
	}
	if cycle == ^uint(0) {
		return transition.ErrInvalidHook
	}
	failure := work.Progress.Result.Failure
	dispatch := transition.HookDispatch{Kind: transition.HookRetry, RetryCycle: cycle + 1, DueAt: now.UTC(), Failure: &failure}
	work.History = append(work.History, hookDispatchHistory{
		ActivityID: work.Progress.ActivityID, Dispatch: work.Progress.Dispatch,
	})
	work.Progress.ActivityID = hookID(executionID, dispatch)
	work.Progress.Dispatch = dispatch
	work.Progress.AvailableAt = dispatch.DueAt
	work.Progress.Completed = false
	work.Progress.Result = transition.HookResult{}
	work.Started = false
	return nil
}
