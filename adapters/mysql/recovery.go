package mysql

import (
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func prepareRecovery(executionID string, work *hookWork, hasWork bool, now time.Time, failure fsm.Failure) {
	if hasWork {
		work.History = append(work.History, hookDispatchHistory{
			ActivityID: work.Progress.ActivityID, Dispatch: work.Progress.Dispatch,
		})
	} else {
		work.Version = 1
	}
	dispatch := transition.HookDispatch{Kind: transition.HookRecover, DueAt: now.UTC(), Failure: &failure}
	work.Progress.ActivityID = hookID(executionID, dispatch)
	work.Progress.Dispatch = dispatch
	work.Progress.AvailableAt = dispatch.DueAt
	work.Progress.Completed = false
	work.Progress.Result = transition.HookResult{}
	work.Started = false
	work.Owner = ""
	work.ExpiresAt = time.Time{}
	work.BudgetUsed = 0
}
