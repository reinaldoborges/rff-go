package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	"github.com/reinaldoborges/rff-go/internal/executionidentity"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type hookWork struct {
	Version           int
	Progress          transition.HookProgress
	History           []hookDispatchHistory
	Redeliveries      []hookRedelivery
	AuthorizedRetries []hookAttemptRef
	Owner             string
	Generation        int64
	ExpiresAt         time.Time
	Started           bool
	FencedUnknown     bool
	BudgetUsed        uint
}

type hookWorkAlias hookWork

func (work hookWork) MarshalJSON() ([]byte, error) {
	ids := make([][]byte, len(work.Progress.Attempts))
	for i, attempt := range work.Progress.Attempts {
		ids[i] = []byte(attempt.Metadata.ExecutionID)
	}
	return json.Marshal(struct {
		hookWorkAlias
		AttemptExecutionIDs [][]byte
	}{hookWorkAlias: hookWorkAlias(work), AttemptExecutionIDs: ids})
}

func (work *hookWork) UnmarshalJSON(data []byte) error {
	var envelope struct {
		hookWorkAlias
		AttemptExecutionIDs [][]byte
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	*work = hookWork(envelope.hookWorkAlias)
	if envelope.AttemptExecutionIDs == nil {
		return nil
	}
	if len(envelope.AttemptExecutionIDs) != len(work.Progress.Attempts) {
		return errors.New("inconsistent hook attempt identities")
	}
	for i, raw := range envelope.AttemptExecutionIDs {
		attempt := &work.Progress.Attempts[i]
		if !executionidentity.Matches(attempt.Metadata.ExecutionID, raw) {
			return errors.New("inconsistent hook attempt identity")
		}
		attempt.Metadata.ExecutionID = string(raw)
	}
	return nil
}

type hookDispatchHistory struct {
	ActivityID string
	Dispatch   transition.HookDispatch
}

type hookRedelivery struct {
	ActivityID  string
	Generation  uint64
	AvailableAt time.Time
}

type hookAttemptRef struct {
	ActivityID string
	Generation uint64
}

func fenceHook(work *hookWork, now time.Time) {
	if work.Started {
		if !work.Progress.Completed {
			work.Progress.Attempts[len(work.Progress.Attempts)-1].Unknown = true
			work.FencedUnknown = true
		}
		work.ExpiresAt = now.UTC()
		return
	}
	work.Owner, work.ExpiresAt = "", time.Time{}
}

func hookAttemptKey(id string, generation uint64) string {
	return id + ":" + strconv.FormatUint(generation, 10)
}

func hookID(executionID string, dispatch transition.HookDispatch) string {
	data := strconv.AppendInt(nil, int64(len(executionID)), 10)
	data = append(data, ':')
	data = append(data, executionID...)
	data = append(data, ':')
	data = append(data, dispatch.Kind...)
	data = append(data, ':')
	data = strconv.AppendUint(data, uint64(dispatch.RetryCycle), 10)
	sum := sha256.Sum256(data)
	return "rff-hook:" + hex.EncodeToString(sum[:])
}

func validDispatch(dispatch transition.HookDispatch) bool {
	if dispatch.DueAt.IsZero() {
		return false
	}
	switch dispatch.Kind {
	case transition.HookEnter, transition.HookTransition:
		return dispatch.Failure == nil
	case transition.HookRetry, transition.HookRecover:
		return dispatch.Failure != nil
	default:
		return false
	}
}

func validateWork(executionID string, work hookWork) error {
	progress := work.Progress
	if work.Version != 1 || executionID == "" || !validDispatch(progress.Dispatch) || progress.AvailableAt.IsZero() ||
		progress.ActivityID != hookID(executionID, progress.Dispatch) || work.Generation < 0 ||
		work.BudgetUsed > uint(len(progress.Attempts)) {
		return fmt.Errorf("%w: invalid hook work", ErrUnsupportedSchema)
	}
	if (work.Owner == "") != work.ExpiresAt.IsZero() || (work.Owner != "" && work.Generation == 0) || (work.Started && work.Owner == "") {
		return fmt.Errorf("%w: invalid hook lease", ErrUnsupportedSchema)
	}
	seen := map[string]bool{progress.ActivityID: true}
	for _, prior := range work.History {
		if !validDispatch(prior.Dispatch) || prior.ActivityID != hookID(executionID, prior.Dispatch) || seen[prior.ActivityID] {
			return fmt.Errorf("%w: invalid hook dispatch history", ErrUnsupportedSchema)
		}
		seen[prior.ActivityID] = true
	}
	redeliveries := make(map[string]bool)
	for _, prior := range work.Redeliveries {
		key := hookAttemptKey(prior.ActivityID, prior.Generation)
		if !seen[prior.ActivityID] || prior.Generation == 0 || prior.AvailableAt.IsZero() || redeliveries[key] {
			return fmt.Errorf("%w: invalid hook redelivery history", ErrUnsupportedSchema)
		}
		redeliveries[key] = true
	}
	retryableAttempts := make(map[string]bool)
	interventionAttempts := make(map[string]bool)
	for i, attempt := range progress.Attempts {
		if attempt.Metadata.ExecutionID != executionID || attempt.Metadata.Number != uint(i+1) ||
			attempt.Metadata.ActivityID == "" || attempt.Metadata.ActivityIdempotencyKey == "" ||
			attempt.Metadata.LeaseGeneration == 0 || attempt.Metadata.LeaseGeneration > uint64(work.Generation) ||
			attempt.StartedAt.IsZero() || (attempt.Result == nil) != attempt.CompletedAt.IsZero() {
			return fmt.Errorf("%w: invalid hook attempt history", ErrUnsupportedSchema)
		}
		expectedID := hookID(executionID, transition.HookDispatch{Kind: attempt.Kind, RetryCycle: attempt.Metadata.RetryCycle})
		if !seen[expectedID] || attempt.Metadata.ActivityID != expectedID || attempt.Metadata.ActivityIdempotencyKey != expectedID ||
			(attempt.Result != nil && (!validHookResult(*attempt.Result) || attempt.CompletedAt.Before(attempt.StartedAt))) ||
			(attempt.Unknown && attempt.Result != nil && attempt.Result.Success) ||
			(attempt.Unknown && attempt.Result == nil && (!work.FencedUnknown || i != len(progress.Attempts)-1)) {
			return fmt.Errorf("%w: inconsistent hook attempt history", ErrUnsupportedSchema)
		}
		switch attempt.Kind {
		case transition.HookEnter, transition.HookTransition, transition.HookRetry, transition.HookRecover:
		default:
			return fmt.Errorf("%w: invalid hook kind in history", ErrUnsupportedSchema)
		}
		if attempt.Result != nil && !attempt.Result.Success && attempt.Result.Disposition == fsm.DispositionRetry {
			key := hookAttemptKey(attempt.Metadata.ActivityID, attempt.Metadata.LeaseGeneration)
			retryableAttempts[key] = true
		}
		if attempt.Result != nil && !attempt.Result.Success && attempt.Result.Disposition == fsm.DispositionIntervention {
			interventionAttempts[hookAttemptKey(attempt.Metadata.ActivityID, attempt.Metadata.LeaseGeneration)] = true
		}
	}
	for _, prior := range work.AuthorizedRetries {
		key := hookAttemptKey(prior.ActivityID, prior.Generation)
		if !interventionAttempts[key] || retryableAttempts[key] {
			return fmt.Errorf("%w: invalid authorized retry history", ErrUnsupportedSchema)
		}
		retryableAttempts[key] = true
	}
	for _, prior := range work.Redeliveries {
		key := hookAttemptKey(prior.ActivityID, prior.Generation)
		if !retryableAttempts[key] {
			return fmt.Errorf("%w: redelivery has no retryable attempt", ErrUnsupportedSchema)
		}
	}
	if work.Started {
		if len(progress.Attempts) == 0 {
			return fmt.Errorf("%w: missing started hook attempt", ErrUnsupportedSchema)
		}
		attempt := progress.Attempts[len(progress.Attempts)-1]
		if attempt.Metadata.ActivityID != progress.ActivityID || attempt.Kind != progress.Dispatch.Kind ||
			attempt.Metadata.RetryCycle != progress.Dispatch.RetryCycle || attempt.Metadata.LeaseGeneration != uint64(work.Generation) ||
			progress.Completed != (attempt.Result != nil) || (progress.Completed && *attempt.Result != progress.Result) {
			return fmt.Errorf("%w: inconsistent hook outcome", ErrUnsupportedSchema)
		}
	} else if progress.Completed {
		return fmt.Errorf("%w: completed hook did not start", ErrUnsupportedSchema)
	}
	if work.FencedUnknown && (!work.Started || progress.Completed || len(progress.Attempts) == 0 ||
		!progress.Attempts[len(progress.Attempts)-1].Unknown || progress.Attempts[len(progress.Attempts)-1].Result != nil) {
		return fmt.Errorf("%w: invalid fenced unknown attempt", ErrUnsupportedSchema)
	}
	return nil
}

func validateHookWork(ctx context.Context, tx *sql.Tx) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT w.execution_id, w.work, e.id FROM rff_hook_work AS w
		LEFT JOIN rff_executions AS e ON e.id = w.execution_id`)
	if err != nil {
		return fmt.Errorf("%w: read hook work: %w", ErrUnsupportedSchema, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close hook work query: %w", closeErr))
		}
	}()
	for rows.Next() {
		var id string
		var data []byte
		var linked sql.NullString
		if err := rows.Scan(&id, &data, &linked); err != nil {
			return fmt.Errorf("%w: read hook work: %w", ErrUnsupportedSchema, err)
		}
		var work hookWork
		if err := json.Unmarshal(data, &work); err != nil {
			return fmt.Errorf("%w: decode hook work: %w", ErrUnsupportedSchema, err)
		}
		if !linked.Valid {
			return fmt.Errorf("%w: orphan hook work", ErrUnsupportedSchema)
		}
		if err := validateWork(id, work); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read hook work: %w", ErrUnsupportedSchema, err)
	}
	return nil
}

func (s *Store[EntityID, StateID]) hookTransaction(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin hook transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback hook transaction: %w", rollbackErr))
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit hook transaction: %w", err)
	}
	return nil
}

func loadHook(ctx context.Context, tx *sql.Tx, id string) (hookWork, error) {
	var data []byte
	err := tx.QueryRowContext(ctx, `SELECT work FROM rff_hook_work WHERE execution_id = ?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return hookWork{}, transition.ErrNoHookActivity
	}
	if err != nil {
		return hookWork{}, fmt.Errorf("read hook work: %w", err)
	}
	var work hookWork
	if err := json.Unmarshal(data, &work); err != nil {
		return work, fmt.Errorf("%w: decode hook work: %w", ErrUnsupportedSchema, err)
	}
	if err := validateWork(id, work); err != nil {
		return work, err
	}
	return work, nil
}

func saveHook(ctx context.Context, tx *sql.Tx, id string, work hookWork) error {
	if err := validateWork(id, work); err != nil {
		return err
	}
	data, err := json.Marshal(work)
	if err != nil {
		return fmt.Errorf("encode hook work: %w", err)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_hook_work WHERE execution_id = ?)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("check hook work: %w", err)
	}
	if exists {
		_, err = tx.ExecContext(ctx, `UPDATE rff_hook_work SET work = ? WHERE execution_id = ?`, data, id)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO rff_hook_work (execution_id, work) VALUES (?, ?)`, id, data)
	}
	if err != nil {
		return fmt.Errorf("write hook work: %w", err)
	}
	return nil
}

func validHookLease(ctx context.Context, tx *sql.Tx, lease transition.HookLease, work hookWork, now time.Time) error {
	if now.IsZero() {
		return transition.ErrInvalidLease
	}
	if lease.ExecutionID == "" || lease.ActivityID != work.Progress.ActivityID ||
		lease.Owner == "" || lease.Owner != work.Owner || lease.Generation != uint64(work.Generation) ||
		(work.Started && work.Progress.Attempts[len(work.Progress.Attempts)-1].Unknown) ||
		!now.Before(work.ExpiresAt) {
		return transition.ErrStaleActivityLease
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_active WHERE execution_id = ?)`, lease.ExecutionID).Scan(&active); err != nil {
		return fmt.Errorf("check hook reservation: %w", err)
	}
	if !active {
		return transition.ErrStaleActivityLease
	}
	return nil
}

func (s *Store[EntityID, StateID]) validTransitionFence(ctx context.Context, tx *sql.Tx, lease transition.TransitionLease[EntityID], now time.Time) error {
	entity, err := s.encodeEntity(lease.Entity)
	if err != nil {
		return err
	}
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Generation == 0 || lease.Generation > math.MaxInt64 {
		return transition.ErrStaleLease
	}
	var expiry string
	err = tx.QueryRowContext(ctx, `SELECT l.expires_at FROM rff_transition_leases AS l
		JOIN rff_active AS a ON a.entity = l.entity AND a.execution_id = l.execution_id
		WHERE l.entity = ? AND l.execution_id = ? AND l.owner = ? AND l.generation = ?`,
		entity, lease.ExecutionID, lease.Owner, int64(lease.Generation)).Scan(&expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrStaleLease
	}
	if err != nil {
		return fmt.Errorf("read transition fence: %w", err)
	}
	expires, err := time.Parse(time.RFC3339Nano, expiry)
	if err != nil {
		return fmt.Errorf("%w: invalid transition lease expiry", ErrUnsupportedSchema)
	}
	if !now.Before(expires) {
		return transition.ErrStaleLease
	}
	return nil
}

func equalDispatch(a, b transition.HookDispatch) bool {
	if a.Kind != b.Kind || a.RetryCycle != b.RetryCycle || !a.DueAt.Equal(b.DueAt) || (a.Failure == nil) != (b.Failure == nil) {
		return false
	}
	return a.Failure == nil || *a.Failure == *b.Failure
}

func ensureCompatibleExecution(ctx context.Context, tx *sql.Tx, id string) error {
	var data []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, id).Scan(&data); err != nil {
		return fmt.Errorf("read execution compatibility: %w", err)
	}
	var execution diskRecord
	if err := json.Unmarshal(data, &execution); err != nil || !transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != id {
		return fmt.Errorf("%w: invalid execution compatibility", ErrUnsupportedSchema)
	}
	if execution.Compatibility != nil {
		return transition.ErrCompatibility
	}
	return nil
}

// DispatchHook durably schedules a logical hook under the current transition fence.
func (s *Store[EntityID, StateID]) DispatchHook(ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, dispatch transition.HookDispatch) (string, error) {
	if clock == nil || !validDispatch(dispatch) {
		return "", transition.ErrInvalidHook
	}
	id := hookID(lease.ExecutionID, dispatch)
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		if err := s.validTransitionFence(ctx, tx, lease, now); err != nil {
			return err
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil && !errors.Is(err, transition.ErrNoHookActivity) {
			return err
		}
		existed := err == nil
		if err == nil {
			for _, prior := range work.History {
				if prior.ActivityID == id {
					if !equalDispatch(prior.Dispatch, dispatch) {
						return transition.ErrConflict
					}
					return nil
				}
			}
			if work.Progress.ActivityID == id {
				if !equalDispatch(work.Progress.Dispatch, dispatch) {
					return transition.ErrConflict
				}
				return nil
			}
			if !work.Progress.Completed {
				return transition.ErrBusy
			}
			work.History = append(work.History, hookDispatchHistory{
				ActivityID: work.Progress.ActivityID, Dispatch: work.Progress.Dispatch,
			})
		} else {
			work.Version = 1
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read dispatch execution: %w", err)
		}
		var execution diskRecord
		if err := json.Unmarshal(data, &execution); err != nil {
			return fmt.Errorf("%w: decode dispatch execution: %w", ErrUnsupportedSchema, err)
		}
		if !transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != lease.ExecutionID {
			return fmt.Errorf("%w: invalid dispatch execution", ErrUnsupportedSchema)
		}
		if execution.Compatibility != nil {
			return transition.ErrCompatibility
		}
		if execution.Operation != transition.Initialize && execution.Operation != transition.Transition ||
			(dispatch.Kind == transition.HookEnter && execution.Operation != transition.Initialize) ||
			(dispatch.Kind == transition.HookTransition && execution.Operation != transition.Transition) ||
			(dispatch.Kind == transition.HookRecover && execution.Status != transition.Aborting) ||
			(dispatch.Kind != transition.HookRecover && execution.Status != transition.Waiting && execution.Status != transition.Running) {
			return transition.ErrInvalidHook
		}
		if existed && (dispatch.Kind == transition.HookEnter || dispatch.Kind == transition.HookTransition) {
			if work.Progress.Dispatch.Kind != transition.HookRetry || !work.Progress.Result.Success {
				return transition.ErrInvalidHook
			}
			var prior *transition.HookAttempt
			for i := len(work.Progress.Attempts) - 1; i >= 0; i-- {
				attempt := &work.Progress.Attempts[i]
				if attempt.Kind == transition.HookEnter || attempt.Kind == transition.HookTransition {
					prior = attempt
					break
				}
			}
			if prior == nil || prior.Result == nil || prior.Result.Success || prior.Unknown {
				return transition.ErrInvalidHook
			}
		}
		if dispatch.Kind == transition.HookRetry {
			if len(work.Progress.Attempts) == 0 || !work.Progress.Completed || work.Progress.Result.Success ||
				work.Progress.Dispatch.Kind == transition.HookRetry || work.Progress.Dispatch.Kind == transition.HookRecover ||
				*dispatch.Failure != work.Progress.Result.Failure {
				return transition.ErrInvalidHook
			}
			delay, err := retryDelay(execution.Policy, work)
			if err != nil {
				return err
			}
			last := work.Progress.Attempts[len(work.Progress.Attempts)-1]
			if !dispatch.DueAt.Equal(last.CompletedAt.Add(delay)) {
				return transition.ErrInvalidHook
			}
		}
		work.Progress.ActivityID = id
		work.Progress.Dispatch = dispatch
		work.Progress.AvailableAt = dispatch.DueAt
		work.Progress.Completed = false
		work.Progress.Result = transition.HookResult{}
		work.Owner = ""
		work.ExpiresAt = time.Time{}
		work.Started = false
		return saveHook(ctx, tx, lease.ExecutionID, work)
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func retryDelay(policy fsm.Policy, work hookWork) (time.Duration, error) {
	if len(work.Progress.Attempts) == 0 || work.BudgetUsed == 0 {
		return 0, transition.ErrInvalidHook
	}
	last := work.Progress.Attempts[len(work.Progress.Attempts)-1]
	if last.Result == nil || last.Result.Success {
		return 0, transition.ErrInvalidHook
	}
	if work.BudgetUsed >= policy.MaxAttempts {
		return 0, transition.ErrAttemptBudget
	}
	var retryableFailures uint
	for _, attempt := range work.Progress.Attempts[len(work.Progress.Attempts)-int(work.BudgetUsed):] {
		if attempt.Result != nil && !attempt.Result.Success && attempt.Result.Disposition == fsm.DispositionRetry {
			retryableFailures++
		}
	}
	if retryableFailures == 0 {
		return 0, transition.ErrRetryIntervention
	}
	retry, delay, err := transition.DecideRetry(policy, work.BudgetUsed, retryableFailures-1, last.Result.Disposition)
	if err != nil {
		return 0, err
	}
	if !retry {
		return 0, transition.ErrRetryIntervention
	}
	return delay, nil
}

// ReadHookProgress returns the durable safe outcome and attempt history.
func (s *Store[EntityID, StateID]) ReadHookProgress(ctx context.Context, executionID string) (transition.HookProgress, error) {
	var progress transition.HookProgress
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		work, err := loadHook(ctx, tx, executionID)
		if err != nil {
			return err
		}
		progress = work.Progress
		progress.Started = work.Started
		progress.LeaseExpiresAt = work.ExpiresAt
		progress.BudgetUsed = work.BudgetUsed
		return nil
	})
	if err != nil {
		return transition.HookProgress{}, err
	}
	return progress, nil
}

// ResolveUnknownHook records a safe classified outcome after a started claim expires.
func (s *Store[EntityID, StateID]) ResolveUnknownHook(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time,
	activityID string, generation uint64, result transition.HookResult,
) error {
	if clock == nil || activityID == "" || generation == 0 || result.Success || !validHookResult(result) {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		if err := s.validTransitionFence(ctx, tx, lease, now); err != nil {
			return err
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil {
			return err
		}
		for _, attempt := range work.Progress.Attempts {
			if attempt.Metadata.ActivityID == activityID && attempt.Metadata.LeaseGeneration == generation && attempt.Unknown && attempt.Result != nil {
				if *attempt.Result == result {
					return nil
				}
				return transition.ErrConflict
			}
		}
		if err := ensureCompatibleExecution(ctx, tx, lease.ExecutionID); err != nil {
			return err
		}
		if work.Progress.ActivityID != activityID || uint64(work.Generation) != generation ||
			!work.Started || work.Progress.Completed {
			return transition.ErrStaleActivityLease
		}
		if now.Before(work.ExpiresAt) {
			return transition.ErrActivityLeaseHeld
		}
		last := &work.Progress.Attempts[len(work.Progress.Attempts)-1]
		last.CompletedAt = now.UTC()
		last.Result = &result
		last.Unknown = true
		work.FencedUnknown = false
		work.Progress.Completed = true
		work.Progress.Result = result
		return saveHook(ctx, tx, lease.ExecutionID, work)
	})
}

// RedeliverHook schedules another attempt of a previously failed invocation.
func (s *Store[EntityID, StateID]) RedeliverHook(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time,
	activityID string, generation uint64, availableAt time.Time,
) error {
	if clock == nil || activityID == "" || generation == 0 || availableAt.IsZero() {
		return transition.ErrInvalidHook
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		if err := s.validTransitionFence(ctx, tx, lease, now); err != nil {
			return err
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil {
			return err
		}
		for _, prior := range work.Redeliveries {
			if prior.ActivityID == activityID && prior.Generation == generation {
				if prior.AvailableAt.Equal(availableAt) {
					return nil
				}
				return transition.ErrConflict
			}
		}
		if err := ensureCompatibleExecution(ctx, tx, lease.ExecutionID); err != nil {
			return err
		}
		var failed *transition.HookAttempt
		for i := range work.Progress.Attempts {
			attempt := &work.Progress.Attempts[i]
			if attempt.Metadata.ActivityID == activityID && attempt.Metadata.LeaseGeneration == generation {
				failed = attempt
				break
			}
		}
		if failed == nil || failed.Result == nil || failed.Result.Success {
			return transition.ErrInvalidHook
		}
		authorized := false
		for _, prior := range work.AuthorizedRetries {
			if prior.ActivityID == activityID && prior.Generation == generation {
				authorized = true
				break
			}
		}
		if failed.Result.Disposition != fsm.DispositionRetry && !authorized {
			return transition.ErrInvalidHook
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read redelivery execution: %w", err)
		}
		var execution diskRecord
		if err := json.Unmarshal(data, &execution); err != nil {
			return fmt.Errorf("%w: decode redelivery execution: %w", ErrUnsupportedSchema, err)
		}
		if !transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != lease.ExecutionID {
			return fmt.Errorf("%w: invalid redelivery execution", ErrUnsupportedSchema)
		}
		if work.BudgetUsed >= execution.Policy.MaxAttempts {
			return transition.ErrAttemptBudget
		}
		deadline := execution.effectiveDeadline()
		if deadline.IsZero() || !now.Before(deadline) || !availableAt.Before(deadline) {
			return transition.ErrTransitionDeadline
		}
		last := work.Progress.Attempts[len(work.Progress.Attempts)-1]
		if !work.Progress.Completed {
			return transition.ErrBusy
		}
		dispatch := work.Progress.Dispatch
		switch failed.Kind {
		case transition.HookEnter, transition.HookTransition:
			if !failed.Unknown || work.Progress.Dispatch.Kind != transition.HookRetry ||
				!work.Progress.Result.Success || last.Metadata.Number <= failed.Metadata.Number ||
				work.Progress.Dispatch.Failure == nil || *work.Progress.Dispatch.Failure != failed.Result.Failure ||
				!availableAt.Equal(last.CompletedAt) {
				return transition.ErrInvalidHook
			}
			found := -1
			for i, prior := range work.History {
				if prior.ActivityID == activityID {
					found, dispatch = i, prior.Dispatch
					break
				}
			}
			if found < 0 {
				return transition.ErrInvalidHook
			}
			work.History = append(work.History[:found], work.History[found+1:]...)
			work.History = append(work.History, hookDispatchHistory{
				ActivityID: work.Progress.ActivityID, Dispatch: work.Progress.Dispatch,
			})
		case transition.HookRetry, transition.HookRecover:
			if work.Progress.ActivityID != activityID || last.Metadata.LeaseGeneration != generation {
				return transition.ErrInvalidHook
			}
			delay, err := retryDelay(execution.Policy, work)
			if err != nil {
				return err
			}
			if !availableAt.Equal(failed.CompletedAt.Add(delay)) {
				return transition.ErrInvalidHook
			}
		default:
			return transition.ErrInvalidHook
		}
		work.Progress.ActivityID = activityID
		work.Progress.Dispatch = dispatch
		work.Progress.AvailableAt = availableAt
		work.Progress.Completed = false
		work.Progress.Result = transition.HookResult{}
		work.Owner = ""
		work.ExpiresAt = time.Time{}
		work.Started = false
		work.Redeliveries = append(work.Redeliveries, hookRedelivery{
			ActivityID: activityID, Generation: generation, AvailableAt: availableAt,
		})
		return saveHook(ctx, tx, lease.ExecutionID, work)
	})
}

func validHookResult(result transition.HookResult) bool {
	if result.Success {
		return result.Failure == (fsm.Failure{}) && result.Disposition == ""
	}
	return result.Disposition == fsm.DispositionRetry || result.Disposition == fsm.DispositionIntervention
}

// MarkWorkSuccess persists the commit-pending marker after successful work.
func (s *Store[EntityID, StateID]) MarkWorkSuccess(ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time) (string, error) {
	if clock == nil {
		return "", transition.ErrInvalidLease
	}
	var operationID string
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		if err := s.validTransitionFence(ctx, tx, lease, now); err != nil {
			return err
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, lease.ExecutionID).Scan(&data); err != nil {
			return fmt.Errorf("read successful execution: %w", err)
		}
		var execution diskRecord
		if err := json.Unmarshal(data, &execution); err != nil {
			return fmt.Errorf("%w: decode successful execution: %w", ErrUnsupportedSchema, err)
		}
		if !transition.SupportedExecutionSchema(execution.SchemaVersion) || execution.ID != lease.ExecutionID {
			return fmt.Errorf("%w: invalid successful execution", ErrUnsupportedSchema)
		}
		if execution.CommitOperationID != "" {
			operationID = execution.CommitOperationID
			return nil
		}
		if execution.Compatibility != nil {
			return transition.ErrCompatibility
		}
		if err := ensureOperationsSettled(ctx, tx, lease.ExecutionID); err != nil {
			return err
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil {
			return err
		}
		if execution.Status != transition.Running || !work.Progress.Completed || !work.Progress.Result.Success ||
			(work.Progress.Dispatch.Kind != transition.HookEnter && work.Progress.Dispatch.Kind != transition.HookTransition) {
			return transition.ErrInvalidHook
		}
		sum := sha256.Sum256([]byte("rff-commit:" + lease.ExecutionID))
		operationID = "rff-commit:" + hex.EncodeToString(sum[:])
		execution.CommitOperationID = operationID
		data, err = json.Marshal(execution)
		if err != nil {
			return fmt.Errorf("encode successful execution: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_executions SET record = ? WHERE id = ?`, data, lease.ExecutionID); err != nil {
			return fmt.Errorf("record successful work: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return operationID, nil
}
