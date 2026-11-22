package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func migrationDigest(parameters []byte) string {
	sum := sha256.Sum256(parameters)
	return hex.EncodeToString(sum[:])
}

func sameMigration(a transition.MigrationReceipt, b transition.MigrationRequest) bool {
	return a.Request.ID == b.ID && a.Request.MachineID == b.MachineID && a.Request.MachineVersion == b.MachineVersion &&
		a.ParametersDigest == migrationDigest(b.Parameters)
}

// ApplyExecutionMigration persists an authorized pre-commit conversion atomically.
func (s *Store[EntityID, StateID]) ApplyExecutionMigration(
	ctx context.Context, lease transition.TransitionLease[EntityID], clock func() time.Time, request transition.MigrationRequest,
	convert func(context.Context, transition.MigrationSource[EntityID]) (transition.MigrationSnapshot[StateID], error),
	validate func(context.Context, transition.MigrationSource[EntityID], transition.MigrationSnapshot[StateID], transition.Execution[EntityID, StateID], transition.HookCall) error,
) (transition.MigrationReceipt, error) {
	var receipt transition.MigrationReceipt
	var migrationFailure error
	if clock == nil || request.ID == "" || request.MachineID == "" || request.MachineVersion == "" || convert == nil || validate == nil {
		return receipt, transition.ErrInvalidRequest
	}
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		stored, err := s.readControlExecution(ctx, tx, lease)
		if err != nil {
			return err
		}
		for _, prior := range stored.Migrations {
			if prior.Request.ID == request.ID {
				if !sameMigration(prior, request) {
					return transition.ErrConflict
				}
				receipt = prior
				return nil
			}
		}
		_, stored, err = s.fencedExecution(ctx, tx, lease, clock)
		if err != nil {
			return err
		}
		if stored.CommitOperationID != "" || stored.RecoveryStarted ||
			(stored.Status != transition.Waiting && stored.Status != transition.Running && stored.Status != transition.NeedsIntervention) {
			return transition.ErrInvalidHook
		}
		work, err := loadHook(ctx, tx, lease.ExecutionID)
		if err != nil && !errors.Is(err, transition.ErrNoHookActivity) {
			return err
		}
		hasWork := err == nil
		if hasWork && work.Progress.Completed && work.Progress.Result.Success &&
			(work.Progress.Dispatch.Kind == transition.HookEnter || work.Progress.Dispatch.Kind == transition.HookTransition) {
			return transition.ErrInvalidHook
		}
		original := stored
		block := func(failure error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			blockTime := clock()
			if blockTime.IsZero() {
				return transition.ErrInvalidLease
			}
			if err := s.validTransitionFence(ctx, tx, lease, blockTime); err != nil {
				return err
			}
			if hasWork {
				fenceMigrationWork(&work, blockTime)
				if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
					return err
				}
			}
			original.Compatibility = &transition.CompatibilityDiagnostic{
				Component: transition.CompatibilityMigration, Name: request.MachineID, Version: request.MachineVersion,
			}
			if err := saveExecution(ctx, tx, lease.ExecutionID, original); err != nil {
				return err
			}
			migrationFailure = failure
			return nil
		}
		entity, err := s.entityCodec.Decode(stored.Entity)
		if err != nil {
			return transition.NewCompatibilityError(transition.CompatibilityDiagnostic{
				Component: transition.CompatibilityEntity, Name: stored.MachineID, Version: stored.MachineVersion,
			}, err)
		}
		source := transition.MigrationSource[EntityID]{
			ExecutionID: stored.ID, Entity: entity, Operation: stored.Operation,
			MachineID: stored.MachineID, MachineVersion: stored.MachineVersion,
			Previous: slices.Clone(stored.Previous), HasPrevious: stored.HasPrevious, Target: slices.Clone(stored.Target),
			Input: stored.Input, Event: stored.Event, Policy: stored.Policy,
		}
		source.Input.Data = slices.Clone(source.Input.Data)
		source.Event.Data = slices.Clone(source.Event.Data)
		source.Policy.RetryDelays = slices.Clone(source.Policy.RetryDelays)
		snapshot, err := convert(ctx, source)
		if err != nil {
			return block(err)
		}
		if snapshot.MachineID != request.MachineID || snapshot.MachineVersion != request.MachineVersion ||
			snapshot.HasPrevious != stored.HasPrevious ||
			snapshot.Policy.MaxAttempts != stored.Policy.MaxAttempts || snapshot.Policy.AttemptTimeout != stored.Policy.AttemptTimeout ||
			snapshot.Policy.OverallTimeout != stored.Policy.OverallTimeout || !slices.Equal(snapshot.Policy.RetryDelays, stored.Policy.RetryDelays) {
			return block(transition.ErrInvalidDecision)
		}
		target, err := s.encodeState(snapshot.Target)
		if err != nil {
			return block(fmt.Errorf("encode migrated target: %w", err))
		}
		var previous []byte
		if snapshot.HasPrevious {
			previous, err = s.encodeState(snapshot.Previous)
			if err != nil {
				return block(fmt.Errorf("encode migrated source: %w", err))
			}
		}
		if stored.OriginalRequest == nil {
			stored.OriginalRequest = &transition.OriginalRequest{
				Operation: stored.Operation, MachineID: stored.MachineID, MachineVersion: stored.MachineVersion,
				Event: stored.Event, Input: stored.Input, NotificationOverride: stored.NotificationOverride,
			}
		}
		stored.MachineID, stored.MachineVersion = snapshot.MachineID, snapshot.MachineVersion
		stored.Previous, stored.Target = previous, target
		stored.Event, stored.Input, stored.Policy = snapshot.Event, snapshot.Input, snapshot.Policy
		data, err := json.Marshal(stored)
		if err != nil {
			return fmt.Errorf("encode migrated execution: %w", err)
		}
		candidate, err := s.decodeRecord(data)
		if err != nil {
			return block(err)
		}
		call := transition.HookCall{Kind: transition.HookEnter}
		if stored.Operation == transition.Transition {
			call.Kind = transition.HookTransition
		}
		if hasWork {
			call.Kind, call.Failure = work.Progress.Dispatch.Kind, work.Progress.Dispatch.Failure
		}
		if err := validate(ctx, source, snapshot, candidate, call); err != nil {
			return block(err)
		}
		decisionTime := clock()
		if decisionTime.IsZero() {
			return transition.ErrInvalidLease
		}
		if err := s.validTransitionFence(ctx, tx, lease, decisionTime); err != nil {
			return err
		}
		if hasWork {
			fenceMigrationWork(&work, decisionTime)
			if err := saveHook(ctx, tx, lease.ExecutionID, work); err != nil {
				return err
			}
		}
		stored.Compatibility = nil
		receipt = transition.MigrationReceipt{Request: request, ParametersDigest: migrationDigest(request.Parameters), AcceptedAt: decisionTime.UTC()}
		receipt.Request.Parameters = nil
		stored.Migrations = append(stored.Migrations, receipt)
		return saveExecution(ctx, tx, lease.ExecutionID, stored)
	})
	if err != nil {
		return transition.MigrationReceipt{}, err
	}
	if migrationFailure != nil {
		return transition.MigrationReceipt{}, migrationFailure
	}
	return receipt, nil
}

func fenceMigrationWork(work *hookWork, now time.Time) {
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
