package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type operationRecord struct {
	Version             int
	Binding             transition.OperationBinding
	ActivityID          string
	Generation          uint64
	DispatchPending     bool
	DeclarationOnly     bool
	PrerequisiteBlocked bool
	Result              *transition.OperationResult
	History             []transition.OperationHistoryEntry
	EvidenceConflict    bool
}

func decodeOperation(id string, key []byte, data []byte) (operationRecord, error) {
	var record operationRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("%w: invalid operation record", ErrUnsupportedSchema)
	}
	b := record.Binding
	if (record.Version != 1 && record.Version != 2 && record.Version != 3) || b.Identity.ExecutionID != id || !bytes.Equal([]byte(b.Identity.Key), key) ||
		id == "" || b.Identity.Key == "" || b.Kind == "" || b.BehaviorVersion == "" ||
		b.ComparatorVersion == "" || b.ResultCodec.Name == "" || b.ResultCodec.Version == "" ||
		record.ActivityID == "" || record.Generation == 0 ||
		(record.Result != nil && !transition.ValidOperationResult(*record.Result, b.ResultCodec)) {
		return record, fmt.Errorf("%w: inconsistent operation record", ErrUnsupportedSchema)
	}
	if record.Version == 1 {
		record.Version = 2
		record.DispatchPending = record.Result == nil
	}
	b, bindingErr := transition.NormalizeOperationBinding(b)
	if bindingErr != nil || record.DispatchPending && record.Result != nil ||
		record.DeclarationOnly && (record.DispatchPending || record.Result != nil) || !validOperationHistory(b, record.History) {
		return record, fmt.Errorf("%w: inconsistent operation history", ErrUnsupportedSchema)
	}
	record.Binding = b
	return record, nil
}

func validOperationHistory(binding transition.OperationBinding, history []transition.OperationHistoryEntry) bool {
	requests := make(map[string]bool)
	for _, entry := range history {
		if (entry.RequestID == "") != (entry.Principal == "") || (entry.RequestID == "") != (entry.Reason == "") {
			return false
		}
		if entry.RequestID != "" {
			if requests[entry.RequestID] {
				return false
			}
			requests[entry.RequestID] = true
		}
		if result := entry.Assessment.Result; result != nil && !transition.ValidOperationResult(*result, binding.ResultCodec) {
			return false
		}
		evidence := entry.Assessment.Evidence
		if evidence.Identity == (transition.OperationIdentity{}) {
			switch entry.Assessment.Outcome {
			case transition.OperationOutcomeSucceeded:
				if entry.Assessment.Result == nil || entry.Assessment.Result.FailureCode != "" || entry.Assessment.Repeat != nil {
					return false
				}
			case transition.OperationOutcomeFailed:
				if entry.Assessment.Result == nil || entry.Assessment.Result.FailureCode == "" {
					return false
				}
			default:
				return false
			}
			continue
		}
		if evidence.Identity != binding.Identity || evidence.Outcome != entry.Assessment.Outcome ||
			evidence.Provider == "" || evidence.Scope == "" || evidence.Reference == "" ||
			evidence.InputFingerprint == "" || evidence.Version == "" || evidence.Provenance == "" ||
			evidence.BehaviorVersion != binding.BehaviorVersion ||
			evidence.ComparatorVersion != binding.ComparatorVersion ||
			evidence.ObservedAt.IsZero() || evidence.ValidUntil.IsZero() ||
			evidence.ValidUntil.Before(evidence.ObservedAt) {
			return false
		}
		switch entry.Assessment.Outcome {
		case transition.OperationOutcomeSucceeded:
			if entry.Assessment.Result == nil || entry.Assessment.Result.FailureCode != "" || entry.Assessment.Repeat != nil {
				return false
			}
		case transition.OperationOutcomeFailed:
			if entry.Assessment.Result == nil || entry.Assessment.Result.FailureCode == "" {
				return false
			}
		case transition.OperationOutcomeUnknown, transition.OperationOutcomeAbsent:
			if entry.Assessment.Result != nil {
				return false
			}
		default:
			return false
		}
		if entry.Assessment.Repeat != nil &&
			!transition.OperationRepeatValid(binding, evidence, *entry.Assessment.Repeat, evidence.ObservedAt) {
			return false
		}
	}
	return true
}

func validateOperations(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT o.execution_id, o.operation_key, e.id FROM rff_operations o LEFT JOIN rff_executions e ON e.id = o.execution_id`)
	if err != nil {
		return fmt.Errorf("%w: read operations: %w", ErrUnsupportedSchema, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var key []byte
		var execution sql.NullString
		if err := rows.Scan(&id, &key, &execution); err != nil {
			return fmt.Errorf("%w: read operation: %w", ErrUnsupportedSchema, err)
		}
		if !execution.Valid || id == "" || len(key) == 0 {
			return fmt.Errorf("%w: operation execution missing", ErrUnsupportedSchema)
		}
	}
	return rows.Err()
}

func operationClaim(ctx context.Context, tx *sql.Tx, lease transition.HookLease, now time.Time) error {
	work, err := loadClaim(ctx, tx, lease, now)
	if err != nil {
		return err
	}
	if !work.Started || work.Progress.Completed {
		return transition.ErrStaleActivityLease
	}
	return ensureCompatibleExecution(ctx, tx, lease.ExecutionID)
}

// InspectTrackedOperation reads a durable operation without requiring an active hook.
func (s *Store[EntityID, StateID]) InspectTrackedOperation(ctx context.Context, identity transition.OperationIdentity) (transition.OperationInspection, error) {
	var inspection transition.OperationInspection
	if identity.ExecutionID == "" || identity.Key == "" {
		return inspection, transition.ErrInvalidRequest
	}
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, identity.ExecutionID, []byte(identity.Key)).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return inspection, transition.ErrOperationNotFound
	}
	if err != nil {
		return inspection, err
	}
	record, err := decodeOperation(identity.ExecutionID, []byte(identity.Key), data)
	if err != nil {
		return inspection, err
	}
	return operationInspection(record), nil
}

func operationInspection(record operationRecord) transition.OperationInspection {
	return transition.OperationInspection{
		Binding: cloneOperationBinding(record.Binding), ActivityID: record.ActivityID, Generation: record.Generation,
		MayHaveDispatched: !record.DeclarationOnly, DispatchPending: record.DispatchPending,
		PrerequisiteBlocked: record.PrerequisiteBlocked,
		Result:              cloneOperationResult(record.Result),
		History:             cloneOperationHistory(record.History), EvidenceConflict: record.EvidenceConflict,
	}
}

func cloneOperationResult(result *transition.OperationResult) *transition.OperationResult {
	if result == nil {
		return nil
	}
	copy := *result
	copy.Data = bytes.Clone(result.Data)
	return &copy
}

func cloneOperationBinding(binding transition.OperationBinding) transition.OperationBinding {
	binding.Input = bytes.Clone(binding.Input)
	binding.Prerequisites = slices.Clone(binding.Prerequisites)
	return binding
}

func sameOperationResult(a, b *transition.OperationResult) bool {
	return a != nil && b != nil && a.Codec == b.Codec && a.NoResult == b.NoResult &&
		a.FailureCode == b.FailureCode && (a.Data == nil) == (b.Data == nil) && bytes.Equal(a.Data, b.Data)
}

func cloneOperationHistory(history []transition.OperationHistoryEntry) []transition.OperationHistoryEntry {
	cloned := make([]transition.OperationHistoryEntry, len(history))
	copy(cloned, history)
	for i := range cloned {
		cloned[i].Assessment.Result = cloneOperationResult(history[i].Assessment.Result)
		if history[i].Assessment.Repeat != nil {
			repeat := *history[i].Assessment.Repeat
			cloned[i].Assessment.Repeat = &repeat
		}
	}
	return cloned
}

// BeginTrackedOperation records the intent and possible dispatch atomically.
func (s *Store[EntityID, StateID]) BeginTrackedOperation(
	ctx context.Context, lease transition.HookLease, clock func() time.Time, binding transition.OperationBinding,
	equal func(context.Context, []byte, []byte) (bool, error),
) (*transition.OperationResult, bool, error) {
	var err error
	binding, err = transition.NormalizeOperationBinding(binding)
	if err != nil {
		return nil, false, err
	}
	if clock == nil || equal == nil || binding.Identity.ExecutionID != lease.ExecutionID || binding.Identity.Key == "" ||
		binding.Kind == "" || binding.BehaviorVersion == "" || binding.ComparatorVersion == "" ||
		binding.ResultCodec.Name == "" || binding.ResultCodec.Version == "" {
		return nil, false, transition.ErrInvalidRequest
	}
	var result *transition.OperationResult
	var started bool
	var deferredErr error
	err = s.hookTransaction(ctx, func(tx *sql.Tx) error {
		if err := operationClaim(ctx, tx, lease, clock()); err != nil {
			return err
		}
		var data []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, lease.ExecutionID, []byte(binding.Identity.Key)).Scan(&data)
		if err == nil {
			record, err := decodeOperation(lease.ExecutionID, []byte(binding.Identity.Key), data)
			if err != nil {
				return err
			}
			if record.EvidenceConflict {
				return transition.ErrOperationEvidenceConflict
			}
			old := record.Binding
			if old.Kind != binding.Kind || old.BehaviorVersion != binding.BehaviorVersion ||
				old.ComparatorVersion != binding.ComparatorVersion || old.ResultCodec != binding.ResultCodec ||
				old.Compensation != binding.Compensation || !operationPrerequisitesEqual(old.Prerequisites, binding.Prerequisites) {
				return transition.ErrConflict
			}
			matches, err := equal(ctx, bytes.Clone(old.Input), bytes.Clone(binding.Input))
			if claimErr := operationClaim(ctx, tx, lease, clock()); claimErr != nil {
				return claimErr
			}
			if err != nil {
				return fmt.Errorf("%w: operation comparison unavailable", transition.ErrCompatibility)
			}
			if !matches {
				return transition.ErrConflict
			}
			if err := ensureOperationPrerequisites(ctx, tx, binding); err != nil {
				record.PrerequisiteBlocked = true
				if saveErr := saveOperationRecord(ctx, tx, binding.Identity, record); saveErr != nil {
					return saveErr
				}
				if isOperationPrerequisiteBlock(err) {
					deferredErr = err
					return operationClaim(ctx, tx, lease, clock())
				}
				return err
			}
			if record.PrerequisiteBlocked {
				record.PrerequisiteBlocked = false
				if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
					return err
				}
			}
			if record.DeclarationOnly {
				record.DeclarationOnly = false
				record.DispatchPending = true
				record.PrerequisiteBlocked = false
				if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
					return err
				}
				started = true
				return operationClaim(ctx, tx, lease, clock())
			}
			if record.Result == nil {
				return transition.ErrOperationUnresolved
			}
			result = record.Result
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read operation: %w", err)
		}
		record := operationRecord{Version: 3, Binding: binding, ActivityID: lease.ActivityID, Generation: lease.Generation, DeclarationOnly: true}
		data, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode operation: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_operations (execution_id, operation_key, record) VALUES (?, ?, ?)`, lease.ExecutionID, []byte(binding.Identity.Key), data); err != nil {
			return fmt.Errorf("record operation intent: %w", err)
		}
		if err := ensureOperationPrerequisites(ctx, tx, binding); err != nil {
			if isOperationPrerequisiteBlock(err) {
				record.PrerequisiteBlocked = true
				if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
					return err
				}
				deferredErr = err
				return operationClaim(ctx, tx, lease, clock())
			}
			return err
		}
		record.DeclarationOnly = false
		record.DispatchPending = true
		record.PrerequisiteBlocked = false
		if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
			return err
		}
		if err := operationClaim(ctx, tx, lease, clock()); err != nil {
			return err
		}
		started = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if deferredErr != nil {
		return nil, false, deferredErr
	}
	return result, started, nil
}

// ReconcileTrackedOperation persists provider evidence and only opens a new
// dispatch after authoritative quiescence or a valid provider repeat guarantee.
func (s *Store[EntityID, StateID]) ReconcileTrackedOperation(
	ctx context.Context,
	lease transition.HookLease,
	clock func() time.Time,
	binding transition.OperationBinding,
	assessment transition.OperationAssessment,
	equal func(context.Context, []byte, []byte) (bool, error),
) (transition.OperationReconciliation, error) {
	var normalizeErr error
	binding, normalizeErr = transition.NormalizeOperationBinding(binding)
	if clock == nil || equal == nil || binding.Identity.ExecutionID != lease.ExecutionID {
		return transition.OperationReconciliation{}, transition.ErrInvalidRequest
	}
	if normalizeErr != nil {
		return transition.OperationReconciliation{}, normalizeErr
	}
	var result transition.OperationReconciliation
	var deferredErr error
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		if err := operationClaim(ctx, tx, lease, now); err != nil {
			return err
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, binding.Identity.ExecutionID, []byte(binding.Identity.Key)).Scan(&data); err != nil {
			return fmt.Errorf("read operation: %w", err)
		}
		record, err := decodeOperation(binding.Identity.ExecutionID, []byte(binding.Identity.Key), data)
		if err != nil {
			return err
		}
		if record.EvidenceConflict {
			return transition.ErrOperationEvidenceConflict
		}
		if record.Binding.Identity != binding.Identity || record.Binding.Kind != binding.Kind ||
			record.Binding.BehaviorVersion != binding.BehaviorVersion ||
			record.Binding.ComparatorVersion != binding.ComparatorVersion || record.Binding.ResultCodec != binding.ResultCodec ||
			record.Binding.Compensation != binding.Compensation ||
			!operationPrerequisitesEqual(record.Binding.Prerequisites, binding.Prerequisites) {
			return transition.ErrConflict
		}
		if err := ensureOperationPrerequisites(ctx, tx, binding); err != nil {
			return err
		}
		matches, err := equal(ctx, bytes.Clone(record.Binding.Input), bytes.Clone(binding.Input))
		if claimErr := operationClaim(ctx, tx, lease, clock()); claimErr != nil {
			return claimErr
		}
		if err != nil {
			return fmt.Errorf("%w: operation comparison unavailable", transition.ErrCompatibility)
		}
		if !matches {
			return transition.ErrConflict
		}
		if !transition.OperationAssessmentValid(binding, assessment, now) {
			return transition.ErrCompatibility
		}
		if len(record.History) > 0 {
			for i := len(record.History) - 1; i >= 0; i-- {
				prior := record.History[i].Assessment.Evidence
				if prior.Identity == binding.Identity && assessment.Outcome != transition.OperationOutcomeUnknown &&
					prior.Outcome != transition.OperationOutcomeUnknown && prior.Outcome != assessment.Outcome {
					record.EvidenceConflict = true
					record.History = append(record.History, transition.OperationHistoryEntry{Assessment: assessment, RecordedAt: now})
					data, err = json.Marshal(record)
					if err != nil {
						return fmt.Errorf("encode conflicting operation evidence: %w", err)
					}
					if _, err := tx.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = ? AND operation_key = ?`, data, binding.Identity.ExecutionID, []byte(binding.Identity.Key)); err != nil {
						return err
					}
					deferredErr = transition.ErrOperationEvidenceConflict
					return operationClaim(ctx, tx, lease, clock())
				}
				if prior.Identity == binding.Identity {
					break
				}
			}
		}
		if record.Result != nil && record.Result.FailureCode == "" {
			if assessment.Outcome == transition.OperationOutcomeUnknown {
				record.History = append(record.History, transition.OperationHistoryEntry{Assessment: assessment, RecordedAt: now})
				if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
					return err
				}
				result = transition.OperationReconciliation{Result: cloneOperationResult(record.Result), Resolved: true}
				return operationClaim(ctx, tx, lease, clock())
			}
			if assessment.Outcome != transition.OperationOutcomeSucceeded || !sameOperationResult(record.Result, assessment.Result) {
				record.EvidenceConflict = true
				record.History = append(record.History, transition.OperationHistoryEntry{Assessment: assessment, RecordedAt: now})
				if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
					return err
				}
				deferredErr = transition.ErrOperationEvidenceConflict
				return operationClaim(ctx, tx, lease, clock())
			}
			record.History = append(record.History, transition.OperationHistoryEntry{Assessment: assessment, RecordedAt: now})
			if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
				return err
			}
			result = transition.OperationReconciliation{Result: cloneOperationResult(record.Result), Resolved: true}
			return operationClaim(ctx, tx, lease, clock())
		}
		if record.Result != nil && record.Result.FailureCode != "" && assessment.Outcome == transition.OperationOutcomeSucceeded {
			record.EvidenceConflict = true
			record.History = append(record.History, transition.OperationHistoryEntry{Assessment: assessment, RecordedAt: now})
			if err := saveOperationRecord(ctx, tx, binding.Identity, record); err != nil {
				return err
			}
			deferredErr = transition.ErrOperationEvidenceConflict
			return operationClaim(ctx, tx, lease, clock())
		}
		oldActivityID := record.ActivityID
		sameClaim := oldActivityID == lease.ActivityID && record.Generation == lease.Generation
		if record.Result != nil {
			record.History = append(record.History, transition.OperationHistoryEntry{
				Assessment: transition.OperationAssessment{Outcome: transition.OperationOutcomeFailed, Result: cloneOperationResult(record.Result)},
				RecordedAt: now,
			})
		}
		record.History = append(record.History, transition.OperationHistoryEntry{Assessment: assessment, RecordedAt: now})
		record.ActivityID, record.Generation = lease.ActivityID, lease.Generation
		record.DispatchPending = sameClaim && record.DispatchPending
		switch assessment.Outcome {
		case transition.OperationOutcomeSucceeded:
			record.Result = cloneOperationResult(assessment.Result)
			record.DispatchPending = false
			result = transition.OperationReconciliation{Result: cloneOperationResult(record.Result), Resolved: true}
		case transition.OperationOutcomeFailed:
			if assessment.Repeat != nil && transition.OperationMayRepeat(binding, assessment, now) {
				record.Result = nil
				if !sameClaim {
					record.DispatchPending = true
					result.Dispatch = true
				} else {
					record.Result = cloneOperationResult(assessment.Result)
					record.DispatchPending = false
					result = transition.OperationReconciliation{Result: cloneOperationResult(record.Result), Resolved: true}
				}
			} else {
				record.Result = cloneOperationResult(assessment.Result)
				record.DispatchPending = false
				result = transition.OperationReconciliation{Result: cloneOperationResult(record.Result), Resolved: true}
			}
		case transition.OperationOutcomeAbsent, transition.OperationOutcomeUnknown:
			record.Result = nil
			if !sameClaim && transition.OperationMayRepeat(binding, assessment, now) {
				record.DispatchPending = true
				result.Dispatch = true
			}
		default:
			return transition.ErrCompatibility
		}
		data, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode reconciled operation: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = ? AND operation_key = ?`, data, binding.Identity.ExecutionID, []byte(binding.Identity.Key)); err != nil {
			return err
		}
		return operationClaim(ctx, tx, lease, clock())
	})
	if err != nil {
		return transition.OperationReconciliation{}, err
	}
	if deferredErr != nil {
		return transition.OperationReconciliation{}, deferredErr
	}
	return result, nil
}

// CompleteTrackedOperation persists success only under the dispatching hook fence.
func (s *Store[EntityID, StateID]) CompleteTrackedOperation(
	ctx context.Context, lease transition.HookLease, clock func() time.Time, identity transition.OperationIdentity, result transition.OperationResult,
) error {
	if clock == nil || identity.ExecutionID != lease.ExecutionID || identity.Key == "" {
		return transition.ErrInvalidRequest
	}
	return s.hookTransaction(ctx, func(tx *sql.Tx) error {
		if err := operationClaim(ctx, tx, lease, clock()); err != nil {
			return err
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, identity.ExecutionID, []byte(identity.Key)).Scan(&data); err != nil {
			return fmt.Errorf("read operation: %w", err)
		}
		record, err := decodeOperation(identity.ExecutionID, []byte(identity.Key), data)
		if err != nil {
			return err
		}
		if err := ensureOperationPrerequisites(ctx, tx, record.Binding); err != nil {
			return err
		}
		record.PrerequisiteBlocked = false
		if record.EvidenceConflict {
			return transition.ErrOperationEvidenceConflict
		}
		if record.ActivityID != lease.ActivityID || record.Generation != lease.Generation || record.Result != nil || !record.DispatchPending {
			return transition.ErrStaleActivityLease
		}
		if !transition.ValidOperationResult(result, record.Binding.ResultCodec) {
			return transition.ErrCompatibility
		}
		record.Result = &result
		record.DispatchPending = false
		outcome := transition.OperationOutcomeSucceeded
		if result.FailureCode != "" {
			outcome = transition.OperationOutcomeFailed
		}
		record.History = append(record.History, transition.OperationHistoryEntry{
			Assessment: transition.OperationAssessment{Outcome: outcome, Result: cloneOperationResult(&result)},
			RecordedAt: clock(),
		})
		data, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode operation result: %w", err)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = ? AND operation_key = ?`, data, identity.ExecutionID, []byte(identity.Key)); err != nil {
			return err
		}
		return operationClaim(ctx, tx, lease, clock())
	})
}

// RecordOperationResolution fences old hook claims and appends an authorized,
// idempotent operator decision without scheduling additional work.
func (s *Store[EntityID, StateID]) RecordOperationResolution(
	ctx context.Context,
	identity transition.OperationIdentity,
	requestID, principal, reason string,
	assessment transition.OperationAssessment,
	clock func() time.Time,
) (transition.OperationInspection, error) {
	if identity.ExecutionID == "" || identity.Key == "" || requestID == "" || principal == "" || reason == "" || clock == nil {
		return transition.OperationInspection{}, transition.ErrInvalidRequest
	}
	var inspection transition.OperationInspection
	var deferredErr error
	err := s.hookTransaction(ctx, func(tx *sql.Tx) error {
		now := clock()
		if now.IsZero() {
			return transition.ErrInvalidLease
		}
		var executionData []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_executions WHERE id = ?`, identity.ExecutionID).Scan(&executionData); err != nil {
			return fmt.Errorf("read operation execution: %w", err)
		}
		var execution diskRecord
		if err := json.Unmarshal(executionData, &execution); err != nil {
			return fmt.Errorf("%w: decode operation execution: %w", ErrUnsupportedSchema, err)
		}
		if execution.Status != transition.NeedsIntervention {
			return transition.ErrInvalidHook
		}
		var data []byte
		if err := tx.QueryRowContext(ctx, `SELECT record FROM rff_operations WHERE execution_id = ? AND operation_key = ?`, identity.ExecutionID, []byte(identity.Key)).Scan(&data); err != nil {
			return fmt.Errorf("read operation: %w", err)
		}
		record, err := decodeOperation(identity.ExecutionID, []byte(identity.Key), data)
		if err != nil {
			return err
		}
		if record.Binding.Identity != identity || !transition.OperationAssessmentValid(record.Binding, assessment, now) {
			return transition.ErrOperationUnresolved
		}
		if record.DeclarationOnly && assessment.Outcome != transition.OperationOutcomeAbsent {
			return transition.ErrOperationUnresolved
		}
		for _, entry := range record.History {
			if entry.RequestID != requestID {
				continue
			}
			if entry.Principal == principal && entry.Reason == reason && sameOperationAssessment(entry.Assessment, assessment) {
				inspection = operationInspection(record)
				return nil
			}
			return transition.ErrConflict
		}
		if record.EvidenceConflict {
			return transition.ErrOperationEvidenceConflict
		}
		if assessment.Outcome == transition.OperationOutcomeAbsent && !assessment.Evidence.Quiesced {
			return transition.ErrOperationUnresolved
		}
		for i := len(record.History) - 1; i >= 0; i-- {
			prior := record.History[i].Assessment.Evidence
			if prior.Identity != identity {
				continue
			}
			if prior.Outcome != transition.OperationOutcomeUnknown && prior.Outcome != assessment.Outcome {
				record.EvidenceConflict = true
				record.History = append(record.History, transition.OperationHistoryEntry{
					Assessment: assessment, RequestID: requestID, Principal: principal, Reason: reason, RecordedAt: now,
				})
				if err := saveOperationRecord(ctx, tx, identity, record); err != nil {
					return err
				}
				inspection = operationInspection(record)
				deferredErr = transition.ErrOperationEvidenceConflict
				return nil
			}
			break
		}
		if record.Result != nil {
			currentOutcome := transition.OperationOutcomeSucceeded
			if record.Result.FailureCode != "" {
				currentOutcome = transition.OperationOutcomeFailed
			}
			if currentOutcome != assessment.Outcome ||
				(assessment.Result != nil && !sameOperationResult(record.Result, assessment.Result)) {
				record.EvidenceConflict = true
				record.History = append(record.History, transition.OperationHistoryEntry{
					Assessment: assessment, RequestID: requestID, Principal: principal, Reason: reason, RecordedAt: now,
				})
				if err := saveOperationRecord(ctx, tx, identity, record); err != nil {
					return err
				}
				inspection = operationInspection(record)
				deferredErr = transition.ErrOperationEvidenceConflict
				return nil
			}
		}
		if record.Generation == ^uint64(0) {
			return transition.ErrInvalidLease
		}
		record.Generation++
		record.ActivityID = "operator:" + requestID
		record.DispatchPending = false
		if assessment.Outcome == transition.OperationOutcomeSucceeded || assessment.Outcome == transition.OperationOutcomeFailed {
			record.Result = cloneOperationResult(assessment.Result)
		}
		record.History = append(record.History, transition.OperationHistoryEntry{
			Assessment: assessment, RequestID: requestID, Principal: principal, Reason: reason, RecordedAt: now,
		})
		if err := saveOperationRecord(ctx, tx, identity, record); err != nil {
			return err
		}
		inspection = operationInspection(record)
		return nil
	})
	if err != nil {
		return transition.OperationInspection{}, err
	}
	if deferredErr != nil {
		return inspection, deferredErr
	}
	return inspection, nil
}

func sameOperationAssessment(a, b transition.OperationAssessment) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

func saveOperationRecord(ctx context.Context, tx *sql.Tx, identity transition.OperationIdentity, record operationRecord) error {
	record.Version = 3
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode operation resolution: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rff_operations SET record = ? WHERE execution_id = ? AND operation_key = ?`, data, identity.ExecutionID, []byte(identity.Key)); err != nil {
		return fmt.Errorf("save operation resolution: %w", err)
	}
	return nil
}

func ensureOperationsSettled(ctx context.Context, tx *sql.Tx, id string) error {
	rows, err := tx.QueryContext(ctx, `SELECT operation_key, record FROM rff_operations WHERE execution_id = ? ORDER BY operation_key`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key, data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return err
		}
		record, err := decodeOperation(id, key, data)
		if err != nil {
			return err
		}
		if record.EvidenceConflict {
			return transition.ErrOperationEvidenceConflict
		}
		if record.PrerequisiteBlocked {
			return transition.ErrOperationPrerequisiteUnresolved
		}
		if record.DeclarationOnly {
			continue
		}
		if !operationSettled(record) {
			return transition.ErrOperationUnresolved
		}
	}
	return rows.Err()
}

func ensureOperationPrerequisites(ctx context.Context, tx *sql.Tx, binding transition.OperationBinding) error {
	type node struct {
		record operationRecord
	}
	records := make(map[string]node)
	graph := make(map[string][]string)
	rows, err := tx.QueryContext(ctx, `SELECT operation_key, record FROM rff_operations WHERE execution_id = ? ORDER BY operation_key`, binding.Identity.ExecutionID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key, data []byte
		if err := rows.Scan(&key, &data); err != nil {
			rows.Close()
			return err
		}
		record, err := decodeOperation(binding.Identity.ExecutionID, key, data)
		if err != nil {
			rows.Close()
			return err
		}
		records[record.Binding.Identity.Key] = node{record: record}
		graph[record.Binding.Identity.Key] = record.Binding.Prerequisites
	}
	if err := rows.Close(); err != nil {
		return err
	}
	graph[binding.Identity.Key] = binding.Prerequisites
	if operationDependencyCycle(graph, binding.Identity.Key) {
		return transition.ErrOperationPrerequisiteCycle
	}
	for _, key := range binding.Prerequisites {
		dependency, ok := records[key]
		if !ok {
			return transition.ErrOperationPrerequisiteUnresolved
		}
		if dependency.record.EvidenceConflict {
			return transition.ErrOperationEvidenceConflict
		}
		if !operationSettled(dependency.record) {
			return transition.ErrOperationPrerequisiteUnresolved
		}
	}
	if binding.Compensation && binding.Prerequisites == nil {
		for key, operation := range records {
			if key == binding.Identity.Key {
				continue
			}
			if !operation.record.DeclarationOnly && !operationSettled(operation.record) {
				return transition.ErrOperationPrerequisiteUnresolved
			}
		}
	}
	return nil
}

func operationDependencyCycle(graph map[string][]string, root string) bool {
	state := make(map[string]uint8, len(graph))
	var visit func(string) bool
	visit = func(key string) bool {
		switch state[key] {
		case 1:
			return true
		case 2:
			return false
		}
		state[key] = 1
		for _, dependency := range graph[key] {
			if _, exists := graph[dependency]; exists && visit(dependency) {
				return true
			}
		}
		state[key] = 2
		return false
	}
	return visit(root)
}

func operationSettled(record operationRecord) bool {
	if record.EvidenceConflict || record.DeclarationOnly || record.DispatchPending {
		return false
	}
	if record.Result != nil {
		return true
	}
	for i := len(record.History) - 1; i >= 0; i-- {
		evidence := record.History[i].Assessment.Evidence
		if evidence.Identity == record.Binding.Identity {
			return evidence.Outcome == transition.OperationOutcomeAbsent && evidence.Quiesced
		}
	}
	return false
}

func isOperationPrerequisiteBlock(err error) bool {
	return errors.Is(err, transition.ErrOperationPrerequisiteUnresolved) ||
		errors.Is(err, transition.ErrOperationPrerequisiteCycle) ||
		errors.Is(err, transition.ErrOperationEvidenceConflict)
}

func operationPrerequisitesEqual(left, right []string) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return slices.Equal(left, right)
}

func loadOperationInspections(ctx context.Context, tx *sql.Tx, id string) ([]transition.OperationInspection, error) {
	rows, err := tx.QueryContext(ctx, `SELECT operation_key, record FROM rff_operations WHERE execution_id = ? ORDER BY operation_key`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var operations []transition.OperationInspection
	for rows.Next() {
		var key, data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return nil, err
		}
		record, err := decodeOperation(id, key, data)
		if err != nil {
			return nil, err
		}
		operations = append(operations, operationInspection(record))
	}
	return operations, rows.Err()
}
