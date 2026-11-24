package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type providerInput struct {
	PlanID    string `json:"plan_id"`
	PeriodID  string `json:"period_id"`
	Amount    int64  `json:"amount_cents"`
	Kind      string `json:"kind"`
	ChargeKey string `json:"charge_key,omitempty"`
}

type providerReceipt struct {
	ProviderKey string `json:"provider_key"`
	Kind        string `json:"kind"`
	PlanID      string `json:"plan_id"`
	PeriodID    string `json:"period_id"`
	Amount      int64  `json:"amount_cents"`
}

type providerGuaranteeMode string

const (
	providerGuaranteeValid    providerGuaranteeMode = "valid"
	providerGuaranteeShort    providerGuaranteeMode = "short"
	providerGuaranteeExpired  providerGuaranteeMode = "expired"
	providerGuaranteeMismatch providerGuaranteeMode = "mismatch"
)

func (h *Host) SetProviderGuarantee(ctx context.Context, plan string, mode providerGuaranteeMode) error {
	switch mode {
	case providerGuaranteeValid, providerGuaranteeShort, providerGuaranteeExpired, providerGuaranteeMismatch:
	default:
		return transition.ErrInvalidRequest
	}
	_, err := h.db.ExecContext(ctx, `INSERT INTO renewal_provider_guarantees (plan_id, guarantee_mode) VALUES (?, ?)`, plan, mode)
	return err
}

func (h *Host) charge(ctx context.Context, execution fsm.ExecutionContext[string, State, Event, Input]) error {
	input := execution.Input()
	if handled, err := h.chargeBatch(ctx, input.PlanID, execution); handled || err != nil {
		return err
	}
	opInput := providerInput{PlanID: input.PlanID, PeriodID: input.PeriodID, Amount: amountCents, Kind: "charge"}
	encoded, err := json.Marshal(opInput)
	if err != nil {
		return err
	}
	request := h.operationRequest("charge", opInput, encoded, false, nil)
	result, err := transition.TrackOperation(ctx, request, h.dispatchProvider)
	if err != nil {
		return err
	}
	return validateReceipt(result, opInput, "charge")
}

func (h *Host) refund(ctx context.Context, execution fsm.ExecutionContext[string, State, Event, Input]) error {
	input := execution.Input()
	rows, err := h.db.QueryContext(ctx, `SELECT record_id FROM renewal_batch_records WHERE batch_id = ? ORDER BY ordinal`, input.PlanID)
	if err != nil {
		return err
	}
	var members []string
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			_ = rows.Close()
			return err
		}
		members = append(members, member)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return h.refundRecord(ctx, execution.Attempt().ExecutionID, input.PlanID, input.PeriodID, "charge", "refund")
	}
	for _, member := range members {
		chargeKey := "charge:" + member
		_, err := h.store.InspectTrackedOperation(ctx, transition.OperationIdentity{ExecutionID: execution.Attempt().ExecutionID, Key: chargeKey})
		if errors.Is(err, transition.ErrOperationNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := h.refundRecord(ctx, execution.Attempt().ExecutionID, member, input.PeriodID, chargeKey, "refund:"+member); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) refundRecord(ctx context.Context, executionID, plan, period, chargeKey, refundKey string) error {
	chargeIdentity := transition.OperationIdentity{ExecutionID: executionID, Key: chargeKey}
	opInput := providerInput{
		PlanID: plan, PeriodID: period, Amount: amountCents,
		Kind: "refund", ChargeKey: providerKey(chargeIdentity),
	}
	encoded, err := json.Marshal(opInput)
	if err != nil {
		return err
	}
	request := h.operationRequest(refundKey, opInput, encoded, true, []string{chargeKey})
	result, err := transition.TrackOperation(ctx, request, h.dispatchProvider)
	if err != nil {
		return err
	}
	return validateReceipt(result, opInput, "refund")
}

func (h *Host) operationRequest(key string, input providerInput, encoded []byte, compensation bool, prerequisites []string) transition.OperationRequest {
	return transition.OperationRequest{
		Key: key, Kind: input.Kind, BehaviorVersion: "durable-fake-provider-v1",
		ComparatorVersion: "canonical-json-v1", Compensation: compensation,
		Prerequisites: prerequisites, Input: encoded,
		ResultCodec: transition.CodecVersion{Name: "renewal-provider-receipt", Version: "1"},
		Preflight:   func(context.Context) error { return nil },
		ValidateInput: func(_ context.Context, data []byte) error {
			var decoded providerInput
			if err := json.Unmarshal(data, &decoded); err != nil {
				return err
			}
			if decoded != input {
				return transition.ErrConflict
			}
			return nil
		},
		EqualInput: func(_ context.Context, first, second []byte) (bool, error) {
			return bytes.Equal(first, second), nil
		},
		ValidateResult: func(_ context.Context, result transition.OperationResult) error {
			return validateReceipt(result, input, input.Kind)
		},
		ClassifyDispatch: func(ctx context.Context, identity transition.OperationIdentity, _ error) (transition.OperationAssessment, error) {
			inspection, err := h.store.InspectTrackedOperation(ctx, identity)
			if err != nil {
				return transition.OperationAssessment{}, err
			}
			return h.unknownAssessment(identity, inspection.Binding), nil
		},
		Reconcile: func(ctx context.Context, identity transition.OperationIdentity, binding transition.OperationBinding, _ transition.OperationInspection) (transition.OperationAssessment, error) {
			assessment, err := h.lookupAssessment(ctx, identity, binding)
			if err != nil || assessment.Outcome != transition.OperationOutcomeUnknown {
				return assessment, err
			}
			return h.attachProviderGuarantee(ctx, input.PlanID, assessment)
		},
		VerifyEvidence:   h.verifyEvidence,
		VerifySafeRepeat: h.verifyProviderGuarantee,
	}
}

func (h *Host) attachProviderGuarantee(ctx context.Context, plan string, assessment transition.OperationAssessment) (transition.OperationAssessment, error) {
	var mode providerGuaranteeMode
	err := h.db.QueryRowContext(ctx, `SELECT guarantee_mode FROM renewal_provider_guarantees WHERE plan_id = ?`, plan).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		return assessment, nil
	}
	if err != nil {
		return assessment, err
	}
	evidence := assessment.Evidence
	now := h.clock()
	guarantee := &transition.SafeRepeatGuarantee{
		Identity: evidence.Identity, Provider: evidence.Provider, Scope: evidence.Scope,
		Reference: evidence.Reference, InputFingerprint: evidence.InputFingerprint,
		Version: "durable-fake-idempotency-v1", BehaviorVersion: evidence.BehaviorVersion,
		ComparatorVersion: evidence.ComparatorVersion, ValidFrom: now.Add(-time.Minute),
		ValidUntil: now.Add(2 * time.Minute), OverlapUntil: now.Add(time.Minute),
	}
	switch mode {
	case providerGuaranteeShort:
		guarantee.ValidUntil = now.Add(30 * time.Second)
	case providerGuaranteeExpired:
		guarantee.ValidUntil = now.Add(-time.Second)
	case providerGuaranteeMismatch:
		guarantee.InputFingerprint = "different-operation"
	case providerGuaranteeValid:
	default:
		return assessment, transition.ErrCompatibility
	}
	if _, err := h.db.ExecContext(ctx, `UPDATE renewal_provider_guarantees SET offers = offers + 1 WHERE plan_id = ?`, plan); err != nil {
		return assessment, err
	}
	assessment.Repeat = guarantee
	return assessment, nil
}

func (h *Host) verifyProviderGuarantee(ctx context.Context, binding transition.OperationBinding, assessment transition.OperationAssessment, _ time.Time) error {
	var input providerInput
	if err := json.Unmarshal(binding.Input, &input); err != nil {
		return transition.ErrCompatibility
	}
	var mode providerGuaranteeMode
	if err := h.db.QueryRowContext(ctx, `SELECT guarantee_mode FROM renewal_provider_guarantees WHERE plan_id = ?`, input.PlanID).Scan(&mode); err != nil {
		return err
	}
	if (mode != providerGuaranteeValid && mode != providerGuaranteeShort && mode != providerGuaranteeExpired && mode != providerGuaranteeMismatch) ||
		assessment.Repeat == nil || assessment.Repeat.Version != "durable-fake-idempotency-v1" {
		return transition.ErrCompatibility
	}
	return nil
}

func validateReceipt(result transition.OperationResult, input providerInput, kind string) error {
	if !transition.ValidOperationResult(result, transition.CodecVersion{Name: "renewal-provider-receipt", Version: "1"}) || result.FailureCode != "" || result.NoResult {
		return transition.ErrCompatibility
	}
	var receipt providerReceipt
	if err := json.Unmarshal(result.Data, &receipt); err != nil {
		return transition.ErrCompatibility
	}
	if receipt.ProviderKey == "" || receipt.Kind != kind || receipt.PlanID != input.PlanID ||
		receipt.PeriodID != input.PeriodID || receipt.Amount != input.Amount {
		return transition.ErrConflict
	}
	return nil
}

func (h *Host) dispatchProvider(ctx context.Context, identity transition.OperationIdentity) (transition.OperationResult, error) {
	inspection, err := h.store.InspectTrackedOperation(ctx, identity)
	if err != nil {
		return transition.OperationResult{}, err
	}
	var input providerInput
	if err := json.Unmarshal(inspection.Binding.Input, &input); err != nil {
		return transition.OperationResult{}, transition.ErrCompatibility
	}
	dispatchID, err := randomID()
	if err != nil {
		return transition.OperationResult{}, err
	}
	if _, err := h.db.ExecContext(ctx, `INSERT INTO renewal_provider_dispatches (dispatch_id, plan_id, period_id) VALUES (?, ?, ?)`, dispatchID, input.PlanID, input.PeriodID); err != nil {
		return transition.OperationResult{}, err
	}
	key := providerKey(identity)
	fingerprint := hashHex(inspection.Binding.Input)
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return transition.OperationResult{}, err
	}
	var priorStatus string
	var priorResult []byte
	err = tx.QueryRowContext(ctx, `SELECT status, result FROM renewal_provider_ops WHERE provider_key = ?`, key).Scan(&priorStatus, &priorResult)
	if err == nil {
		_ = tx.Rollback()
		if priorStatus == "succeeded" {
			return operationResult(priorResult), nil
		}
		return transition.OperationResult{}, errLostProviderResponse
	}
	if !errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return transition.OperationResult{}, err
	}
	mode := providerNormal
	var consumed int
	if input.Kind == "charge" {
		var modeText string
		if err := tx.QueryRowContext(ctx, `SELECT mode, consumed FROM renewal_provider_settings WHERE plan_id = ?`, input.PlanID).Scan(&modeText, &consumed); err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
		mode = providerMode(modeText)
		if consumed != 0 && (mode == providerLostResponse || mode == providerLateCall) {
			mode = providerNormal
		}
		if _, err := tx.ExecContext(ctx, `UPDATE renewal_provider_settings SET consumed = 1 WHERE plan_id = ?`, input.PlanID); err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
	}
	if input.Kind == "charge" {
		if err := h.claimPeriodEffect(ctx, tx, input.PlanID, input.PeriodID, key); err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
	}
	created := h.clock()
	status := "pending"
	var encodedResult []byte
	if mode != providerLateCall {
		receipt := providerReceipt{ProviderKey: key, Kind: input.Kind, PlanID: input.PlanID, PeriodID: input.PeriodID, Amount: input.Amount}
		encodedResult, err = json.Marshal(receipt)
		if err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
		status = "succeeded"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_provider_ops (provider_key, execution_id, operation_key, kind, plan_id, period_id, fingerprint, input, status, result, related_key, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		key, identity.ExecutionID, identity.Key, input.Kind, input.PlanID, input.PeriodID, fingerprint, inspection.Binding.Input, status, nullable(encodedResult), input.ChargeKey, created); err != nil {
		_ = tx.Rollback()
		return transition.OperationResult{}, err
	}
	if input.Kind == "charge" && mode == providerLateCall {
		if _, err := tx.ExecContext(ctx, `UPDATE renewal_period_effects SET status = 'in-flight' WHERE plan_id = ? AND period_id = ? AND provider_key = ?`, input.PlanID, input.PeriodID, key); err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
	}
	if input.Kind == "charge" && mode != providerLateCall {
		if _, err := tx.ExecContext(ctx, `UPDATE renewal_period_effects SET status = 'charged' WHERE plan_id = ? AND period_id = ? AND provider_key = ?`, input.PlanID, input.PeriodID, key); err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
	}
	if input.Kind == "refund" && status == "succeeded" {
		if err := h.settleRefund(ctx, tx, input, key); err != nil {
			_ = tx.Rollback()
			return transition.OperationResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return transition.OperationResult{}, err
	}
	if mode == providerLostResponse || mode == providerLateCall {
		return transition.OperationResult{}, errLostProviderResponse
	}
	return operationResult(encodedResult), nil
}

func (h *Host) claimPeriodEffect(ctx context.Context, tx *sql.Tx, plan, period, key string) error {
	var existingKey, status string
	err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT provider_key, status FROM renewal_period_effects WHERE plan_id = ? AND period_id = ?`),
		plan, period).Scan(&existingKey, &status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO renewal_period_effects (plan_id, period_id, provider_key, status) VALUES (?, ?, ?, 'in-flight')`, plan, period, key)
		return err
	case err != nil:
		return err
	case existingKey == key:
		return nil
	case status == "refunded":
		_, err = tx.ExecContext(ctx, `UPDATE renewal_period_effects SET provider_key = ?, status = 'in-flight' WHERE plan_id = ? AND period_id = ? AND provider_key = ? AND status = 'refunded'`,
			key, plan, period, existingKey)
		return err
	default:
		return errors.New("plan period already has a charged or unresolved payment")
	}
}

func (h *Host) settleRefund(ctx context.Context, tx *sql.Tx, input providerInput, refundKey string) error {
	var status, currentKey string
	if err := tx.QueryRowContext(ctx, `SELECT status, provider_key FROM renewal_period_effects WHERE plan_id = ? AND period_id = ?`, input.PlanID, input.PeriodID).Scan(&status, &currentKey); err != nil {
		return err
	}
	if status != "charged" || currentKey != input.ChargeKey {
		return transition.ErrOperationPrerequisiteUnresolved
	}
	_, err := tx.ExecContext(ctx, `UPDATE renewal_period_effects SET status = 'refunded' WHERE plan_id = ? AND period_id = ? AND provider_key = ? AND status = 'charged'`,
		input.PlanID, input.PeriodID, input.ChargeKey)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE renewal_provider_ops SET related_key = ? WHERE provider_key = ?`, input.ChargeKey, refundKey)
	return err
}

func (h *Host) lookupAssessment(ctx context.Context, identity transition.OperationIdentity, binding transition.OperationBinding) (transition.OperationAssessment, error) {
	var result []byte
	var status string
	err := h.db.QueryRowContext(ctx, `SELECT status, result FROM renewal_provider_ops WHERE provider_key = ?`, providerKey(identity)).Scan(&status, &result)
	if errors.Is(err, sql.ErrNoRows) || status == "pending" {
		return h.unknownAssessment(identity, binding), nil
	}
	if err != nil {
		return transition.OperationAssessment{}, err
	}
	if status != "succeeded" {
		return h.unknownAssessment(identity, binding), nil
	}
	assessment := h.unknownAssessment(identity, binding)
	assessment.Outcome = transition.OperationOutcomeSucceeded
	assessment.Evidence.Outcome = transition.OperationOutcomeSucceeded
	assessment.Evidence.Provenance = "durable-provider-ledger"
	assessment.Result = ptrOperationResult(operationResult(result))
	return assessment, nil
}

func (h *Host) unknownAssessment(identity transition.OperationIdentity, binding transition.OperationBinding) transition.OperationAssessment {
	now := h.clock()
	return transition.OperationAssessment{
		Outcome: transition.OperationOutcomeUnknown,
		Evidence: transition.OperationEvidence{
			Identity: identity, Outcome: transition.OperationOutcomeUnknown,
			Provider: "durable-fake-provider", Scope: "plan-period",
			Reference: providerKey(identity), InputFingerprint: hashHex(binding.Input),
			Version: "1", Provenance: "dispatch-may-still-arrive",
			BehaviorVersion: binding.BehaviorVersion, ComparatorVersion: binding.ComparatorVersion,
			ObservedAt: now, ValidUntil: now.Add(24 * time.Hour),
		},
	}
}

func (h *Host) verifyEvidence(ctx context.Context, binding transition.OperationBinding, evidence transition.OperationEvidence) error {
	if evidence.Identity != binding.Identity || evidence.Provider != "durable-fake-provider" ||
		evidence.Reference != providerKey(binding.Identity) || evidence.Scope != "plan-period" ||
		evidence.InputFingerprint != hashHex(binding.Input) || evidence.Version != "1" ||
		evidence.BehaviorVersion != binding.BehaviorVersion || evidence.ComparatorVersion != binding.ComparatorVersion {
		return transition.ErrCompatibility
	}
	var status string
	var fingerprint string
	err := h.db.QueryRowContext(ctx, `SELECT status, fingerprint FROM renewal_provider_ops WHERE provider_key = ?`, evidence.Reference).Scan(&status, &fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return transition.ErrCompatibility
	}
	if err != nil {
		return err
	}
	if fingerprint != evidence.InputFingerprint ||
		evidence.Outcome == transition.OperationOutcomeSucceeded && status != "succeeded" ||
		evidence.Outcome == transition.OperationOutcomeUnknown && status != "pending" && status != "succeeded" {
		return transition.ErrCompatibility
	}
	return nil
}

func (h *Host) ApplyLateProviderCall(ctx context.Context, plan, period string) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	query := `SELECT provider_key, input FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'charge' AND status = 'pending' ORDER BY created_at DESC LIMIT 1`
	if h.dialect == "mysql" {
		query += ` FOR UPDATE`
	}
	var key string
	var inputData []byte
	if err := tx.QueryRowContext(ctx, query, plan, period).Scan(&key, &inputData); err != nil {
		_ = tx.Rollback()
		return err
	}
	var input providerInput
	if err := json.Unmarshal(inputData, &input); err != nil {
		_ = tx.Rollback()
		return transition.ErrCompatibility
	}
	result, err := json.Marshal(providerReceipt{
		ProviderKey: key, Kind: "charge", PlanID: input.PlanID,
		PeriodID: input.PeriodID, Amount: input.Amount,
	})
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE renewal_provider_ops SET status = 'succeeded', result = ? WHERE provider_key = ? AND status = 'pending'`, result, key); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE renewal_period_effects SET status = 'charged' WHERE plan_id = ? AND period_id = ? AND provider_key = ? AND status = 'in-flight'`, plan, period, key); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (h *Host) operationResult(ctx context.Context, identity transition.OperationIdentity) (transition.OperationResult, error) {
	inspection, err := h.store.InspectTrackedOperation(ctx, identity)
	if err != nil {
		return transition.OperationResult{}, err
	}
	assessment, err := h.lookupAssessment(ctx, identity, inspection.Binding)
	if err != nil {
		return transition.OperationResult{}, err
	}
	if assessment.Outcome != transition.OperationOutcomeSucceeded || assessment.Result == nil {
		return transition.OperationResult{}, transition.ErrOperationUnresolved
	}
	return *assessment.Result, nil
}

func (h *Host) InspectCharge(ctx context.Context, plan, period, principal string) (transition.OperationInspection, error) {
	if principal != "operator" {
		return transition.OperationInspection{}, errors.New("operation inspection denied")
	}
	var executionID, operationKey string
	err := h.db.QueryRowContext(ctx, `SELECT execution_id, operation_key FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'charge' ORDER BY created_at DESC LIMIT 1`,
		plan, period).Scan(&executionID, &operationKey)
	if err != nil {
		return transition.OperationInspection{}, err
	}
	return h.store.InspectTrackedOperation(ctx, transition.OperationIdentity{ExecutionID: executionID, Key: operationKey})
}

func (h *Host) ResolveCharge(ctx context.Context, plan, actor string) error {
	if actor != "operator" {
		return transition.ErrOperationResolutionDenied
	}
	execution, err := h.activePayment(ctx, plan)
	if err != nil {
		return err
	}
	key := "charge"
	var recordID string
	err = h.db.QueryRowContext(ctx, `SELECT record_id FROM renewal_batch_records WHERE batch_id = ? AND status = 'pending' ORDER BY ordinal LIMIT 1`, plan).Scan(&recordID)
	if err == nil {
		key = "charge:" + recordID
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	identity := transition.OperationIdentity{ExecutionID: execution.ID, Key: key}
	inspection, err := h.store.InspectTrackedOperation(ctx, identity)
	if err != nil {
		return err
	}
	assessment, err := h.lookupAssessment(ctx, identity, inspection.Binding)
	if err != nil {
		return err
	}
	if assessment.Outcome != transition.OperationOutcomeSucceeded {
		return transition.ErrOperationUnresolved
	}
	_, err = h.resolver.Resolve(ctx, transition.OperationResolutionRequest{
		ID: "resolve:" + execution.ID + ":" + key, Identity: identity, Assessment: assessment,
		VerifyEvidence: h.verifyEvidence,
		ValidateResult: func(_ context.Context, binding transition.OperationBinding, result transition.OperationResult) error {
			var input providerInput
			if err := json.Unmarshal(binding.Input, &input); err != nil {
				return err
			}
			return validateReceipt(result, input, "charge")
		},
	})
	return err
}

func (h *Host) activePayment(ctx context.Context, plan string) (transition.Execution[string, State], error) {
	ids, err := h.store.ActiveExecutionIDs(ctx)
	if err != nil {
		return transition.Execution[string, State]{}, err
	}
	for _, id := range ids {
		execution, err := h.store.ReadActiveExecution(ctx, id)
		if err == nil && execution.Entity == plan && execution.Target == PaymentCaptured {
			return execution, nil
		}
	}
	return transition.Execution[string, State]{}, transition.ErrExecutionNotActive
}

func (h *Host) Retry(ctx context.Context, plan, actor, requestID string) error {
	return h.control(ctx, plan, actor, requestID, transition.RetryIntervention)
}

func (h *Host) Abort(ctx context.Context, plan, actor, requestID string) error {
	return h.control(ctx, plan, actor, requestID, transition.AbortIntervention)
}

func (h *Host) control(ctx context.Context, plan, actor, requestID string, action transition.HookControlAction) error {
	if actor != "operator" || plan == "" || requestID == "" {
		return transition.ErrInvalidRequest
	}
	var storedPlan, storedActor, storedAction, executionID string
	readRequest := func() error {
		return h.db.QueryRowContext(ctx, `SELECT plan_id, principal, action, execution_id FROM renewal_control_requests WHERE request_id = ?`, requestID).
			Scan(&storedPlan, &storedActor, &storedAction, &executionID)
	}
	err := readRequest()
	if errors.Is(err, sql.ErrNoRows) {
		execution, activeErr := h.activePayment(ctx, plan)
		if activeErr != nil {
			if err := readRequest(); err != nil {
				return activeErr
			}
		} else {
			_, insertErr := h.db.ExecContext(ctx, `INSERT INTO renewal_control_requests (request_id, plan_id, principal, action, execution_id) VALUES (?, ?, ?, ?, ?)`,
				requestID, plan, actor, action, execution.ID)
			if insertErr != nil && !isDuplicate(insertErr) {
				return insertErr
			}
		}
		err = readRequest()
	}
	if err != nil {
		return err
	}
	if storedPlan != plan || storedActor != actor || storedAction != string(action) {
		return transition.ErrConflict
	}
	inspection, err := h.store.InspectExecution(ctx, executionID)
	if err != nil {
		return err
	}
	for _, receipt := range inspection.Controls {
		if receipt.Control.ID == requestID {
			if receipt.Control.Action != action {
				return transition.ErrConflict
			}
			return nil
		}
	}
	lease, err := h.store.AcquireTransitionLease(ctx, plan, executionID, "renewal-host", h.clock, 5*time.Second)
	if err != nil {
		return err
	}
	_, err = h.controls.Hook(ctx, lease, h.clock, transition.HookControl{ID: requestID, Action: action})
	return err
}

func (h *Host) InspectControl(ctx context.Context, plan, actor, requestID string) (transition.HookControlReceipt, error) {
	if actor != "operator" || plan == "" || requestID == "" {
		return transition.HookControlReceipt{}, transition.ErrInvalidRequest
	}
	var storedPlan, storedActor, executionID string
	if err := h.db.QueryRowContext(ctx, `SELECT plan_id, principal, execution_id FROM renewal_control_requests WHERE request_id = ?`, requestID).
		Scan(&storedPlan, &storedActor, &executionID); err != nil {
		return transition.HookControlReceipt{}, err
	}
	if storedPlan != plan || storedActor != actor {
		return transition.HookControlReceipt{}, transition.ErrConflict
	}
	inspection, err := h.store.InspectExecution(ctx, executionID)
	if err != nil {
		return transition.HookControlReceipt{}, err
	}
	for _, receipt := range inspection.Controls {
		if receipt.Control.ID == requestID {
			return receipt, nil
		}
	}
	return transition.HookControlReceipt{}, transition.ErrInvalidHook
}

func (h *Host) Reattempt(ctx context.Context, plan, period, actor string) (string, error) {
	if actor != "operator" {
		return "", transition.ErrInvalidRequest
	}
	if err := h.checkPlan(ctx, plan, period, Due); err != nil {
		return "", err
	}
	var status, refundKey string
	if err := h.db.QueryRowContext(ctx, `SELECT status FROM renewal_period_effects WHERE plan_id = ? AND period_id = ?`, plan, period).Scan(&status); err != nil || status != "refunded" {
		return "", errors.New("a confirmed refund is required before another attempt")
	}
	if err := h.db.QueryRowContext(ctx, `SELECT provider_key FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'refund' AND status = 'succeeded' ORDER BY created_at DESC LIMIT 1`,
		plan, period).Scan(&refundKey); err != nil {
		return "", err
	}
	execution, _, err := h.acceptor.Accept(ctx, transition.Request[string, State, Event, Input]{
		Entity: plan, Key: "retry-capture:" + period + ":" + refundKey,
		Operation: transition.Transition, MachineID: machineID, MachineVersion: machineVersion,
		Previous: Due, HasPrevious: true, Event: RetryCapture, HasEvent: true,
		EventCodec: (jsonCodec[Event]{version: "1"}).Version(),
		Input:      Input{PlanID: plan, PeriodID: period, Principal: actor},
		InputCodec: (jsonCodec[Input]{version: "1"}).Version(), Notification: transition.NotifyDisable,
	})
	return execution.ID, err
}

type Summary struct {
	PlanID          string
	PeriodID        string
	State           State
	History         []State
	Revision        int64
	Charges         int
	Refunds         int
	ProviderCalls   int
	GuaranteeOffers int
	ActiveStatus    transition.Status
	FailureCode     string
	Continuations   []transition.ContinuationPhase
}

func (h *Host) Inspect(ctx context.Context, plan, period, actor string) (Summary, error) {
	if actor != "operator" {
		return Summary{}, errors.New("inspection denied")
	}
	var summary Summary
	summary.PlanID, summary.PeriodID = plan, period
	if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_provider_dispatches WHERE plan_id = ? AND period_id = ?`, plan, period).Scan(&summary.ProviderCalls); err != nil {
		return summary, err
	}
	if err := h.db.QueryRowContext(ctx, `SELECT COALESCE((SELECT offers FROM renewal_provider_guarantees WHERE plan_id = ?), 0)`, plan).Scan(&summary.GuaranteeOffers); err != nil {
		return summary, err
	}
	var current string
	if err := h.db.QueryRowContext(ctx, `SELECT state, revision FROM renewal_plans WHERE plan_id = ? AND period_id = ?`, plan, period).Scan(&current, &summary.Revision); err != nil {
		return summary, err
	}
	summary.State = State(current)
	rows, err := h.db.QueryContext(ctx, `SELECT state FROM renewal_commit_receipts WHERE plan_id = ? ORDER BY revision`, plan)
	if err != nil {
		return summary, err
	}
	for rows.Next() {
		var committed string
		if err := rows.Scan(&committed); err != nil {
			_ = rows.Close()
			return summary, err
		}
		summary.History = append(summary.History, State(committed))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return summary, err
	}
	_ = rows.Close()
	summary.Charges, summary.Refunds, err = h.providerCounts(ctx, plan, period)
	if err != nil {
		return summary, err
	}
	active, err := h.store.ActiveExecutionIDs(ctx)
	if err != nil {
		return summary, err
	}
	for _, id := range active {
		execution, err := h.store.ReadActiveExecution(ctx, id)
		if err != nil || execution.Entity != plan {
			continue
		}
		inspection, err := h.store.InspectExecution(ctx, id)
		if err != nil {
			return summary, err
		}
		summary.ActiveStatus = inspection.Status
		if inspection.Failure != nil {
			summary.FailureCode = inspection.Failure.Code
		}
	}
	markers, err := h.store.ContinuationMarkers(ctx)
	if err != nil {
		return summary, err
	}
	for _, marker := range markers {
		if marker.Entity != plan {
			continue
		}
		inspection, err := h.continuationControls.Inspect(ctx, marker.ExecutionID)
		if err != nil {
			return summary, err
		}
		summary.Continuations = append(summary.Continuations, inspection.Phase)
	}
	return summary, nil
}

func providerKey(identity transition.OperationIdentity) string {
	return hashHex([]byte(identity.ExecutionID + "\x00" + identity.Key))
}

func operationResult(data []byte) transition.OperationResult {
	return transition.OperationResult{
		Codec: transition.CodecVersion{Name: "renewal-provider-receipt", Version: "1"},
		Data:  append([]byte(nil), data...),
	}
}

func ptrOperationResult(result transition.OperationResult) *transition.OperationResult {
	return &result
}

func nullable(data []byte) any {
	if data == nil {
		return nil
	}
	return data
}

func (h *Host) providerCounts(ctx context.Context, plan, period string) (charges, refunds int, err error) {
	err = h.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN kind = 'charge' AND status = 'succeeded' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN kind = 'refund' AND status = 'succeeded' THEN 1 ELSE 0 END), 0)
		FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ?`, plan, period).Scan(&charges, &refunds)
	return
}

func (h *Host) paymentOutcome(ctx context.Context, executionID string) (transition.OperationInspection, error) {
	return h.store.InspectTrackedOperation(ctx, transition.OperationIdentity{ExecutionID: executionID, Key: "charge"})
}

func safeProviderError(err error) string {
	if errors.Is(err, transition.ErrOperationUnresolved) {
		return "unknown"
	}
	if err == nil {
		return "confirmed"
	}
	return fmt.Sprintf("failed:%T", err)
}
