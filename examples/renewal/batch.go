package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type BatchRecordProgress struct {
	Ordinal         int
	RecordID        string
	Complete        bool
	Effects         int
	Refunds         int
	OperationStatus string
}

type BatchProgress struct {
	Cursor  int
	Records []BatchRecordProgress
}

type FanoutRecordProgress struct {
	Ordinal     int
	EntityID    string
	ExecutionID string
	Complete    bool
	Effects     int
}

type FanoutProgress struct {
	Cursor  int
	Records []FanoutRecordProgress
}

type batchRecord struct {
	ordinal  int
	recordID string
	complete bool
}

type fanoutRecord struct {
	ordinal     int
	recordID    string
	executionID string
	complete    bool
}

func (h *Host) StartBatch(ctx context.Context, batchID, period string, recordIDs []string) (string, error) {
	executionID, err := h.startBatch(ctx, batchID, period, recordIDs)
	if isRecordClaimConflict(err) {
		return "", transition.ErrConflict
	}
	return executionID, err
}

func (h *Host) startBatch(ctx context.Context, batchID, period string, recordIDs []string) (string, error) {
	if batchID == "" || period == "" || len(recordIDs) == 0 {
		return "", transition.ErrInvalidRequest
	}
	seen := make(map[string]struct{}, len(recordIDs))
	for _, id := range recordIDs {
		if id == "" || id == batchID {
			return "", transition.ErrInvalidRequest
		}
		if _, exists := seen[id]; exists {
			return "", transition.ErrConflict
		}
		seen[id] = struct{}{}
	}

	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	var storedPeriod string
	err = tx.QueryRowContext(ctx, h.lockQuery(`SELECT period_id FROM renewal_batch_runs WHERE batch_id = ?`), batchID).Scan(&storedPeriod)
	switch {
	case err == nil:
		if storedPeriod != period {
			_ = tx.Rollback()
			return "", transition.ErrConflict
		}
		rows, queryErr := tx.QueryContext(ctx, `SELECT record_id FROM renewal_batch_records WHERE batch_id = ? ORDER BY ordinal`, batchID)
		if queryErr != nil {
			_ = tx.Rollback()
			return "", queryErr
		}
		var existing []string
		for rows.Next() {
			var id string
			if queryErr = rows.Scan(&id); queryErr != nil {
				break
			}
			existing = append(existing, id)
		}
		if queryErr == nil {
			queryErr = rows.Err()
		}
		_ = rows.Close()
		if queryErr != nil {
			_ = tx.Rollback()
			return "", queryErr
		}
		if len(existing) != len(recordIDs) {
			_ = tx.Rollback()
			return "", transition.ErrConflict
		}
		for index := range recordIDs {
			if existing[index] != recordIDs[index] {
				_ = tx.Rollback()
				return "", transition.ErrConflict
			}
		}
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_batch_runs (batch_id, period_id, cursor_ordinal) VALUES (?, ?, 0)`, batchID, period); err != nil {
			_ = tx.Rollback()
			return "", err
		}
		for index, id := range recordIDs {
			var ownerKind, ownerID string
			err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT owner_kind, owner_id FROM renewal_record_claims WHERE record_id = ? AND period_id = ?`),
				id, period).Scan(&ownerKind, &ownerID)
			if err == nil && (ownerKind != "batch" || ownerID != batchID) {
				_ = tx.Rollback()
				return "", transition.ErrConflict
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				_ = tx.Rollback()
				return "", err
			}
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_record_claims (record_id, period_id, owner_kind, owner_id) VALUES (?, ?, 'batch', ?)`, id, period, batchID); err != nil {
					_ = tx.Rollback()
					return "", err
				}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_batch_records (batch_id, period_id, ordinal, record_id, status) VALUES (?, ?, ?, ?, 'pending')`,
				batchID, period, index+1, id); err != nil {
				_ = tx.Rollback()
				return "", err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_provider_settings (plan_id, mode, consumed) VALUES (?, ?, 0)`, id, providerNormal); err != nil && !isDuplicate(err) {
				_ = tx.Rollback()
				return "", err
			}
		}
	default:
		_ = tx.Rollback()
		return "", err
	}
	if err := h.claimRecord(ctx, tx, batchID, period, "batch", batchID); err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return h.startOwned(ctx, batchID, period, "batch", batchID)
}

func (h *Host) claimRecord(ctx context.Context, tx *sql.Tx, record, period, kind, owner string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO renewal_record_claims (record_id, period_id, owner_kind, owner_id) VALUES (?, ?, ?, ?)`, record, period, kind, owner)
	if err == nil {
		return nil
	}
	if !isDuplicate(err) {
		return err
	}
	var storedKind, storedOwner string
	if err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT owner_kind, owner_id FROM renewal_record_claims WHERE record_id = ? AND period_id = ?`),
		record, period).Scan(&storedKind, &storedOwner); err != nil {
		return err
	}
	if storedKind != kind || storedOwner != owner {
		return transition.ErrConflict
	}
	return nil
}

func (h *Host) BatchProgress(ctx context.Context, batchID string) (BatchProgress, error) {
	var period string
	var cursor int
	if err := h.db.QueryRowContext(ctx, `SELECT period_id, cursor_ordinal FROM renewal_batch_runs WHERE batch_id = ?`, batchID).Scan(&period, &cursor); err != nil {
		return BatchProgress{}, err
	}
	rows, err := h.db.QueryContext(ctx, `SELECT ordinal, record_id, status FROM renewal_batch_records WHERE batch_id = ? ORDER BY ordinal`, batchID)
	if err != nil {
		return BatchProgress{}, err
	}
	var records []batchRecord
	for rows.Next() {
		var record batchRecord
		var status string
		if err := rows.Scan(&record.ordinal, &record.recordID, &status); err != nil {
			_ = rows.Close()
			return BatchProgress{}, err
		}
		record.complete = status == "complete"
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return BatchProgress{}, err
	}
	_ = rows.Close()
	progress := BatchProgress{Cursor: cursor, Records: make([]BatchRecordProgress, 0, len(records))}
	for _, record := range records {
		var effects, refunds int
		if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'charge' AND status = 'succeeded'`,
			record.recordID, period).Scan(&effects); err != nil {
			return BatchProgress{}, err
		}
		if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'refund' AND status = 'succeeded'`,
			record.recordID, period).Scan(&refunds); err != nil {
			return BatchProgress{}, err
		}
		var operationStatus string
		err := h.db.QueryRowContext(ctx, `SELECT status FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'charge' ORDER BY created_at DESC LIMIT 1`,
			record.recordID, period).Scan(&operationStatus)
		if errors.Is(err, sql.ErrNoRows) {
			operationStatus = "not-started"
		} else if err != nil {
			return BatchProgress{}, err
		}
		progress.Records = append(progress.Records, BatchRecordProgress{
			Ordinal: record.ordinal, RecordID: record.recordID, Complete: record.complete, Effects: effects, Refunds: refunds, OperationStatus: operationStatus,
		})
	}
	return progress, nil
}

func (h *Host) StartFanout(ctx context.Context, runID, period string, recordIDs []string) error {
	err := h.startFanout(ctx, runID, period, recordIDs)
	if isRecordClaimConflict(err) {
		return transition.ErrConflict
	}
	return err
}

func (h *Host) startFanout(ctx context.Context, runID, period string, recordIDs []string) error {
	if runID == "" || period == "" || len(recordIDs) == 0 {
		return transition.ErrInvalidRequest
	}
	seen := make(map[string]struct{}, len(recordIDs))
	for _, id := range recordIDs {
		if id == "" {
			return transition.ErrInvalidRequest
		}
		if _, exists := seen[id]; exists {
			return transition.ErrConflict
		}
		seen[id] = struct{}{}
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var storedPeriod string
	err = tx.QueryRowContext(ctx, h.lockQuery(`SELECT period_id FROM renewal_fanout_runs WHERE run_id = ?`), runID).Scan(&storedPeriod)
	switch {
	case err == nil:
		if storedPeriod != period {
			_ = tx.Rollback()
			return transition.ErrConflict
		}
		rows, queryErr := tx.QueryContext(ctx, `SELECT record_id FROM renewal_fanout_records WHERE run_id = ? ORDER BY ordinal`, runID)
		if queryErr != nil {
			_ = tx.Rollback()
			return queryErr
		}
		var existing []string
		for rows.Next() {
			var id string
			if queryErr = rows.Scan(&id); queryErr != nil {
				break
			}
			existing = append(existing, id)
		}
		if queryErr == nil {
			queryErr = rows.Err()
		}
		_ = rows.Close()
		if queryErr != nil {
			_ = tx.Rollback()
			return queryErr
		}
		if len(existing) != len(recordIDs) {
			_ = tx.Rollback()
			return transition.ErrConflict
		}
		for index := range recordIDs {
			if existing[index] != recordIDs[index] {
				_ = tx.Rollback()
				return transition.ErrConflict
			}
		}
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_fanout_runs (run_id, period_id, cursor_ordinal) VALUES (?, ?, 0)`, runID, period); err != nil {
			_ = tx.Rollback()
			return err
		}
		for index, id := range recordIDs {
			var ownerKind, ownerID string
			err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT owner_kind, owner_id FROM renewal_record_claims WHERE record_id = ? AND period_id = ?`),
				id, period).Scan(&ownerKind, &ownerID)
			if err == nil && (ownerKind != "fanout" || ownerID != runID) {
				_ = tx.Rollback()
				return transition.ErrConflict
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				_ = tx.Rollback()
				return err
			}
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_record_claims (record_id, period_id, owner_kind, owner_id) VALUES (?, ?, 'fanout', ?)`, id, period, runID); err != nil {
					_ = tx.Rollback()
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_fanout_records (run_id, period_id, ordinal, record_id, execution_id, status) VALUES (?, ?, ?, ?, '', 'pending')`,
				runID, period, index+1, id); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	default:
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, id := range recordIDs {
		if err := h.acceptFanoutRecord(ctx, runID, id, period); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) acceptFanoutRecord(ctx context.Context, runID, id, period string) error {
	executionID, err := h.startOwned(ctx, id, period, "fanout", runID)
	if err != nil {
		return err
	}
	if h.afterFanoutAccept != nil {
		h.afterFanoutAccept(id)
	}
	if _, err := h.db.ExecContext(ctx, `UPDATE renewal_fanout_records SET execution_id = ? WHERE run_id = ? AND record_id = ? AND execution_id = ''`,
		executionID, runID, id); err != nil {
		return err
	}
	var stored string
	if err := h.db.QueryRowContext(ctx, `SELECT execution_id FROM renewal_fanout_records WHERE run_id = ? AND record_id = ?`, runID, id).Scan(&stored); err != nil {
		return err
	}
	if stored != executionID {
		return transition.ErrConflict
	}
	return nil
}

func isRecordClaimConflict(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return isDuplicate(err) || strings.Contains(message, "deadlock") || strings.Contains(message, "lock wait timeout")
}

func (h *Host) FanoutProgress(ctx context.Context, runID string) (FanoutProgress, error) {
	var period string
	var cursor int
	if err := h.db.QueryRowContext(ctx, `SELECT period_id, cursor_ordinal FROM renewal_fanout_runs WHERE run_id = ?`, runID).Scan(&period, &cursor); err != nil {
		return FanoutProgress{}, err
	}
	rows, err := h.db.QueryContext(ctx, `SELECT ordinal, record_id, execution_id, status FROM renewal_fanout_records WHERE run_id = ? ORDER BY ordinal`, runID)
	if err != nil {
		return FanoutProgress{}, err
	}
	var records []fanoutRecord
	for rows.Next() {
		var record fanoutRecord
		var status string
		if err := rows.Scan(&record.ordinal, &record.recordID, &record.executionID, &status); err != nil {
			_ = rows.Close()
			return FanoutProgress{}, err
		}
		record.complete = status == "complete"
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return FanoutProgress{}, err
	}
	_ = rows.Close()
	progress := FanoutProgress{Cursor: cursor, Records: make([]FanoutRecordProgress, 0, len(records))}
	for _, record := range records {
		var effects int
		if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_provider_ops WHERE plan_id = ? AND period_id = ? AND kind = 'charge' AND status = 'succeeded'`,
			record.recordID, period).Scan(&effects); err != nil {
			return FanoutProgress{}, err
		}
		progress.Records = append(progress.Records, FanoutRecordProgress{
			Ordinal: record.ordinal, EntityID: record.recordID, ExecutionID: record.executionID,
			Complete: record.complete, Effects: effects,
		})
	}
	return progress, nil
}

func (h *Host) DriveFanout(ctx context.Context, runID string) error {
	progress, err := h.FanoutProgress(ctx, runID)
	if err != nil {
		return err
	}
	var period string
	if err := h.db.QueryRowContext(ctx, `SELECT period_id FROM renewal_fanout_runs WHERE run_id = ?`, runID).Scan(&period); err != nil {
		return err
	}
	for _, record := range progress.Records {
		if err := h.acceptFanoutRecord(ctx, runID, record.EntityID, period); err != nil {
			return err
		}
	}
	if err := h.Drive(ctx); err != nil {
		return err
	}
	for _, record := range progress.Records {
		if record.Complete {
			continue
		}
		status, err := h.Inspect(ctx, record.EntityID, period, "operator")
		if err != nil {
			return err
		}
		if status.State == AwaitingApproval {
			if _, err := h.Approve(ctx, record.EntityID, period, "operator"); err != nil {
				return err
			}
		}
	}
	if err := h.Drive(ctx); err != nil {
		return err
	}
	for _, record := range progress.Records {
		if record.Complete {
			continue
		}
		status, err := h.Inspect(ctx, record.EntityID, period, "operator")
		if err != nil {
			return err
		}
		if status.State != Renewed {
			continue
		}
		if err := h.markFanoutRecordComplete(ctx, runID, fanoutRecord{ordinal: record.Ordinal, recordID: record.EntityID}); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) chargeBatch(ctx context.Context, plan string, execution fsm.ExecutionContext[string, State, Event, Input]) (bool, error) {
	rows, err := h.db.QueryContext(ctx, `SELECT ordinal, record_id, status FROM renewal_batch_records WHERE batch_id = ? ORDER BY ordinal`, plan)
	if err != nil {
		return false, err
	}
	var records []batchRecord
	for rows.Next() {
		var record batchRecord
		var status string
		if err := rows.Scan(&record.ordinal, &record.recordID, &status); err != nil {
			_ = rows.Close()
			return false, err
		}
		record.complete = status == "complete"
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, err
	}
	_ = rows.Close()
	if len(records) == 0 {
		return false, nil
	}
	var period string
	if err := h.db.QueryRowContext(ctx, `SELECT period_id FROM renewal_batch_runs WHERE batch_id = ?`, plan).Scan(&period); err != nil {
		return true, err
	}
	for _, record := range records {
		if record.complete {
			continue
		}
		if err := h.chargeRecord(ctx, execution, record.recordID, period); err != nil {
			return true, err
		}
		if h.afterBatchEffect != nil {
			h.afterBatchEffect(record.recordID)
		}
		if err := h.markBatchRecordComplete(ctx, plan, record); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (h *Host) markBatchRecordComplete(ctx context.Context, batchID string, record batchRecord) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE renewal_batch_records SET status = 'complete' WHERE batch_id = ? AND record_id = ? AND status = 'pending'`,
		batchID, record.recordID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if updated == 0 {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM renewal_batch_records WHERE batch_id = ? AND record_id = ?`,
			batchID, record.recordID).Scan(&status); err != nil {
			_ = tx.Rollback()
			return err
		}
		if status != "complete" {
			_ = tx.Rollback()
			return fmt.Errorf("batch record %q lost its host progress claim", record.recordID)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE renewal_batch_runs SET cursor_ordinal = ? WHERE batch_id = ? AND cursor_ordinal < ?`,
		record.ordinal, batchID, record.ordinal); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (h *Host) markFanoutRecordComplete(ctx context.Context, runID string, record fanoutRecord) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var priorCursor int
	if err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT cursor_ordinal FROM renewal_fanout_runs WHERE run_id = ?`), runID).Scan(&priorCursor); err != nil {
		_ = tx.Rollback()
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE renewal_fanout_records SET status = 'complete' WHERE run_id = ? AND record_id = ? AND status = 'pending'`,
		runID, record.recordID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if updated == 0 {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM renewal_fanout_records WHERE run_id = ? AND record_id = ?`,
			runID, record.recordID).Scan(&status); err != nil {
			_ = tx.Rollback()
			return err
		}
		if status != "complete" {
			_ = tx.Rollback()
			return fmt.Errorf("fan-out entity %q lost its host progress claim", record.recordID)
		}
	}
	var firstPending sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(ordinal) FROM renewal_fanout_records WHERE run_id = ? AND status <> 'complete'`, runID).Scan(&firstPending); err != nil {
		_ = tx.Rollback()
		return err
	}
	var cursor int64
	if firstPending.Valid {
		cursor = firstPending.Int64 - 1
	} else if err := tx.QueryRowContext(ctx, `SELECT MAX(ordinal) FROM renewal_fanout_records WHERE run_id = ?`, runID).Scan(&cursor); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE renewal_fanout_runs SET cursor_ordinal = ? WHERE run_id = ? AND cursor_ordinal < ?`,
		cursor, runID, cursor); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (h *Host) chargeRecord(ctx context.Context, execution fsm.ExecutionContext[string, State, Event, Input], recordID, period string) error {
	input := providerInput{PlanID: recordID, PeriodID: period, Amount: amountCents, Kind: "charge"}
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request := h.operationRequest("charge:"+recordID, input, encoded, false, nil)
	result, err := transition.TrackOperation(ctx, request, h.dispatchProvider)
	if err != nil {
		return err
	}
	return validateReceipt(result, input, "charge")
}
