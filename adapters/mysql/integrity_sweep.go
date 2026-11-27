package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

// Execution and reservation rows are inspected only through linked work
// projections; legacy checkpoints for their retired sweep kinds are ignored.
var integritySweepKinds = []string{"timer", "hook", "notification", "input"}

type integritySweepObservation struct {
	locator int64
	id      string
}

// RunIntegritySweep advances one durable batch for each retained selection
// table. Each call is bounded by batchSize per table and serializes with other
// sweepers through the adapter transaction. The captured high-water marks
// exclude later inserts; changes behind a cursor are not discovered.
func (m *IntegrityMaintenance) RunIntegritySweep(
	ctx context.Context, actor, reason string, batchSize int,
) (transition.IntegritySweepProgress, error) {
	if batchSize <= 0 || batchSize > transition.MaxIntegrityPageSize {
		return transition.IntegritySweepProgress{}, transition.ErrInvalidRequest
	}
	if err := m.checkAccess(ctx, "sweep", actor, reason, 0, 0); err != nil {
		return transition.IntegritySweepProgress{}, err
	}
	tx, err := m.store.beginTx(ctx)
	if err != nil {
		return transition.IntegritySweepProgress{}, err
	}
	defer tx.Rollback()
	if err := lockMySQLSchema(ctx, tx, false); err != nil {
		return transition.IntegritySweepProgress{}, err
	}
	if err := validateMySQLSchema(ctx, tx, false); err != nil {
		return transition.IntegritySweepProgress{}, err
	}

	now := time.Now().UTC()
	sweepID, startedAt, healthy, incomplete, complete, _, err := prepareIntegritySweep(ctx, tx, now)
	if err != nil {
		return transition.IntegritySweepProgress{}, err
	}
	if !complete {
		for _, kind := range integritySweepKinds {
			highWater, cursor, inspected, err := integritySweepCheckpoint(ctx, tx, kind)
			if err != nil {
				return transition.IntegritySweepProgress{}, err
			}
			if cursor >= highWater {
				continue
			}
			observations, err := readIntegritySweepBatch(ctx, tx, kind, cursor, highWater, batchSize)
			if err != nil {
				return transition.IntegritySweepProgress{}, err
			}
			for _, observation := range observations {
				unverifiedBehavior, err := inspectIntegritySweepObservation(ctx, tx, kind, observation)
				if errors.Is(err, transition.ErrIntegrity) {
					healthy = false
				} else if err != nil {
					return transition.IntegritySweepProgress{}, err
				}
				if unverifiedBehavior {
					healthy = false
					incomplete = true
				}
				cursor = observation.locator
				inspected++
			}
			if len(observations) < batchSize {
				cursor = highWater
			}
			result, err := tx.ExecContext(ctx, `UPDATE rff_integrity_sweep_progress
				SET inspected_through = ?, inspected = ? WHERE kind = ?`, cursor, inspected, kind)
			if err != nil {
				return transition.IntegritySweepProgress{}, fmt.Errorf("persist %s sweep checkpoint: %w", kind, err)
			}
			if count, err := result.RowsAffected(); err != nil || count != 1 {
				return transition.IntegritySweepProgress{}, fmt.Errorf("%w: missing %s sweep checkpoint", ErrUnsupportedSchema, kind)
			}
		}
		complete, err = integritySweepComplete(ctx, tx)
		if err != nil {
			return transition.IntegritySweepProgress{}, err
		}
	}

	updatedAt := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `UPDATE rff_integrity_sweep_state
		SET updated_at = ?, complete = ?, healthy = ?, incomplete = ? WHERE id = 1`,
		updatedAt.Format(time.RFC3339Nano), boolInt(complete), boolInt(healthy), boolInt(incomplete))
	if err != nil {
		return transition.IntegritySweepProgress{}, fmt.Errorf("persist integrity sweep state: %w", err)
	}
	progress, err := readIntegritySweepProgress(ctx, tx, sweepID, startedAt, updatedAt, complete, healthy, incomplete)
	if err != nil {
		return transition.IntegritySweepProgress{}, err
	}
	if err := tx.Commit(); err != nil {
		return transition.IntegritySweepProgress{}, fmt.Errorf("commit integrity sweep batch: %w", err)
	}
	return progress, nil
}

func prepareIntegritySweep(ctx context.Context, tx *sql.Tx, now time.Time) (
	sweepID int64, startedAt time.Time, healthy, incomplete, complete, exists bool, err error,
) {
	var started, updated string
	var healthyFlag, incompleteFlag, completeFlag int
	err = tx.QueryRowContext(ctx, `SELECT sweep_id, started_at, updated_at, healthy, incomplete, complete
		FROM rff_integrity_sweep_state WHERE id = 1 FOR UPDATE`).Scan(
		&sweepID, &started, &updated, &healthyFlag, &incompleteFlag, &completeFlag,
	)
	if errors.Is(err, sql.ErrNoRows) {
		sweepID = 1
	} else if err != nil {
		return 0, time.Time{}, false, false, false, false, fmt.Errorf("read integrity sweep state: %w", err)
	} else {
		exists = true
		if sweepID <= 0 || healthyFlag < 0 || healthyFlag > 1 || incompleteFlag < 0 || incompleteFlag > 1 ||
			completeFlag < 0 || completeFlag > 1 {
			return 0, time.Time{}, false, false, false, false, fmt.Errorf("%w: invalid integrity sweep state", ErrUnsupportedSchema)
		}
		startedAt, err = time.Parse(time.RFC3339Nano, started)
		if err != nil {
			return 0, time.Time{}, false, false, false, false, fmt.Errorf("%w: invalid integrity sweep start time", ErrUnsupportedSchema)
		}
		if _, err := time.Parse(time.RFC3339Nano, updated); err != nil {
			return 0, time.Time{}, false, false, false, false, fmt.Errorf("%w: invalid integrity sweep update time", ErrUnsupportedSchema)
		}
		healthy, incomplete, complete = healthyFlag == 1, incompleteFlag == 1, completeFlag == 1
		if !complete {
			return sweepID, startedAt, healthy, incomplete, false, true, nil
		}
		if sweepID == math.MaxInt64 {
			return 0, time.Time{}, false, false, false, false, fmt.Errorf("%w: integrity sweep ID exhausted", ErrUnsupportedSchema)
		}
		sweepID++
	}

	startedAt = now
	healthy, incomplete, complete = true, false, false
	if _, err := tx.ExecContext(ctx, `DELETE FROM rff_integrity_sweep_progress`); err != nil {
		return 0, time.Time{}, false, false, false, exists, fmt.Errorf("reset integrity sweep checkpoints: %w", err)
	}
	for _, kind := range integritySweepKinds {
		var highWater int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(row_id), 0) FROM `+integritySweepTable(kind)).Scan(&highWater); err != nil {
			return 0, time.Time{}, false, false, false, exists, fmt.Errorf("capture %s sweep high-water mark: %w", kind, err)
		}
		if highWater < 0 {
			return 0, time.Time{}, false, false, false, exists, fmt.Errorf("%w: invalid %s sweep high-water mark", ErrUnsupportedSchema, kind)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rff_integrity_sweep_progress
			(kind, high_water_mark, inspected_through, inspected) VALUES (?, ?, 0, 0)`, kind, highWater); err != nil {
			return 0, time.Time{}, false, false, false, exists, fmt.Errorf("create %s sweep checkpoint: %w", kind, err)
		}
	}
	if exists {
		_, err = tx.ExecContext(ctx, `UPDATE rff_integrity_sweep_state
			SET sweep_id = ?, started_at = ?, updated_at = ?, complete = 0, healthy = 1, incomplete = 0 WHERE id = 1`,
			sweepID, startedAt.Format(time.RFC3339Nano), startedAt.Format(time.RFC3339Nano))
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO rff_integrity_sweep_state
			(id, sweep_id, started_at, updated_at, complete, healthy, incomplete)
			VALUES (1, ?, ?, ?, 0, 1, 0)`,
			sweepID, startedAt.Format(time.RFC3339Nano), startedAt.Format(time.RFC3339Nano))
	}
	if err != nil {
		return 0, time.Time{}, false, false, false, exists, fmt.Errorf("start integrity sweep: %w", err)
	}
	return sweepID, startedAt, healthy, incomplete, complete, exists, nil
}

func integritySweepTable(kind string) string {
	switch kind {
	case "timer":
		return "rff_timers"
	case "hook":
		return "rff_hook_work"
	case "notification":
		return "rff_notification_work"
	case "input":
		return "rff_inputs"
	default:
		panic("unknown integrity sweep kind")
	}
}

func integritySweepCheckpoint(ctx context.Context, tx *sql.Tx, kind string) (int64, int64, int64, error) {
	var highWater, cursor, inspected int64
	if err := tx.QueryRowContext(ctx, `SELECT high_water_mark, inspected_through, inspected
		FROM rff_integrity_sweep_progress WHERE kind = ?`, kind).Scan(&highWater, &cursor, &inspected); err != nil {
		return 0, 0, 0, fmt.Errorf("read %s sweep checkpoint: %w", kind, err)
	}
	if highWater < 0 || cursor < 0 || cursor > highWater || inspected < 0 {
		return 0, 0, 0, fmt.Errorf("%w: invalid %s sweep checkpoint", ErrUnsupportedSchema, kind)
	}
	return highWater, cursor, inspected, nil
}

func readIntegritySweepBatch(ctx context.Context, tx *sql.Tx, kind string, cursor, highWater int64, limit int) (
	observations []integritySweepObservation, err error,
) {
	query := `SELECT row_id, id FROM ` + integritySweepTable(kind) +
		` WHERE row_id > ? AND row_id <= ? ORDER BY row_id LIMIT ?`
	if kind == "hook" || kind == "notification" {
		query = `SELECT row_id, execution_id FROM ` + integritySweepTable(kind) +
			` WHERE row_id > ? AND row_id <= ? ORDER BY row_id LIMIT ?`
	}
	rows, err := tx.QueryContext(ctx, query, cursor, highWater, limit)
	if err != nil {
		return nil, fmt.Errorf("read %s sweep batch: %w", kind, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close %s sweep batch: %w", kind, closeErr))
		}
	}()
	for rows.Next() {
		var observation integritySweepObservation
		if err := rows.Scan(&observation.locator, &observation.id); err != nil {
			return nil, fmt.Errorf("scan %s sweep batch: %w", kind, err)
		}
		if observation.locator <= cursor || observation.locator > highWater {
			return nil, fmt.Errorf("%w: invalid %s sweep observation", ErrUnsupportedSchema, kind)
		}
		observations = append(observations, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s sweep batch: %w", kind, err)
	}
	return observations, nil
}

func inspectIntegritySweepObservation(ctx context.Context, tx *sql.Tx, kind string, observation integritySweepObservation) (bool, error) {
	switch kind {
	case "timer":
		record, err := loadIntegrityTimer(ctx, tx, observation.id)
		return record.Compatibility != nil, err
	case "hook":
		if transition.ValidateDurableID(observation.id) != nil {
			return false, recordHookFinding(ctx, tx, observation.locator, "invalid_hook", "record")
		}
		if _, err := loadHook(ctx, tx, observation.id); err != nil {
			return false, err
		}
		record, err := loadHookExecution(ctx, tx, observation.id)
		return record.Compatibility != nil, err
	case "notification":
		if transition.ValidateDurableID(observation.id) != nil {
			return false, recordNotificationFinding(ctx, tx, observation.locator, "invalid_notification", "record")
		}
		return inspectNotificationSweepObservation(ctx, tx, observation)
	case "input":
		_, _, _, err := loadInputByID(ctx, tx, observation.id)
		return false, err
	default:
		return false, fmt.Errorf("%w: unknown integrity sweep kind", ErrUnsupportedSchema)
	}
}

func inspectNotificationSweepObservation(ctx context.Context, tx *sql.Tx, observation integritySweepObservation) (bool, error) {
	var locator, revision int64
	var data []byte
	var projection notificationProjection
	err := tx.QueryRowContext(ctx, `SELECT row_id, work, revision, completed, due_at_seconds, due_at_nanoseconds,
		lease_owner_present, lease_expires_at_seconds, lease_expires_at_nanoseconds
		FROM rff_notification_work WHERE execution_id = ? FOR UPDATE`, observation.id).Scan(
		&locator, &data, &revision, &projection.completed, &projection.dueSeconds, &projection.dueNanoseconds,
		&projection.leaseOwnerPresent, &projection.leaseExpiresAtSeconds, &projection.leaseExpiresAtNanosecs,
	)
	if err != nil {
		return false, fmt.Errorf("read notification sweep observation: %w", err)
	}
	if locator != observation.locator || revision <= 0 {
		return false, fmt.Errorf("%w: invalid notification sweep locator or revision", ErrUnsupportedSchema)
	}
	quarantined, err := notificationQuarantined(ctx, tx, locator)
	if err != nil {
		return false, err
	}
	if quarantined {
		return false, transition.ErrIntegrity
	}
	var work notificationWork
	if err := json.Unmarshal(data, &work); err != nil {
		return false, errors.Join(err, recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"))
	}
	if !notificationProjectionMatches(work, projection) {
		return false, recordNotificationFinding(ctx, tx, locator, "projection_mismatch", "selection")
	}
	_, execution, err := loadNotificationExecution(ctx, tx, observation.id, locator)
	if err != nil {
		return false, err
	}
	if err := validateNotification(execution, work); err != nil {
		return false, errors.Join(err, recordNotificationFinding(ctx, tx, locator, "invalid_notification", "record"))
	}
	return execution.Compatibility != nil || work.Compatibility != nil, nil
}

func integritySweepComplete(ctx context.Context, tx *sql.Tx) (bool, error) {
	var incomplete bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rff_integrity_sweep_progress
		WHERE kind IN ('timer', 'hook', 'notification', 'input')
		AND inspected_through < high_water_mark)`).Scan(&incomplete); err != nil {
		return false, fmt.Errorf("check integrity sweep completion: %w", err)
	}
	return !incomplete, nil
}

func readIntegritySweepProgress(
	ctx context.Context, tx *sql.Tx, sweepID int64, startedAt, updatedAt time.Time,
	complete, healthy, incomplete bool,
) (progress transition.IntegritySweepProgress, err error) {
	progress = transition.IntegritySweepProgress{
		SweepID: sweepID, StartedAt: startedAt, UpdatedAt: updatedAt,
		Complete: complete, Healthy: healthy, Incomplete: incomplete,
		Coverage: make([]transition.IntegritySweepCoverage, 0, len(integritySweepKinds)),
	}
	rows, err := tx.QueryContext(ctx, `SELECT kind, high_water_mark, inspected_through, inspected
		FROM rff_integrity_sweep_progress`)
	if err != nil {
		return transition.IntegritySweepProgress{}, fmt.Errorf("read integrity sweep coverage: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close integrity sweep coverage: %w", closeErr))
		}
	}()
	coverageByKind := make(map[string]transition.IntegritySweepCoverage, len(integritySweepKinds))
	for rows.Next() {
		var coverage transition.IntegritySweepCoverage
		if err := rows.Scan(&coverage.Kind, &coverage.HighWaterMark, &coverage.InspectedThrough, &coverage.Inspected); err != nil {
			return transition.IntegritySweepProgress{}, fmt.Errorf("read integrity sweep coverage: %w", err)
		}
		if coverage.HighWaterMark < 0 || coverage.InspectedThrough < 0 ||
			coverage.InspectedThrough > coverage.HighWaterMark || coverage.Inspected < 0 {
			return transition.IntegritySweepProgress{}, fmt.Errorf("%w: invalid integrity sweep coverage", ErrUnsupportedSchema)
		}
		switch coverage.Kind {
		case "execution", "reservation":
			continue
		case "timer", "hook", "notification", "input":
		default:
			return transition.IntegritySweepProgress{}, fmt.Errorf("%w: unknown integrity sweep coverage", ErrUnsupportedSchema)
		}
		if _, exists := coverageByKind[coverage.Kind]; exists {
			return transition.IntegritySweepProgress{}, fmt.Errorf("%w: duplicate integrity sweep coverage", ErrUnsupportedSchema)
		}
		coverageByKind[coverage.Kind] = coverage
	}
	if err := rows.Err(); err != nil {
		return transition.IntegritySweepProgress{}, fmt.Errorf("read integrity sweep coverage: %w", err)
	}
	for _, kind := range integritySweepKinds {
		coverage, exists := coverageByKind[kind]
		if !exists {
			return transition.IntegritySweepProgress{}, fmt.Errorf("%w: incomplete integrity sweep coverage", ErrUnsupportedSchema)
		}
		progress.Coverage = append(progress.Coverage, coverage)
	}
	return progress, nil
}
