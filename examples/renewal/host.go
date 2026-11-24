package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

const (
	machineID      = "recurring-renewal"
	machineVersion = "1"
	amountCents    = int64(1200)
)

type State string
type Event string

const (
	AwaitingApproval State = "AwaitingApproval"
	Due              State = "Due"
	PaymentCaptured  State = "PaymentCaptured"
	Renewed          State = "Renewed"

	Approve      Event = "approve"
	Capture      Event = "capture"
	Renew        Event = "renew"
	RetryCapture Event = "retry-capture"
)

type Input struct {
	PlanID    string `json:"plan_id"`
	PeriodID  string `json:"period_id"`
	Principal string `json:"principal"`
}

type jsonCodec[T any] struct{ version string }

func (c jsonCodec[T]) Version() transition.CodecVersion {
	return transition.CodecVersion{Name: "renewal-json", Version: c.version}
}

func (c jsonCodec[T]) Encode(_ context.Context, value T) ([]byte, error) {
	return json.Marshal(value)
}

func (c jsonCodec[T]) Decode(_ context.Context, data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

func (c jsonCodec[T]) Equal(ctx context.Context, stored []byte, incoming T) (bool, error) {
	value, err := c.Decode(ctx, stored)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(value, incoming), nil
}

type persistence interface {
	transition.AcceptanceStore[string, State]
	transition.CoordinatorStore[string, State]
	transition.HookActivityStore[string, State]
	transition.CommitStore[string, State]
	transition.ContinuationDriveStore[string, State]
	transition.ContinuationControlStore[string, State]
	transition.ContinuationMarkerStore[string, State]
	transition.CheckedHookControlStore[string, State]
	ContinuationMarkers(context.Context) ([]transition.ContinuationMarker[string, State], error)
	BlockHookCompatibility(context.Context, transition.HookLease, transition.CompatibilityDiagnostic, func() time.Time) error
	transition.OperationStore
	transition.OperationReconciliationStore
	transition.OperationResolutionStore
	transition.InspectionStore
	transition.TransitionLeaseStore[string]
	transition.ActiveExecutionReader[string, State]
	ContinuationReady(context.Context, string) (bool, error)
	CommitAtomic(context.Context, transition.TransitionLease[string], func() time.Time,
		func(context.Context, *sql.Tx, transition.Execution[string, State]) (transition.CommitReceipt, error),
	) (transition.CommitReceipt, error)
}

type providerMode string

const (
	providerNormal       providerMode = "normal"
	providerLostResponse providerMode = "lost-response"
	providerLateCall     providerMode = "late-call"
	providerFailAfter    providerMode = "fail-after-charge"
)

var errPreCommitFailure = errors.New("injected failure after confirmed charge")
var errLostProviderResponse = errors.New("fake provider response was lost")

type Host struct {
	store                 persistence
	close                 func() error
	db                    *sql.DB
	shared                bool
	dialect               string
	clock                 func() time.Time
	afterBatchEffect      func(string)
	machines              *fsm.Registry[string, State, Event, Input]
	acceptor              *transition.Acceptor[string, State, Event, Input]
	coordinator           *transition.Coordinator[string, State]
	worker                *transition.HookWorker[string, State, Event, Input]
	commits               *transition.CommitCoordinator[string, State]
	discovery             *transition.ContinuationDiscovery[string, State]
	driver                *transition.ContinuationDriver[string, State, Event, Input]
	controls              *transition.AuthorizedHookControls[string, State, Event, Input]
	continuationControls  *transition.ContinuationControls[string, State]
	resolver              *transition.AuthorizedOperationResolver
	beforeSuccessorAccept func(context.Context, transition.SuccessorBinding[string, State]) error
}

func OpenSQLite(path string) (*Host, error) {
	return openSQLite(path, path)
}

// OpenSQLiteSeparate keeps host domain data and runtime records in distinct
// SQLite files, exercising the replayable marker and acceptance-receipt path.
func OpenSQLiteSeparate(runtimePath, hostPath string) (*Host, error) {
	return openSQLite(runtimePath, hostPath)
}

func openSQLite(runtimePath, hostPath string) (*Host, error) {
	store, err := sqlite.Open[string, State](runtimePath, sqlite.StringCodec{}, stateCodec{})
	if err != nil {
		return nil, err
	}
	absolute, err := filepath.Abs(hostPath)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	uri := url.URL{Scheme: "file", Path: absolute}
	query := uri.Query()
	query.Set("mode", "rwc")
	query.Set("_txlock", "immediate")
	query.Set("_busy_timeout", "5000")
	query.Set("_foreign_keys", "on")
	query.Set("_synchronous", "FULL")
	uri.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", uri.String())
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	db.SetMaxOpenConns(4)
	host, err := newHost(store, store.Close, db, runtimePath == hostPath, "sqlite")
	if err != nil {
		_ = db.Close()
		_ = store.Close()
		return nil, err
	}
	return host, nil
}

func OpenMySQL(dsn string) (*Host, error) {
	return openMySQL(dsn, dsn)
}

// OpenMySQLSeparate opens independent runtime and host databases.
func OpenMySQLSeparate(runtimeDSN, hostDSN string) (*Host, error) {
	return openMySQL(runtimeDSN, hostDSN)
}

func openMySQL(runtimeDSN, hostDSN string) (*Host, error) {
	store, err := mysql.Open[string, State](runtimeDSN, mysql.StringCodec{}, stateCodec{})
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", hostDSN)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		_ = store.Close()
		return nil, err
	}
	host, err := newHost(store, store.Close, db, runtimeDSN == hostDSN, "mysql")
	if err != nil {
		_ = db.Close()
		_ = store.Close()
		return nil, err
	}
	return host, nil
}

type stateCodec struct{}

func (stateCodec) Encode(value State) ([]byte, error) { return []byte(value), nil }
func (stateCodec) Decode(value []byte) (State, error) { return State(value), nil }

func newHost(store persistence, closeStore func() error, db *sql.DB, shared bool, dialect string) (*Host, error) {
	host := &Host{store: store, close: closeStore, db: db, shared: shared, dialect: dialect, clock: time.Now}
	if err := host.initialize(); err != nil {
		return nil, err
	}
	eventCodec := jsonCodec[Event]{version: "1"}
	inputCodec := jsonCodec[Input]{version: "1"}
	resolveEvent := func(version transition.CodecVersion) (transition.Codec[Event], error) {
		if version != eventCodec.Version() {
			return nil, transition.ErrCompatibility
		}
		return eventCodec, nil
	}
	resolveInput := func(version transition.CodecVersion) (transition.Codec[Input], error) {
		if version != inputCodec.Version() {
			return nil, transition.ErrCompatibility
		}
		return inputCodec, nil
	}
	machine, err := host.machine()
	if err != nil {
		return nil, err
	}
	host.machines = &fsm.Registry[string, State, Event, Input]{}
	if err := host.machines.Register(machine); err != nil {
		return nil, err
	}
	host.acceptor, err = transition.NewAcceptor[string, State, Event, Input](
		store, host.machines, resolveInput, resolveEvent, newExecutionID, host.clock,
	)
	if err != nil {
		return nil, err
	}
	runner, err := transition.NewHookRunner[string, State, Event, Input](host.machines, resolveInput, resolveEvent)
	if err != nil {
		return nil, err
	}
	host.coordinator, err = transition.NewCoordinator[string, State](store, runner, transition.CoordinatorConfig{
		ID: "renewal-host", LeaseTTL: 5 * time.Second, Clock: host.clock,
	})
	if err != nil {
		return nil, err
	}
	host.worker, err = transition.NewHookWorker[string, State, Event, Input](store, runner, transition.WorkerConfig{
		ID: "renewal-worker", Capacity: 1, LeaseTTL: 5 * time.Second,
		RenewalInterval: time.Second, Clock: transition.SystemWorkerClock{},
	})
	if err != nil {
		return nil, err
	}
	host.commits, err = transition.NewCommitCoordinator[string, State](store, host, transition.CoordinatorConfig{
		ID: "renewal-host", LeaseTTL: 5 * time.Second, Clock: host.clock,
	})
	if err != nil {
		return nil, err
	}
	host.discovery, err = transition.NewContinuationDiscovery[string, State](store, host)
	if err != nil {
		return nil, err
	}
	host.driver, err = transition.NewContinuationDriver[string, State, Event, Input](store, host.machines,
		transition.ContinuationDriverConfig[string, State, Event, Input]{
			Clock: host.clock, LeaseDuration: 30 * time.Second,
			EventCodec: eventCodec.Version(), InputCodec: inputCodec.Version(),
			ResolveEvent: resolveEvent, ResolveInput: resolveInput, Acceptor: host.acceptor, Host: host,
			OriginRevision: host.originRevision,
		},
	)
	if err != nil {
		return nil, err
	}
	host.controls, err = transition.NewAuthorizedHookControls[string, State, Event, Input](
		store, runner, host.authorizeHookControl,
	)
	if err != nil {
		return nil, err
	}
	host.continuationControls, err = transition.NewContinuationControls[string, State](store, host.authorizeContinuationControl)
	if err != nil {
		return nil, err
	}
	host.resolver, err = transition.NewAuthorizedOperationResolver(store, host.authorizeOperationResolution, host.clock)
	if err != nil {
		return nil, err
	}
	return host, nil
}

func newExecutionID() (string, error) {
	id, err := randomID()
	if err != nil {
		return "", err
	}
	return "renewal-" + id, nil
}

func randomID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func (h *Host) Close() error {
	return errors.Join(h.db.Close(), h.close())
}

func (h *Host) initialize() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS renewal_plans (plan_id VARCHAR(255) PRIMARY KEY, period_id VARCHAR(255) NOT NULL, state VARCHAR(64) NOT NULL, revision BIGINT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS renewal_workload_grants (plan_id VARCHAR(255) NOT NULL, principal VARCHAR(255) NOT NULL, allowed INTEGER NOT NULL, PRIMARY KEY (plan_id, principal))`,
		`CREATE TABLE IF NOT EXISTS renewal_commit_receipts (execution_id VARCHAR(255) PRIMARY KEY, plan_id VARCHAR(255) NOT NULL, operation_id VARCHAR(255) NOT NULL, state VARCHAR(64) NOT NULL, revision BIGINT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS renewal_markers (execution_id VARCHAR(255) PRIMARY KEY, marker BLOB NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS renewal_fences (plan_id VARCHAR(255) PRIMARY KEY, execution_id VARCHAR(255) NOT NULL, origin_revision BIGINT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS renewal_successors (origin_execution_id VARCHAR(255) PRIMARY KEY, binding BLOB NOT NULL, outcome VARCHAR(32) NOT NULL, principal VARCHAR(255) NOT NULL, decision VARCHAR(64) NOT NULL, accepted_at TIMESTAMP NOT NULL, cancellation BLOB)`,
		`CREATE TABLE IF NOT EXISTS renewal_provider_settings (plan_id VARCHAR(255) PRIMARY KEY, mode VARCHAR(64) NOT NULL, consumed INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS renewal_provider_ops (provider_key VARCHAR(64) PRIMARY KEY, execution_id VARCHAR(255) NOT NULL, operation_key VARCHAR(255) NOT NULL, kind VARCHAR(32) NOT NULL, plan_id VARCHAR(255) NOT NULL, period_id VARCHAR(255) NOT NULL, fingerprint VARCHAR(64) NOT NULL, input BLOB NOT NULL, status VARCHAR(32) NOT NULL, result BLOB, related_key VARCHAR(64), created_at TIMESTAMP NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS renewal_period_effects (plan_id VARCHAR(255) NOT NULL, period_id VARCHAR(255) NOT NULL, provider_key VARCHAR(64) NOT NULL, status VARCHAR(32) NOT NULL, PRIMARY KEY (plan_id, period_id))`,
		`CREATE TABLE IF NOT EXISTS renewal_completed_periods (plan_id VARCHAR(255) NOT NULL, period_id VARCHAR(255) NOT NULL, execution_id VARCHAR(255) NOT NULL, PRIMARY KEY (plan_id, period_id))`,
		`CREATE TABLE IF NOT EXISTS renewal_batch_runs (batch_id VARCHAR(255) PRIMARY KEY, period_id VARCHAR(255) NOT NULL, cursor_ordinal BIGINT NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS renewal_batch_records (batch_id VARCHAR(255) NOT NULL, period_id VARCHAR(255) NOT NULL, ordinal BIGINT NOT NULL, record_id VARCHAR(255) NOT NULL, status VARCHAR(32) NOT NULL, PRIMARY KEY (batch_id, record_id), UNIQUE (batch_id, ordinal))`,
		`CREATE TABLE IF NOT EXISTS renewal_record_claims (record_id VARCHAR(255) NOT NULL, period_id VARCHAR(255) NOT NULL, owner_kind VARCHAR(32) NOT NULL, owner_id VARCHAR(255) NOT NULL, PRIMARY KEY (record_id, period_id))`,
		`CREATE TABLE IF NOT EXISTS renewal_fanout_runs (run_id VARCHAR(255) PRIMARY KEY, period_id VARCHAR(255) NOT NULL, cursor_ordinal BIGINT NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS renewal_fanout_records (run_id VARCHAR(255) NOT NULL, period_id VARCHAR(255) NOT NULL, ordinal BIGINT NOT NULL, record_id VARCHAR(255) NOT NULL, execution_id VARCHAR(255) NOT NULL, status VARCHAR(32) NOT NULL, PRIMARY KEY (run_id, record_id), UNIQUE (run_id, ordinal))`,
	}
	for _, statement := range statements {
		if _, err := h.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize renewal host schema: %w", err)
		}
	}
	return nil
}

func (h *Host) machine() (*fsm.Machine[string, State, Event, Input], error) {
	policy := fsm.Policy{MaxAttempts: 3, AttemptTimeout: 5 * time.Second, OverallTimeout: time.Minute, RetryDelays: []time.Duration{10 * time.Millisecond}}
	continuationPolicy := fsm.Policy{MaxAttempts: 3, AttemptTimeout: 30 * time.Second, OverallTimeout: time.Minute, RetryDelays: []time.Duration{10 * time.Millisecond}}
	noOp := fsm.NoopHandler[string, State, Event, Input]{}
	return fsm.NewMachine(fsm.Config[string, State, Event, Input]{
		ID: machineID, Version: machineVersion,
		InitialStates: []State{AwaitingApproval}, TerminalStates: []State{Renewed},
		ContinuationPolicies: map[State]fsm.Policy{Due: continuationPolicy, PaymentCaptured: continuationPolicy},
		Handlers: map[State]fsm.Handler[string, State, Event, Input]{
			AwaitingApproval: noOp,
			Due:              noOp,
			PaymentCaptured:  &paymentHandler{host: h},
			Renewed:          noOp,
		},
		SelectInitial: func(input Input) (fsm.Selection[State], error) {
			if input.PlanID == "" || input.PeriodID == "" {
				return fsm.Selection[State]{}, transition.ErrInvalidRequest
			}
			return fsm.Selection[State]{Target: AwaitingApproval, Policy: policy}, nil
		},
		SelectTransition: func(source State, event Event, input Input) (fsm.Selection[State], error) {
			if input.PlanID == "" || input.PeriodID == "" {
				return fsm.Selection[State]{}, transition.ErrInvalidRequest
			}
			switch {
			case source == AwaitingApproval && event == Approve:
				return fsm.Selection[State]{Target: Due, Policy: policy}, nil
			case source == Due && (event == Capture || event == RetryCapture):
				return fsm.Selection[State]{Target: PaymentCaptured, Policy: policy}, nil
			case source == PaymentCaptured && event == Renew:
				return fsm.Selection[State]{Target: Renewed, Policy: policy}, nil
			default:
				return fsm.Selection[State]{}, fmt.Errorf("event %q is invalid from %q", event, source)
			}
		},
		Continue: func(ctx context.Context, plan string, state State) (fsm.ContinuationDecision[Event, Input], error) {
			var period string
			if err := h.db.QueryRowContext(ctx, `SELECT period_id FROM renewal_plans WHERE plan_id = ?`, plan).Scan(&period); err != nil {
				return fsm.ContinuationDecision[Event, Input]{}, err
			}
			input := Input{PlanID: plan, PeriodID: period, Principal: "renewal-worker"}
			switch state {
			case Due:
				return fsm.ContinuationDecision[Event, Input]{Continue: true, Event: Capture, Input: input}, nil
			case PaymentCaptured:
				return fsm.ContinuationDecision[Event, Input]{Continue: true, Event: Renew, Input: input}, nil
			default:
				return fsm.ContinuationDecision[Event, Input]{}, nil
			}
		},
		ClassifyFailure: func(err error) (fsm.Failure, fsm.Disposition) {
			if errors.Is(err, errPreCommitFailure) {
				return fsm.Failure{Code: "payment_captured_commit_failed"}, fsm.DispositionIntervention
			}
			if errors.Is(err, transition.ErrOperationUnresolved) || errors.Is(err, errLostProviderResponse) {
				return fsm.Failure{Code: "payment_outcome_unknown"}, fsm.DispositionRetry
			}
			return fsm.Failure{Code: "renewal_work_failed"}, fsm.DispositionIntervention
		},
	})
}

type paymentHandler struct{ host *Host }

func (p *paymentHandler) Enter(context.Context, fsm.ExecutionContext[string, State, Event, Input]) error {
	return nil
}

func (p *paymentHandler) Transition(ctx context.Context, execution fsm.ExecutionContext[string, State, Event, Input]) error {
	if err := p.host.charge(ctx, execution); err != nil {
		return err
	}
	mode, err := p.host.mode(ctx, execution.EntityID())
	if err != nil {
		return err
	}
	if mode == providerFailAfter {
		return errPreCommitFailure
	}
	return nil
}

func (p *paymentHandler) Retry(ctx context.Context, execution fsm.ExecutionContext[string, State, Event, Input], _ fsm.Failure) error {
	return p.Transition(ctx, execution)
}

func (p *paymentHandler) Recover(ctx context.Context, execution fsm.ExecutionContext[string, State, Event, Input], _ fsm.Failure) error {
	return p.host.refund(ctx, execution)
}

func (h *Host) Start(ctx context.Context, plan, period string) (string, error) {
	if plan == "" || period == "" {
		return "", transition.ErrInvalidRequest
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_plans (plan_id, period_id, state, revision) VALUES (?, ?, '', 0)`, plan, period); err != nil {
		if !isDuplicate(err) {
			_ = tx.Rollback()
			return "", err
		}
		var existingPeriod string
		if readErr := tx.QueryRowContext(ctx, `SELECT period_id FROM renewal_plans WHERE plan_id = ?`, plan).Scan(&existingPeriod); readErr != nil {
			_ = tx.Rollback()
			return "", readErr
		}
		if existingPeriod != period {
			_ = tx.Rollback()
			return "", transition.ErrConflict
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_provider_settings (plan_id, mode, consumed) VALUES (?, ?, 0)`, plan, providerNormal); err != nil && !isDuplicate(err) {
		_ = tx.Rollback()
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_workload_grants (plan_id, principal, allowed) VALUES (?, 'renewal-worker', 1)`, plan); err != nil && !isDuplicate(err) {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	execution, _, err := h.acceptor.Accept(ctx, transition.Request[string, State, Event, Input]{
		Entity: plan, Key: "initialize:" + period, Operation: transition.Initialize,
		MachineID: machineID, MachineVersion: machineVersion,
		Input:        Input{PlanID: plan, PeriodID: period, Principal: "renewal-host"},
		InputCodec:   (jsonCodec[Input]{version: "1"}).Version(),
		Notification: transition.NotifyDisable,
	})
	if err != nil {
		return "", err
	}
	return execution.ID, nil
}

func (h *Host) Approve(ctx context.Context, plan, period, principal string) (string, error) {
	if principal != "operator" {
		return "", errors.New("approval denied")
	}
	if err := h.checkPlan(ctx, plan, period, AwaitingApproval); err != nil {
		return "", err
	}
	execution, _, err := h.acceptor.Accept(ctx, transition.Request[string, State, Event, Input]{
		Entity: plan, Key: "approval:" + period, Operation: transition.Transition,
		MachineID: machineID, MachineVersion: machineVersion, Previous: AwaitingApproval, HasPrevious: true,
		Event: Approve, HasEvent: true, EventCodec: (jsonCodec[Event]{version: "1"}).Version(),
		Input:      Input{PlanID: plan, PeriodID: period, Principal: principal},
		InputCodec: (jsonCodec[Input]{version: "1"}).Version(), Notification: transition.NotifyDisable,
	})
	if err != nil {
		return "", err
	}
	return execution.ID, nil
}

func (h *Host) SetWorkloadAuthorization(ctx context.Context, plan, workload, actor string, allowed bool) error {
	if actor != "operator" {
		return errors.New("workload authorization denied")
	}
	if plan == "" || workload != "renewal-worker" {
		return transition.ErrInvalidRequest
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var planID string
	if err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT plan_id FROM renewal_plans WHERE plan_id = ?`), plan).Scan(&planID); err != nil {
		_ = tx.Rollback()
		return err
	}
	var current int
	err = tx.QueryRowContext(ctx, h.lockQuery(`SELECT allowed FROM renewal_workload_grants WHERE plan_id = ? AND principal = ?`),
		plan, workload).Scan(&current)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO renewal_workload_grants (plan_id, principal, allowed) VALUES (?, ?, ?)`,
			plan, workload, allowed)
	case err != nil:
	default:
		_, err = tx.ExecContext(ctx, `UPDATE renewal_workload_grants SET allowed = ? WHERE plan_id = ? AND principal = ?`,
			allowed, plan, workload)
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (h *Host) checkPlan(ctx context.Context, plan, period string, state State) error {
	var gotPeriod, gotState string
	err := h.db.QueryRowContext(ctx, `SELECT period_id, state FROM renewal_plans WHERE plan_id = ?`, plan).Scan(&gotPeriod, &gotState)
	if err != nil {
		return err
	}
	if gotPeriod != period || State(gotState) != state {
		return transition.ErrConflict
	}
	return nil
}

func (h *Host) SetProviderMode(ctx context.Context, plan string, mode providerMode) error {
	switch mode {
	case providerNormal, providerLostResponse, providerLateCall, providerFailAfter:
	default:
		return transition.ErrInvalidRequest
	}
	_, err := h.db.ExecContext(ctx, `UPDATE renewal_provider_settings SET mode = ?, consumed = 0 WHERE plan_id = ?`, mode, plan)
	return err
}

func (h *Host) mode(ctx context.Context, plan string) (providerMode, error) {
	var mode string
	err := h.db.QueryRowContext(ctx, `SELECT mode FROM renewal_provider_settings WHERE plan_id = ?`, plan).Scan(&mode)
	return providerMode(mode), err
}

func (h *Host) Drive(ctx context.Context) error {
	for round := 0; round < 64; round++ {
		if err := h.coordinator.Reconcile(ctx); err != nil && !errors.Is(err, transition.ErrLeaseHeld) {
			return fmt.Errorf("reconcile transitions: %w", err)
		}
		workerErr := h.worker.RunOne(ctx)
		if workerErr != nil && !errors.Is(workerErr, transition.ErrNoHookActivity) {
			return workerErr
		}
		if err := h.commits.Reconcile(ctx); err != nil {
			return fmt.Errorf("reconcile commits: %w", err)
		}
		if err := h.reconcileFences(ctx); err != nil {
			return fmt.Errorf("reconcile host fences: %w", err)
		}
		if err := h.discovery.Reconcile(ctx); err != nil {
			return fmt.Errorf("discover continuation markers: %w", err)
		}
		markers, err := h.store.ContinuationMarkers(ctx)
		if err != nil {
			return fmt.Errorf("list continuation markers: %w", err)
		}
		continued := false
		for _, marker := range markers {
			ready, err := h.store.ContinuationReady(ctx, marker.ExecutionID)
			if err != nil {
				if errors.Is(err, transition.ErrContinuationPending) {
					continue
				}
				return fmt.Errorf("continuation readiness: %w", err)
			}
			if !ready {
				continue
			}
			progress, err := h.store.ReadContinuation(ctx, marker.ExecutionID)
			if err != nil {
				return fmt.Errorf("read continuation progress: %w", err)
			}
			switch progress.Phase {
			case transition.ContinuationAccepted, transition.ContinuationStopped,
				transition.ContinuationSuperseded, transition.ContinuationWaived,
				transition.ContinuationIntervention:
				continue
			}
			if err := h.driver.Drive(ctx, marker, "renewal-continuation"); err != nil {
				if errors.Is(err, transition.ErrBusy) || errors.Is(err, transition.ErrLeaseHeld) {
					continue
				}
				return fmt.Errorf("drive continuation %s for %v -> %v: %w", marker.ExecutionID, marker.Entity, marker.Target, err)
			}
			continued = true
		}
		if errors.Is(workerErr, transition.ErrNoHookActivity) && !continued {
			active, err := h.pendingWork(ctx)
			if err != nil {
				return err
			}
			if !active {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return errors.New("renewal scenario exceeded its bounded reconciliation rounds")
}

func (h *Host) reconcileFences(ctx context.Context) error {
	rows, err := h.db.QueryContext(ctx, `SELECT plan_id, execution_id FROM renewal_fences`)
	if err != nil {
		return err
	}
	type fence struct{ planID, executionID string }
	var fences []fence
	for rows.Next() {
		var item fence
		if err := rows.Scan(&item.planID, &item.executionID); err != nil {
			_ = rows.Close()
			return err
		}
		fences = append(fences, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for _, item := range fences {
		inspection, err := h.store.InspectExecution(ctx, item.executionID)
		if err != nil {
			continue
		}
		if inspection.Status == transition.Aborted {
			if _, err := h.db.ExecContext(ctx, `DELETE FROM renewal_fences WHERE plan_id = ? AND execution_id = ?`, item.planID, item.executionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Host) pendingWork(ctx context.Context) (bool, error) {
	ids, err := h.store.ActiveExecutionIDs(ctx)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		execution, err := h.store.ReadActiveExecution(ctx, id)
		if err != nil {
			continue
		}
		switch execution.Status {
		case transition.Waiting, transition.Running, transition.Aborting:
			return true, nil
		}
	}
	return false, nil
}

func (h *Host) DriveOneHook(ctx context.Context) error {
	if err := h.coordinator.Reconcile(ctx); err != nil && !errors.Is(err, transition.ErrLeaseHeld) {
		return err
	}
	err := h.worker.RunOne(ctx)
	if errors.Is(err, transition.ErrNoHookActivity) {
		return nil
	}
	return err
}

func (h *Host) DriveUntilCharge(ctx context.Context, plan string) error {
	for round := 0; round < 32; round++ {
		if err := h.coordinator.Reconcile(ctx); err != nil && !errors.Is(err, transition.ErrLeaseHeld) {
			return err
		}
		workerErr := h.worker.RunOne(ctx)
		if workerErr != nil && !errors.Is(workerErr, transition.ErrNoHookActivity) {
			return workerErr
		}
		if err := h.commits.Reconcile(ctx); err != nil {
			return err
		}
		if err := h.discovery.Reconcile(ctx); err != nil {
			return err
		}
		markers, err := h.store.ContinuationMarkers(ctx)
		if err != nil {
			return err
		}
		for _, marker := range markers {
			ready, err := h.store.ContinuationReady(ctx, marker.ExecutionID)
			if err != nil || !ready {
				continue
			}
			progress, err := h.store.ReadContinuation(ctx, marker.ExecutionID)
			if err != nil {
				return err
			}
			if progress.Phase == transition.ContinuationAccepted || progress.Phase == transition.ContinuationStopped {
				continue
			}
			if err := h.driver.Drive(ctx, marker, "renewal-continuation"); err != nil &&
				!errors.Is(err, transition.ErrBusy) && !errors.Is(err, transition.ErrLeaseHeld) {
				return err
			}
		}
		var count int
		err = h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_provider_ops WHERE plan_id = ? AND period_id = (SELECT period_id FROM renewal_plans WHERE plan_id = ?) AND kind = 'charge'`, plan, plan).Scan(&count)
		if err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
	}
	return errors.New("charge hook was not reached within bounded rounds")
}

func (h *Host) Commit(ctx context.Context, lease transition.TransitionLease[string], execution transition.Execution[string, State]) (transition.CommitReceipt, error) {
	if h.shared {
		return h.store.CommitAtomic(ctx, lease, h.clock, h.commitTransaction)
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	receipt, err := h.commitTransaction(ctx, tx, execution)
	if err != nil {
		_ = tx.Rollback()
		return transition.CommitReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return transition.CommitReceipt{}, err
	}
	if receipt.Outcome == transition.CommitSucceeded && execution.Continuation != nil {
		marker := transition.ContinuationMarker[string, State]{
			SchemaVersion: 1, ExecutionID: execution.ID, OperationID: execution.CommitOperationID,
			Entity: execution.Entity, Target: execution.Target, MachineID: execution.Continuation.MachineID,
			MachineVersion: execution.Continuation.MachineVersion, Policy: execution.Continuation.Policy,
		}
		if err := h.store.RecordContinuationMarker(ctx, marker); err != nil {
			return transition.CommitReceipt{}, err
		}
	}
	return receipt, nil
}

func (h *Host) ContinuationCommitSupported() bool { return true }

func (h *Host) commitTransaction(ctx context.Context, tx *sql.Tx, execution transition.Execution[string, State]) (transition.CommitReceipt, error) {
	var priorOperation string
	var priorState string
	var priorRevision int64
	err := tx.QueryRowContext(ctx, `SELECT operation_id, state, revision FROM renewal_commit_receipts WHERE execution_id = ?`, execution.ID).Scan(&priorOperation, &priorState, &priorRevision)
	if err == nil {
		return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: priorOperation, Outcome: transition.CommitSucceeded}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return transition.CommitReceipt{}, err
	}
	var planPeriod, state string
	var revision int64
	err = tx.QueryRowContext(ctx, h.lockQuery(`SELECT period_id, state, revision FROM renewal_plans WHERE plan_id = ?`),
		execution.Entity).Scan(&planPeriod, &state, &revision)
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	if execution.HasPrevious && State(state) != execution.Previous || !execution.HasPrevious && state != "" {
		return rejectedReceipt(execution, "stale_origin"), nil
	}
	var fenceID string
	var fenceRevision int64
	err = tx.QueryRowContext(ctx, h.lockQuery(`SELECT execution_id, origin_revision FROM renewal_fences WHERE plan_id = ?`), execution.Entity).Scan(&fenceID, &fenceRevision)
	if err == nil && (fenceID != execution.ID || fenceRevision != revision) {
		return rejectedReceipt(execution, "host_fence_held"), nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return transition.CommitReceipt{}, err
	}
	input, err := (jsonCodec[Input]{version: "1"}).Decode(ctx, execution.Input.Data)
	if err != nil || input.PlanID != execution.Entity || input.PeriodID != planPeriod {
		return rejectedReceipt(execution, "renewal_input_mismatch"), nil
	}
	var batchPeriod string
	batchErr := tx.QueryRowContext(ctx, h.lockQuery(`SELECT period_id FROM renewal_batch_runs WHERE batch_id = ?`),
		execution.Entity).Scan(&batchPeriod)
	isBatch := batchErr == nil
	if batchErr != nil && !errors.Is(batchErr, sql.ErrNoRows) {
		return transition.CommitReceipt{}, batchErr
	}
	var batchRecords []string
	if isBatch && execution.Target == Renewed {
		if batchPeriod != planPeriod {
			return rejectedReceipt(execution, "batch_period_mismatch"), nil
		}
		rows, err := tx.QueryContext(ctx, h.lockQuery(`SELECT record_id, status FROM renewal_batch_records WHERE batch_id = ? ORDER BY ordinal`),
			execution.Entity)
		if err != nil {
			return transition.CommitReceipt{}, err
		}
		for rows.Next() {
			var recordID, status string
			if err := rows.Scan(&recordID, &status); err != nil {
				_ = rows.Close()
				return transition.CommitReceipt{}, err
			}
			if status != "complete" {
				_ = rows.Close()
				return rejectedReceipt(execution, "batch_record_incomplete"), nil
			}
			batchRecords = append(batchRecords, recordID)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return transition.CommitReceipt{}, err
		}
		_ = rows.Close()
		if len(batchRecords) == 0 {
			return rejectedReceipt(execution, "batch_empty"), nil
		}
	}
	if execution.Target == Renewed {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_completed_periods WHERE plan_id = ? AND period_id = ?`, execution.Entity, planPeriod).Scan(&count); err != nil {
			return transition.CommitReceipt{}, err
		}
		if count != 0 {
			return rejectedReceipt(execution, "renewal_period_already_completed"), nil
		}
		effectRecords := []string{execution.Entity}
		if isBatch {
			effectRecords = batchRecords
		}
		for _, recordID := range effectRecords {
			var effectStatus string
			if err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT status FROM renewal_period_effects WHERE plan_id = ? AND period_id = ?`),
				recordID, planPeriod).Scan(&effectStatus); err != nil || effectStatus != "charged" {
				return rejectedReceipt(execution, "payment_not_confirmed"), nil
			}
			if isBatch {
				var completed int
				if err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT COUNT(*) FROM renewal_completed_periods WHERE plan_id = ? AND period_id = ?`),
					recordID, planPeriod).Scan(&completed); err != nil {
					return transition.CommitReceipt{}, err
				}
				if completed != 0 {
					return rejectedReceipt(execution, "renewal_period_already_completed"), nil
				}
			}
		}
	}
	if revision < 0 || revision == math.MaxInt64 {
		return rejectedReceipt(execution, "revision_exhausted"), nil
	}
	nextRevision := revision + 1
	result, err := tx.ExecContext(ctx, `UPDATE renewal_plans SET state = ?, revision = ? WHERE plan_id = ? AND state = ? AND revision = ?`,
		execution.Target, nextRevision, execution.Entity, state, revision)
	if err != nil {
		return transition.CommitReceipt{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return rejectedReceipt(execution, "stale_revision"), nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_commit_receipts (execution_id, plan_id, operation_id, state, revision) VALUES (?, ?, ?, ?, ?)`,
		execution.ID, execution.Entity, execution.CommitOperationID, execution.Target, nextRevision); err != nil {
		return transition.CommitReceipt{}, err
	}
	if execution.Target == Renewed {
		if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_completed_periods (plan_id, period_id, execution_id) VALUES (?, ?, ?)`,
			execution.Entity, planPeriod, execution.ID); err != nil {
			return rejectedReceipt(execution, "renewal_period_already_completed"), nil
		}
		for _, recordID := range batchRecords {
			if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_completed_periods (plan_id, period_id, execution_id) VALUES (?, ?, ?)`,
				recordID, planPeriod, execution.ID); err != nil {
				return transition.CommitReceipt{}, err
			}
		}
	}
	if execution.Continuation != nil {
		marker := transition.ContinuationMarker[string, State]{
			SchemaVersion: 1, ExecutionID: execution.ID, OperationID: execution.CommitOperationID,
			Entity: execution.Entity, Target: execution.Target, MachineID: execution.Continuation.MachineID,
			MachineVersion: execution.Continuation.MachineVersion, Policy: execution.Continuation.Policy,
		}
		data, err := json.Marshal(marker)
		if err != nil {
			return transition.CommitReceipt{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_markers (execution_id, marker, acknowledged) VALUES (?, ?, 0)`,
			execution.ID, data); err != nil {
			return transition.CommitReceipt{}, err
		}
	}
	if fenceID == execution.ID {
		if _, err := tx.ExecContext(ctx, `DELETE FROM renewal_fences WHERE plan_id = ? AND execution_id = ?`, execution.Entity, execution.ID); err != nil {
			return transition.CommitReceipt{}, err
		}
	}
	return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitSucceeded}, nil
}

func rejectedReceipt(execution transition.Execution[string, State], code string) transition.CommitReceipt {
	return transition.CommitReceipt{ExecutionID: execution.ID, OperationID: execution.CommitOperationID, Outcome: transition.CommitDenied, Failure: &fsm.Failure{Code: code}}
}

func (h *Host) ListContinuationMarkers(ctx context.Context, cursor string) ([]transition.ContinuationMarker[string, State], string, error) {
	rows, err := h.db.QueryContext(ctx, `SELECT execution_id, marker FROM renewal_markers WHERE acknowledged = 0 AND execution_id > ? ORDER BY execution_id LIMIT 100`, cursor)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var markers []transition.ContinuationMarker[string, State]
	last := cursor
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, "", err
		}
		var marker transition.ContinuationMarker[string, State]
		if err := json.Unmarshal(data, &marker); err != nil {
			return nil, "", err
		}
		if marker.ExecutionID != id {
			return nil, "", transition.ErrConflict
		}
		markers = append(markers, marker)
		last = id
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(markers) < 100 {
		last = ""
	}
	return markers, last, nil
}

func (h *Host) AcknowledgeContinuationMarker(ctx context.Context, marker transition.ContinuationMarker[string, State]) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	result, err := h.db.ExecContext(ctx, `UPDATE renewal_markers SET acknowledged = 1 WHERE execution_id = ? AND marker = ?`, marker.ExecutionID, data)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var acknowledged int
		err := h.db.QueryRowContext(ctx, `SELECT acknowledged FROM renewal_markers WHERE execution_id = ? AND marker = ?`, marker.ExecutionID, data).Scan(&acknowledged)
		if err != nil {
			return err
		}
		if acknowledged != 1 {
			return transition.ErrConflict
		}
	}
	return nil
}

func (h *Host) LookupSuccessor(ctx context.Context, binding transition.SuccessorBinding[string, State]) (transition.SuccessorHostResult[string, State], error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	var data []byte
	var outcome, principal, decision string
	var accepted time.Time
	var cancellation []byte
	query := h.lockQuery(`SELECT binding, outcome, principal, decision, accepted_at, cancellation FROM renewal_successors WHERE origin_execution_id = ?`)
	err = tx.QueryRowContext(ctx, query,
		binding.Origin.ExecutionID).Scan(&data, &outcome, &principal, &decision, &accepted, &cancellation)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return transition.SuccessorHostResult[string, State]{}, err
		}
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostAbsent}, nil
	}
	if err != nil {
		_ = tx.Rollback()
		return transition.SuccessorHostResult[string, State]{}, err
	}
	var stored transition.SuccessorBinding[string, State]
	if err := json.Unmarshal(data, &stored); err != nil {
		_ = tx.Rollback()
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if !reflect.DeepEqual(stored, binding) {
		_ = tx.Rollback()
		return transition.SuccessorHostResult[string, State]{}, transition.ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	switch outcome {
	case "accepted":
		receipt := transition.SuccessorReceipt[string, State]{Binding: stored, Principal: principal, Decision: decision, AcceptedAt: accepted}
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostAccepted, Receipt: &receipt}, nil
	case "cancelled", "superseded":
		canceled := transition.SuccessorCancellation[string, State]{Binding: stored, Evidence: cancellation, Superseded: outcome == "superseded"}
		result := transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostCanceled, Cancellation: &canceled}
		if canceled.Superseded {
			result.Outcome = transition.SuccessorHostSuperseded
		}
		return result, nil
	case "denied":
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostDenied, Principal: principal}, nil
	default:
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostPending}, nil
	}
}

func (h *Host) AcceptContinuationSuccessor(ctx context.Context, binding transition.SuccessorBinding[string, State], permit transition.ContinuationDeliveryPermit) (transition.SuccessorHostResult[string, State], error) {
	if h.beforeSuccessorAccept != nil {
		if err := h.beforeSuccessorAccept(ctx, binding); err != nil {
			return transition.SuccessorHostResult[string, State]{}, err
		}
	}
	now := h.clock()
	if permit.Generation == 0 || !now.Before(permit.ExpiresAt) {
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostPending}, nil
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	result, err := h.acceptSuccessorTx(ctx, tx, binding, permit)
	if err != nil {
		_ = tx.Rollback()
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if err := tx.Commit(); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	return result, nil
}

func (h *Host) acceptSuccessorTx(ctx context.Context, tx *sql.Tx, binding transition.SuccessorBinding[string, State], permit transition.ContinuationDeliveryPermit) (transition.SuccessorHostResult[string, State], error) {
	var data []byte
	var outcome, principal, decision string
	var accepted time.Time
	var cancellation []byte
	query := h.lockQuery(`SELECT binding, outcome, principal, decision, accepted_at, cancellation FROM renewal_successors WHERE origin_execution_id = ?`)
	err := tx.QueryRowContext(ctx, query,
		binding.Origin.ExecutionID).Scan(&data, &outcome, &principal, &decision, &accepted, &cancellation)
	if err == nil {
		var stored transition.SuccessorBinding[string, State]
		if err := json.Unmarshal(data, &stored); err != nil || !reflect.DeepEqual(stored, binding) {
			return transition.SuccessorHostResult[string, State]{}, transition.ErrConflict
		}
		switch outcome {
		case "accepted":
			return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostAccepted,
				Receipt: &transition.SuccessorReceipt[string, State]{Binding: stored, Principal: principal, Decision: decision, AcceptedAt: accepted}}, nil
		case "denied":
			return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostDenied, Principal: principal}, nil
		case "cancelled", "superseded":
			canceled := &transition.SuccessorCancellation[string, State]{Binding: stored, Evidence: cancellation, Superseded: outcome == "superseded"}
			status := transition.SuccessorHostCanceled
			if canceled.Superseded {
				status = transition.SuccessorHostSuperseded
			}
			return transition.SuccessorHostResult[string, State]{Outcome: status, Cancellation: canceled}, nil
		default:
			return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostPending}, nil
		}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if !h.clock().Before(permit.ExpiresAt) {
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostPending}, nil
	}
	originRevision, err := strconv.ParseInt(string(binding.OriginRevision), 10, 64)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, transition.ErrCompatibility
	}
	var currentState, periodID string
	var currentRevision int64
	if err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT state, period_id, revision FROM renewal_plans WHERE plan_id = ?`),
		binding.Origin.Entity).Scan(&currentState, &periodID, &currentRevision); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	var storedRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM renewal_commit_receipts WHERE execution_id = ?`, binding.Origin.ExecutionID).Scan(&storedRevision); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if State(currentState) != binding.Origin.Target || currentRevision != originRevision || storedRevision != originRevision {
		cancellation := transition.SuccessorCancellation[string, State]{
			Binding: binding, Evidence: []byte("origin-revision-advanced"), Superseded: true,
		}
		cancellationData, _ := json.Marshal(cancellation.Evidence)
		bindingData, _ := json.Marshal(binding)
		if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_successors (origin_execution_id, binding, outcome, principal, decision, accepted_at, cancellation) VALUES (?, ?, 'superseded', '', '', ?, ?)`,
			binding.Origin.ExecutionID, bindingData, h.clock(), cancellationData); err != nil {
			return transition.SuccessorHostResult[string, State]{}, err
		}
		return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostSuperseded, Cancellation: &cancellation}, nil
	}
	var completed int
	principal, allowed, err := h.authorizeRenewalWorkload(ctx, tx, binding, periodID)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if !allowed {
		return h.persistSuccessorDenial(ctx, tx, binding, principal)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM renewal_completed_periods WHERE plan_id = ? AND period_id = ?`, binding.Origin.Entity, periodID).Scan(&completed); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if completed != 0 {
		return h.persistSuccessorDenial(ctx, tx, binding, principal)
	}
	bindingData, err := json.Marshal(binding)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	receipt := transition.SuccessorReceipt[string, State]{
		Binding: binding, Principal: principal, Decision: "allowed", AcceptedAt: h.clock(),
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_successors (origin_execution_id, binding, outcome, principal, decision, accepted_at, cancellation) VALUES (?, ?, 'accepted', ?, ?, ?, NULL)`,
		binding.Origin.ExecutionID, bindingData, receipt.Principal, receipt.Decision, receipt.AcceptedAt); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_fences (plan_id, execution_id, origin_revision) VALUES (?, ?, ?)`,
		binding.Origin.Entity, binding.Execution.ID, originRevision); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostAccepted, Receipt: &receipt}, nil
}

func (h *Host) authorizeRenewalWorkload(ctx context.Context, tx *sql.Tx, binding transition.SuccessorBinding[string, State], period string) (string, bool, error) {
	if binding.Execution.Input.Codec != (transition.CodecVersion{Name: "renewal-json", Version: "1"}) {
		return "", false, transition.ErrCompatibility
	}
	var input Input
	if err := json.Unmarshal(binding.Execution.Input.Data, &input); err != nil {
		return "", false, transition.ErrCompatibility
	}
	if input.PlanID != binding.Origin.Entity || input.PeriodID != period || input.Principal == "" {
		return "", false, transition.ErrConflict
	}
	var allowed int
	err := tx.QueryRowContext(ctx, h.lockQuery(`SELECT allowed FROM renewal_workload_grants WHERE plan_id = ? AND principal = ?`),
		input.PlanID, input.Principal).Scan(&allowed)
	if errors.Is(err, sql.ErrNoRows) {
		return input.Principal, false, nil
	}
	if err != nil {
		return "", false, err
	}
	return input.Principal, allowed == 1, nil
}

func (h *Host) persistSuccessorDenial(
	ctx context.Context,
	tx *sql.Tx,
	binding transition.SuccessorBinding[string, State],
	principal string,
) (transition.SuccessorHostResult[string, State], error) {
	bindingData, err := json.Marshal(binding)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_successors (origin_execution_id, binding, outcome, principal, decision, accepted_at, cancellation) VALUES (?, ?, 'denied', ?, 'denied', ?, NULL)`,
		binding.Origin.ExecutionID, bindingData, principal, h.clock()); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostDenied, Principal: principal}, nil
}

func (h *Host) CancelContinuationSuccessor(ctx context.Context, binding transition.SuccessorBinding[string, State]) (transition.SuccessorHostResult[string, State], error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	var data []byte
	var outcome, principal, decision string
	var accepted time.Time
	var cancellationData []byte
	err = tx.QueryRowContext(ctx, h.lockQuery(`SELECT binding, outcome, principal, decision, accepted_at, cancellation FROM renewal_successors WHERE origin_execution_id = ?`),
		binding.Origin.ExecutionID).Scan(&data, &outcome, &principal, &decision, &accepted, &cancellationData)
	if err == nil {
		var stored transition.SuccessorBinding[string, State]
		if decodeErr := json.Unmarshal(data, &stored); decodeErr != nil || !reflect.DeepEqual(stored, binding) {
			_ = tx.Rollback()
			return transition.SuccessorHostResult[string, State]{}, transition.ErrConflict
		}
		_ = tx.Rollback()
		switch outcome {
		case "accepted":
			return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostAccepted,
				Receipt: &transition.SuccessorReceipt[string, State]{Binding: stored, Principal: principal, Decision: decision, AcceptedAt: accepted}}, nil
		case "denied":
			return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostDenied, Principal: principal}, nil
		case "superseded", "cancelled":
			canceled := &transition.SuccessorCancellation[string, State]{Binding: stored, Evidence: cancellationData, Superseded: outcome == "superseded"}
			result := transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostCanceled, Cancellation: canceled}
			if canceled.Superseded {
				result.Outcome = transition.SuccessorHostSuperseded
			}
			return result, nil
		default:
			return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostPending}, nil
		}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO renewal_successors (origin_execution_id, binding, outcome, principal, decision, accepted_at, cancellation) VALUES (?, ?, 'cancelled', '', '', ?, ?)`,
		binding.Origin.ExecutionID, mustJSON(binding), h.clock(), []byte("waiver-tombstone")); err != nil {
		_ = tx.Rollback()
		return transition.SuccessorHostResult[string, State]{}, err
	}
	if err := tx.Commit(); err != nil {
		return transition.SuccessorHostResult[string, State]{}, err
	}
	cancellation := transition.SuccessorCancellation[string, State]{Binding: binding, Evidence: []byte("waiver-tombstone")}
	return transition.SuccessorHostResult[string, State]{Outcome: transition.SuccessorHostCanceled, Cancellation: &cancellation}, nil
}

func (h *Host) lockQuery(query string) string {
	if h.dialect == "mysql" {
		return query + ` FOR UPDATE`
	}
	return query
}

func (h *Host) originRevision(ctx context.Context, marker transition.ContinuationMarker[string, State]) ([]byte, error) {
	var revision int64
	err := h.db.QueryRowContext(ctx, `SELECT revision FROM renewal_commit_receipts WHERE execution_id = ?`, marker.ExecutionID).Scan(&revision)
	if err != nil {
		return nil, err
	}
	return []byte(strconv.FormatInt(revision, 10)), nil
}

func (h *Host) authorizeHookControl(_ context.Context, authorization transition.ControlAuthorization) error {
	if authorization.RequestID == "" || authorization.ExecutionID == "" {
		return transition.ErrInvalidRequest
	}
	return nil
}

func (h *Host) authorizeContinuationControl(_ context.Context, access transition.ContinuationControlAccess) (string, error) {
	if access.Action == transition.InspectContinuation {
		return "operator", nil
	}
	if access.RequestID == "" || access.Reason == "" {
		return "", transition.ErrContinuationControlDenied
	}
	return "operator", nil
}

func (h *Host) authorizeOperationResolution(_ context.Context, request transition.OperationResolutionRequest) (transition.OperationResolutionAuthorization, error) {
	if request.ID == "" {
		return transition.OperationResolutionAuthorization{}, transition.ErrOperationResolutionDenied
	}
	return transition.OperationResolutionAuthorization{Principal: "operator", Reason: "provider evidence reconciled"}, nil
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func isDuplicate(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique") || strings.Contains(message, "duplicate") || strings.Contains(message, "constraint")
}

func hashHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func sameBytes(a, b []byte) bool { return bytes.Equal(a, b) }
