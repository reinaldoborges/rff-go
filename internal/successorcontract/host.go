// Package successorcontract provides durable host fixtures for adapter tests.
package successorcontract

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "github.com/mattn/go-sqlite3"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type Binding = transition.SuccessorBinding[string, string]
type Result = transition.SuccessorHostResult[string, string]

// Host is a separate SQLite domain store. Its state-change trigger applies to
// direct SQL writers as well as runtime-originated writes.
type Host struct {
	db        *sql.DB
	Authorize string
	LoseReply bool
}

func OpenHost(path string) (*Host, error) {
	uri := url.URL{Scheme: "file", Path: path}
	q := uri.Query()
	q.Set("_txlock", "immediate")
	q.Set("_busy_timeout", "5000")
	q.Set("_synchronous", "FULL")
	uri.RawQuery = q.Encode()
	db, err := sql.Open("sqlite3", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS domain (entity TEXT PRIMARY KEY, state TEXT NOT NULL, revision BLOB NOT NULL, fence TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS successor_receipts (origin TEXT PRIMARY KEY, binding BLOB NOT NULL, outcome TEXT NOT NULL, principal TEXT NOT NULL, accepted_at TEXT NOT NULL, evidence BLOB NOT NULL);
		CREATE TABLE IF NOT EXISTS host_commits (execution TEXT PRIMARY KEY, operation TEXT NOT NULL, revision BLOB NOT NULL);
		CREATE TRIGGER IF NOT EXISTS fence_domain BEFORE UPDATE OF state, revision ON domain
		WHEN OLD.fence <> '' BEGIN SELECT RAISE(ABORT, 'state is fenced'); END`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Host{db: db, Authorize: "allow"}, nil
}

func (h *Host) Close() error { return h.db.Close() }

func (h *Host) Seed(entity string) error {
	_, err := h.db.Exec(`INSERT INTO domain(entity, state, revision, fence) VALUES (?, 'due', ?, '')`, entity, []byte("revision-1"))
	return err
}

func (h *Host) Write(entity, state, revision string) error {
	result, err := h.db.Exec(`UPDATE domain SET state = ?, revision = ? WHERE entity = ?`, state, []byte(revision), entity)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return errors.New("domain entity missing")
	}
	return nil
}

func bindingBytes(binding Binding) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(binding); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func lookup(ctx context.Context, query func(context.Context, string, ...any) *sql.Row, binding Binding) (Result, error) {
	var data, evidence []byte
	var outcome, principal, acceptedAt string
	err := query(ctx, `SELECT binding, outcome, principal, accepted_at, evidence FROM successor_receipts WHERE origin = ?`, binding.Origin.ExecutionID).
		Scan(&data, &outcome, &principal, &acceptedAt, &evidence)
	if errors.Is(err, sql.ErrNoRows) {
		return Result{Outcome: transition.SuccessorHostAbsent}, nil
	}
	if err != nil {
		return Result{}, err
	}
	expected, err := bindingBytes(binding)
	if err != nil {
		return Result{}, err
	}
	if !bytes.Equal(data, expected) {
		return Result{}, transition.ErrConflict
	}
	switch outcome {
	case "pending":
		return Result{Outcome: transition.SuccessorHostPending}, nil
	case "accepted":
		at, err := time.Parse(time.RFC3339Nano, acceptedAt)
		if err != nil {
			return Result{}, err
		}
		return Result{Outcome: transition.SuccessorHostAccepted, Receipt: &transition.SuccessorReceipt[string, string]{
			Binding: binding, Principal: principal, Decision: "allowed", AcceptedAt: at,
		}}, nil
	case "superseded":
		return Result{Outcome: transition.SuccessorHostSuperseded, Cancellation: &transition.SuccessorCancellation[string, string]{
			Binding: binding, Evidence: bytes.Clone(evidence), Superseded: true,
		}}, nil
	case "cancelled":
		return Result{Outcome: transition.SuccessorHostCanceled, Cancellation: &transition.SuccessorCancellation[string, string]{
			Binding: binding, Evidence: bytes.Clone(evidence),
		}}, nil
	default:
		return Result{}, transition.ErrCompatibility
	}
}

func (h *Host) LookupSuccessor(ctx context.Context, binding Binding) (Result, error) {
	return lookup(ctx, h.db.QueryRowContext, binding)
}

// ClaimSuccessor models an in-flight host claim with a durable writer fence.
func (h *Host) ClaimSuccessor(ctx context.Context, binding Binding) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	prior, err := lookup(ctx, tx.QueryRowContext, binding)
	if err != nil {
		return err
	}
	if prior.Outcome == transition.SuccessorHostPending {
		return nil
	}
	if prior.Outcome != transition.SuccessorHostAbsent {
		return transition.ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE domain SET fence = ? WHERE entity = ? AND revision = ? AND fence = ''`,
		binding.Execution.ID, binding.Origin.Entity, binding.OriginRevision)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return transition.ErrConflict
	}
	data, err := bindingBytes(binding)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO successor_receipts(origin, binding, outcome, principal, accepted_at, evidence) VALUES (?, ?, 'pending', '', '', ?)`,
		binding.Origin.ExecutionID, data, []byte{}); err != nil {
		return err
	}
	return tx.Commit()
}

// CancelPrepared atomically tombstones an unaccepted claim and releases its
// fence. The hook runs inside the transaction to inject interruption.
func (h *Host) CancelPrepared(ctx context.Context, binding Binding, beforeCommit func() error) (Result, error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	prior, err := lookup(ctx, tx.QueryRowContext, binding)
	if err != nil {
		return Result{}, err
	}
	if prior.Outcome == transition.SuccessorHostCanceled {
		return prior, nil
	}
	if prior.Outcome != transition.SuccessorHostPending {
		return Result{}, transition.ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE domain SET fence = '' WHERE entity = ? AND fence = ?`, binding.Origin.Entity, binding.Execution.ID)
	if err != nil {
		return Result{}, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return Result{}, transition.ErrConflict
	}
	evidence := []byte("cancelled:" + binding.Origin.ExecutionID)
	if _, err := tx.ExecContext(ctx, `UPDATE successor_receipts SET outcome = 'cancelled', evidence = ? WHERE origin = ? AND outcome = 'pending'`, evidence, binding.Origin.ExecutionID); err != nil {
		return Result{}, err
	}
	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			return Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	if h.LoseReply {
		h.LoseReply = false
		return Result{}, errors.New("lost host cancellation response")
	}
	return h.LookupSuccessor(ctx, binding)
}

func (h *Host) AcceptSuccessor(ctx context.Context, binding Binding) (Result, error) {
	return h.acceptSuccessor(ctx, binding, nil)
}

func (h *Host) acceptSuccessor(ctx context.Context, binding Binding, check func() error) (Result, error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	prior, err := lookup(ctx, tx.QueryRowContext, binding)
	if err != nil {
		return Result{}, err
	}
	pending := prior.Outcome == transition.SuccessorHostPending
	if prior.Outcome != transition.SuccessorHostAbsent && !pending {
		return prior, nil
	}
	if check != nil {
		if err := check(); err != nil {
			return Result{}, err
		}
	}
	if h.Authorize == "temporary" {
		return Result{}, errors.New("authorization temporarily unavailable")
	}
	if h.Authorize == "deny" {
		return Result{Outcome: transition.SuccessorHostDenied, Principal: "test-worker"}, nil
	}
	if h.Authorize != "allow" {
		return Result{}, transition.ErrContinuationUnavailable
	}
	var revision []byte
	var fence string
	if err := tx.QueryRowContext(ctx, `SELECT revision, fence FROM domain WHERE entity = ?`, binding.Origin.Entity).Scan(&revision, &fence); err != nil {
		return Result{}, err
	}
	if pending && (fence != binding.Execution.ID || !bytes.Equal(revision, binding.OriginRevision)) {
		return Result{}, transition.ErrConflict
	}
	if !pending && fence != "" {
		return Result{}, transition.ErrBusy
	}
	data, err := bindingBytes(binding)
	if err != nil {
		return Result{}, err
	}
	acceptedAt := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	outcome, principal, evidence := "accepted", "test-worker", []byte{}
	if !bytes.Equal(revision, binding.OriginRevision) {
		outcome, principal, evidence = "superseded", "", []byte("host-tombstone")
	} else if !pending {
		if _, err := tx.ExecContext(ctx, `UPDATE domain SET fence = ? WHERE entity = ?`, binding.Execution.ID, binding.Origin.Entity); err != nil {
			return Result{}, err
		}
	}
	if pending {
		if _, err := tx.ExecContext(ctx, `UPDATE successor_receipts SET outcome = ?, principal = ?, accepted_at = ?, evidence = ? WHERE origin = ? AND outcome = 'pending'`,
			outcome, principal, acceptedAt.Format(time.RFC3339Nano), evidence, binding.Origin.ExecutionID); err != nil {
			return Result{}, err
		}
	} else if _, err := tx.ExecContext(ctx, `INSERT INTO successor_receipts(origin, binding, outcome, principal, accepted_at, evidence) VALUES (?, ?, ?, ?, ?, ?)`,
		binding.Origin.ExecutionID, data, outcome, principal, acceptedAt.Format(time.RFC3339Nano), evidence); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	if h.LoseReply {
		h.LoseReply = false
		return Result{}, errors.New("lost host response")
	}
	return h.LookupSuccessor(ctx, binding)
}

// Complete simulates a host commit by the accepted execution: it consumes the
// fence and changes state in one domain transaction.
func (h *Host) Complete(entity, execution, revision, operation string) error {
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var storedOperation string
	var storedRevision []byte
	err = tx.QueryRow(`SELECT operation, revision FROM host_commits WHERE execution = ?`, execution).Scan(&storedOperation, &storedRevision)
	if err == nil {
		if storedOperation != operation || !bytes.Equal(storedRevision, []byte(revision)) {
			return transition.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	result, err := tx.Exec(`UPDATE domain SET fence = '' WHERE entity = ? AND fence = ?`, entity, execution)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("host fence not owned by execution")
	}
	if _, err := tx.Exec(`UPDATE domain SET state = 'paid', revision = ? WHERE entity = ?`, []byte(revision), entity); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO host_commits(execution, operation, revision) VALUES (?, ?, ?)`, execution, operation, []byte(revision)); err != nil {
		return err
	}
	return tx.Commit()
}
