// Package taskmanager provides the internal first-slice conformance fixture.
package taskmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

var (
	ErrDenied         = errors.New("task-manager access denied")
	ErrConflict       = errors.New("task-manager request conflicts with current state")
	ErrInvalidRequest = errors.New("invalid task-manager request")
	ErrNotFound       = errors.New("task-manager record not found")
)

type Group struct {
	ID     string
	Owner  string
	Closed bool
}

// Fixture is an application-owned SQLite domain store for local conformance.
type Fixture struct {
	db       *sql.DB
	clock    func() time.Time
	store    *sqlite.Store[string, string]
	machines fsm.Registry[string, string, string, membershipInput]
	runner   *transition.HookRunner[string, string, string, membershipInput]
}

func Open(path string, clock func() time.Time) (*Fixture, error) {
	if path == "" || path == ":memory:" || clock == nil {
		return nil, ErrInvalidRequest
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve fixture path: %w", err)
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
		return nil, fmt.Errorf("open fixture: %w", err)
	}
	db.SetMaxOpenConns(1)
	fixture := &Fixture{db: db, clock: clock}
	if err := fixture.transaction(context.Background(), func(tx *sql.Tx) error {
		for _, statement := range []string{
			`CREATE TABLE IF NOT EXISTS tm_users (id TEXT PRIMARY KEY NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tm_groups (id TEXT PRIMARY KEY NOT NULL, owner_id TEXT NOT NULL REFERENCES tm_users(id), closed INTEGER NOT NULL CHECK (closed IN (0, 1)))`,
			`CREATE TABLE IF NOT EXISTS tm_memberships (id TEXT PRIMARY KEY NOT NULL, group_id TEXT NOT NULL REFERENCES tm_groups(id), user_id TEXT NOT NULL REFERENCES tm_users(id), status TEXT NOT NULL, deadline TEXT, reason TEXT NOT NULL)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS tm_live_membership ON tm_memberships (group_id, user_id) WHERE status IN ('Invited', 'Active')`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("initialize fixture schema: %w", err)
			}
		}
		return nil
	}); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	fixture.store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := fixture.initializeMembershipRuntime(); err != nil {
		return nil, errors.Join(err, fixture.store.Close(), db.Close())
	}
	return fixture, nil
}

func (f *Fixture) Close() error { return errors.Join(f.store.Close(), f.db.Close()) }

func (f *Fixture) transaction(ctx context.Context, run func(*sql.Tx) error) (err error) {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fixture transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback fixture transaction: %w", rollbackErr))
		}
	}()
	if err := run(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fixture transaction: %w", err)
	}
	return nil
}

// SeedUsers provides opaque local test identities, not authentication.
func (f *Fixture) SeedUsers(ctx context.Context, users ...string) error {
	for _, user := range users {
		if user == "" {
			return ErrInvalidRequest
		}
	}
	return f.transaction(ctx, func(tx *sql.Tx) error {
		for _, user := range users {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO tm_users (id) VALUES (?)`, user); err != nil {
				return fmt.Errorf("seed user: %w", err)
			}
		}
		return nil
	})
}
