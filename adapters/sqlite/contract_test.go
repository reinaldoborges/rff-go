package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/acceptancecontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type faultStore struct {
	store  *sqlite.Store[string, string]
	db     *sql.DB
	before atomic.Bool
	after  atomic.Bool
}

func (s *faultStore) Accept(ctx context.Context, entity, key string, decide transition.AcceptanceDecisionFunc[string, string]) (transition.Execution[string, string], bool, error) {
	if s.before.Swap(false) {
		if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER fail_accept BEFORE INSERT ON rff_executions BEGIN SELECT RAISE(FAIL, 'injected storage failure'); END`); err != nil {
			return transition.Execution[string, string]{}, false, err
		}
		record, created, err := s.store.Accept(ctx, entity, key, decide)
		_, cleanupErr := s.db.ExecContext(ctx, `DROP TRIGGER fail_accept`)
		return record, created, errors.Join(err, cleanupErr)
	}
	record, created, err := s.store.Accept(ctx, entity, key, decide)
	if err == nil && created && s.after.Swap(false) {
		return transition.Execution[string, string]{}, false, errors.New("injected lost commit response")
	}
	return record, created, err
}

func TestAcceptanceStoreContract(t *testing.T) {
	acceptancecontract.Run(t, func(t *testing.T) acceptancecontract.Fixture {
		path := filepath.Join(t.TempDir(), "contract.db")
		store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := errors.Join(store.Close(), db.Close()); err != nil {
				t.Error(err)
			}
		})
		wrapped := &faultStore{store: store, db: db}
		return acceptancecontract.Fixture{
			Store:           wrapped,
			FailNextCommit:  func() { wrapped.before.Store(true) },
			FailAfterCommit: func() { wrapped.after.Store(true) },
		}
	})
}
