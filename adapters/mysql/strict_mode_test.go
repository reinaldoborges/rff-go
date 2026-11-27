package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestStrictModeOnNewAndReusedConnections(t *testing.T) {
	ctx := context.Background()
	db, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	admin, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var original string
	if err := admin.QueryRowContext(ctx, `SELECT @@GLOBAL.sql_mode`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(ctx, `SET GLOBAL sql_mode = ?`, original); err != nil {
			t.Errorf("restore global SQL mode: %v", err)
		}
	})
	entered, release := make(chan int64), make(chan struct{})
	rollback := errors.New("roll back held transaction")
	done := make(chan error, 1)
	go func() {
		_, _, err := store.AcceptAtomic(ctx, "held", "held", func(ctx context.Context, tx *sql.Tx, _ transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
			var id int64
			if err := tx.QueryRowContext(ctx, `SELECT CONNECTION_ID()`).Scan(&id); err != nil {
				return nil, err
			}
			entered <- id
			<-release
			return nil, rollback
		})
		done <- err
	}()
	var held int64
	select {
	case held = <-entered:
	case err := <-done:
		t.Fatalf("hold connection: %v", err)
	}
	var adminID int64
	if err := admin.QueryRowContext(ctx, `SELECT CONNECTION_ID()`).Scan(&adminID); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	rows, err := admin.QueryContext(ctx, `SELECT id FROM information_schema.processlist WHERE db = DATABASE() AND id NOT IN (?, ?)`, held, adminID)
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	var idle []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			close(release)
			<-done
			t.Fatal(err)
		}
		idle = append(idle, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	for _, id := range idle {
		if _, err := admin.ExecContext(ctx, fmt.Sprintf("KILL %d", id)); err != nil {
			close(release)
			<-done
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(ctx, `SET GLOBAL sql_mode = ''`); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	called := false
	claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, _, err = store.AcceptInput(claimCtx, "new-connection", func(*transition.InputRecord) (*transition.InputRecord, error) {
		called = true
		return nil, nil
	})
	cancel()
	if _, restoreErr := admin.ExecContext(ctx, `SET GLOBAL sql_mode = ?`, original); restoreErr != nil {
		close(release)
		<-done
		t.Fatal(restoreErr)
	}
	close(release)
	if heldErr := <-done; !errors.Is(heldErr, rollback) {
		t.Fatalf("held transaction: %v", heldErr)
	}
	if !errors.Is(err, mysql.ErrUnsupportedSQLMode) || called {
		t.Fatalf("new non-strict connection accepted: error=%v callback=%v", err, called)
	}

	_, _, err = store.AcceptAtomic(ctx, "reset", "reset", func(ctx context.Context, tx *sql.Tx, _ transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		if _, err := tx.ExecContext(ctx, `SET SESSION sql_mode = ''`); err != nil {
			return nil, err
		}
		return nil, rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("alter session: %v", err)
	}
	_, _, err = store.AcceptAtomic(ctx, "reused", "reused", func(ctx context.Context, tx *sql.Tx, _ transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		var mode string
		if err := tx.QueryRowContext(ctx, `SELECT @@SESSION.sql_mode`).Scan(&mode); err != nil {
			return nil, err
		}
		if !strings.Contains(mode, "STRICT_TRANS_TABLES") && !strings.Contains(mode, "STRICT_ALL_TABLES") {
			return nil, errors.New("reused non-strict connection")
		}
		return nil, rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("connection reuse did not restore strict mode: %v", err)
	}
}

func TestOpenRejectsNonStrictConnection(t *testing.T) {
	_, config := newMySQLDatabase(t)
	if config.Params == nil {
		config.Params = make(map[string]string)
	}
	config.Params["sql_mode"] = "''"
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err == nil {
		store.Close()
		t.Fatal("non-strict connection accepted")
	}
	if !strings.Contains(err.Error(), "strict") {
		t.Fatalf("unexpected error: %v", err)
	}
}
