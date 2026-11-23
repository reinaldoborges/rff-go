package sqlite

import (
	"database/sql"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
)

func TestMigrationWriteProcessCrash(t *testing.T) {
	child := os.Getenv(runtimecontract.MigrationWriteCrashAddress) != ""
	if child {
		sql.Register("rff-migration-crash", &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return conn.RegisterFunc("rff_migration_crash", func() int { os.Exit(72); return 0 }, true)
		}})
	}
	open := func(path string) (runtimecontract.MigrationRestartStore, error) {
		store, err := Open[string, string](path, StringCodec{}, StringCodec{})
		if err != nil || !child {
			return store, err
		}
		if err := store.db.Close(); err != nil {
			return nil, err
		}
		uri := url.URL{Scheme: "file", Path: path}
		query := uri.Query()
		query.Set("mode", "rwc")
		query.Set("_txlock", "immediate")
		query.Set("_busy_timeout", "5000")
		query.Set("_foreign_keys", "on")
		query.Set("_synchronous", "FULL")
		uri.RawQuery = query.Encode()
		store.db, err = sql.Open("rff-migration-crash", uri.String())
		if err != nil {
			return nil, err
		}
		store.db.SetMaxOpenConns(1)
		return store, nil
	}
	if child {
		runtimecontract.MigrationRestartChild(t, open)
		return
	}
	for _, step := range []struct{ name, trigger string }{
		{"after-hook", "BEFORE UPDATE ON rff_transition_leases"},
		{"after-lease", "BEFORE UPDATE ON rff_executions"},
		{"after-snapshot", "AFTER UPDATE ON rff_executions"},
	} {
		t.Run(step.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "migration.db")
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			install := func() error {
				_, err := db.Exec("CREATE TRIGGER rff_crash " + step.trigger + " BEGIN SELECT rff_migration_crash(); END")
				return err
			}
			remove := func() error {
				_, err := db.Exec("DROP TRIGGER rff_crash")
				return err
			}
			crash := func(cmd *exec.Cmd) error {
				err := cmd.Wait()
				var exit *exec.ExitError
				if errors.As(err, &exit) && exit.ExitCode() == 72 {
					return nil
				}
				return errors.New("child did not reach the crash trigger")
			}
			runtimecontract.RunMigrationWriteCrash(t, path, open, install, crash, remove)
		})
	}
}
