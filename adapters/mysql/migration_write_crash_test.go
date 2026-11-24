package mysql_test

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
)

func TestMigrationWriteProcessCrash(t *testing.T) {
	open := func(dsn string) (runtimecontract.MigrationRestartStore, error) {
		return mysql.Open[string, string](dsn, mysql.StringCodec{}, mysql.StringCodec{})
	}
	if os.Getenv(runtimecontract.MigrationWriteCrashAddress) != "" {
		runtimecontract.MigrationRestartChild(t, open)
		return
	}
	for _, step := range []struct{ name, trigger string }{
		{"after-hook", "BEFORE UPDATE ON rff_transition_leases"},
		{"after-lease", "BEFORE UPDATE ON rff_executions"},
		{"after-snapshot", "AFTER UPDATE ON rff_executions"},
	} {
		t.Run(step.name, func(t *testing.T) {
			db, config := newMySQLDatabase(t)
			install := func() error {
				_, err := db.Exec("CREATE TRIGGER rff_crash " + step.trigger + " FOR EACH ROW DO SLEEP(5)")
				return err
			}
			remove := func() error {
				_, err := db.Exec("DROP TRIGGER rff_crash")
				return err
			}
			crash := func(cmd *exec.Cmd) error {
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					var count int
					if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.processlist WHERE db = ? AND state = 'User sleep'", config.DBName).Scan(&count); err != nil {
						_ = cmd.Process.Kill()
						_ = cmd.Wait()
						return err
					}
					if count > 0 {
						if err := cmd.Process.Kill(); err != nil {
							return err
						}
						_ = cmd.Wait()
						return nil
					}
					time.Sleep(20 * time.Millisecond)
				}
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				return fmt.Errorf("child never reached the write trigger")
			}
			runtimecontract.RunMigrationWriteCrash(t, config.FormatDSN(), open, install, crash, remove)
		})
	}
}
