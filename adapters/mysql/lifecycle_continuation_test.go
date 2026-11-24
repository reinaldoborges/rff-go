package mysql_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestLifecycleContinuationLostAcceptanceAndRestart(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunLifecycleContinuation(t, openMySQL(t), host)
}
