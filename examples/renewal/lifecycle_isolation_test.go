package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestLifecycleDemoIsolatesAndRestartsFailedInputPath(t *testing.T) {
	ctx := context.Background()
	host, err := OpenSQLite(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.demoSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := host.db.ExecContext(ctx, `CREATE TRIGGER reject_input BEFORE INSERT ON renewal_lifecycle_effects
		WHEN NEW.kind = 'input' BEGIN SELECT RAISE(FAIL, 'input unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Start(ctx, "isolation", "period"); err != nil {
		t.Fatal(err)
	}
	if err := host.enqueueDemoInput(ctx, "isolation", "period"); err != nil {
		t.Fatal(err)
	}
	if err := host.enqueueDemoTimer(ctx, "isolation", "period"); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := host.lifecycle()
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Start(ctx); err == nil {
		t.Fatal("expected input scan failure")
	}
	defer lifecycle.Shutdown(ctx)
	wait := func(condition func() bool) {
		t.Helper()
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for !condition() {
			select {
			case <-deadline.C:
				t.Fatalf("progress timeout: %+v", lifecycle.Status())
			case <-tick.C:
			}
		}
	}
	wait(func() bool {
		counts, err := host.demoCounts(ctx, "isolation")
		summary, inspectErr := host.Inspect(ctx, "isolation", "period", "operator")
		return err == nil && inspectErr == nil && counts["timer"] == 1 && summary.State == AwaitingApproval
	})
	if lifecycle.Status().Ready {
		t.Fatal("failed input path reported ready")
	}
	if _, err := host.db.ExecContext(ctx, `DROP TRIGGER reject_input`); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		for _, path := range lifecycle.Status().Paths {
			if path.Name == "input:renewal-demo" {
				return !path.Running
			}
		}
		return false
	})
	if err := lifecycle.RestartPath("input:renewal-demo"); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		counts, err := host.demoCounts(ctx, "isolation")
		return err == nil && counts["input"] == 1 && lifecycle.Status().Ready
	})
	ingress, err := host.demoInput(false)
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[Input]{ID: "input:isolation:period", WorkflowID: "isolation",
		Kind: transition.Command, Value: Input{PlanID: "isolation", PeriodID: "period", Principal: "operator"},
		Codec: (jsonCodec[Input]{version: "1"}).Version()}
	if _, err := ingress.Submit(ctx, request); err != nil {
		t.Fatalf("duplicate input: %v", err)
	}
	request.Value.PeriodID = "changed"
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("changed duplicate: %v", err)
	}
}
