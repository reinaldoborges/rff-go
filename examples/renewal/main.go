package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

func run() error {
	var (
		action     = flag.String("action", "demo", "demo, start, approve, configure, charge-once, drive, late-call, resolve, retry, abort, reattempt, or inspect")
		backend    = flag.String("backend", "sqlite", "sqlite or mysql")
		dbPath     = flag.String("db", "renewal.db", "SQLite database path")
		hostPath   = flag.String("host-db", "", "separate host-owned SQLite database path")
		runtimeDSN = flag.String("dsn", os.Getenv("RFF_MYSQL_DSN"), "MySQL runtime DSN")
		hostDSN    = flag.String("host-dsn", "", "separate host-owned MySQL DSN")
		plan       = flag.String("plan", "demo-plan", "plan entity ID")
		period     = flag.String("period", "2026-11", "renewal period ID")
		mode       = flag.String("provider-mode", "normal", "normal, lost-response, late-call, or fail-after-charge")
		principal  = flag.String("principal", "operator", "host principal")
		requestID  = flag.String("request-id", "", "stable request identity for retry or abort")
	)
	flag.Parse()

	var (
		host *Host
		err  error
	)
	switch *backend {
	case "sqlite":
		if *hostPath != "" {
			host, err = OpenSQLiteSeparate(*dbPath, *hostPath)
		} else {
			host, err = OpenSQLite(*dbPath)
		}
	case "mysql":
		if *runtimeDSN == "" {
			return errors.New("-dsn or RFF_MYSQL_DSN is required for MySQL")
		}
		if *hostDSN != "" {
			host, err = OpenMySQLSeparate(*runtimeDSN, *hostDSN)
		} else {
			host, err = OpenMySQL(*runtimeDSN)
		}
	default:
		return fmt.Errorf("unsupported backend %q", *backend)
	}
	if err != nil {
		return err
	}
	defer host.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	switch *action {
	case "demo":
		if _, err := host.Start(ctx, *plan, *period); err != nil {
			return err
		}
		if err := host.Drive(ctx); err != nil {
			return err
		}
		if _, err := host.Approve(ctx, *plan, *period, *principal); err != nil {
			return err
		}
		if err := host.Drive(ctx); err != nil {
			return err
		}
	case "start":
		if _, err := host.Start(ctx, *plan, *period); err != nil {
			return err
		}
		if err := host.Drive(ctx); err != nil {
			return err
		}
	case "approve":
		if _, err := host.Approve(ctx, *plan, *period, *principal); err != nil {
			return err
		}
	case "configure":
		if err := host.SetProviderMode(ctx, *plan, providerMode(*mode)); err != nil {
			return err
		}
	case "charge-once":
		if err := host.SetProviderMode(ctx, *plan, providerMode(*mode)); err != nil {
			return err
		}
		if err := host.DriveUntilCharge(ctx, *plan); err != nil {
			return err
		}
	case "drive":
		if err := host.Drive(ctx); err != nil {
			return err
		}
	case "late-call":
		if err := host.ApplyLateProviderCall(ctx, *plan, *period); err != nil {
			return err
		}
	case "resolve":
		if err := host.ResolveCharge(ctx, *plan, *principal); err != nil {
			return err
		}
	case "retry":
		if err := host.Retry(ctx, *plan, *principal, *requestID); err != nil {
			return err
		}
	case "abort":
		if err := host.Abort(ctx, *plan, *principal, *requestID); err != nil {
			return err
		}
	case "reattempt":
		if _, err := host.Reattempt(ctx, *plan, *period, *principal); err != nil {
			return err
		}
	default:
		if *action != "inspect" {
			return fmt.Errorf("unsupported action %q", *action)
		}
	}
	summary, err := host.Inspect(ctx, *plan, *period, *principal)
	if err != nil {
		return err
	}
	fmt.Printf("plan=%s period=%s state=%s states=%v revision=%d charge=%s charges=%d refunds=%d phase=%s continuations=%v failure=%s\n",
		summary.PlanID, summary.PeriodID, summary.State, summary.History, summary.Revision,
		chargeStatus(summary.Charges), summary.Charges, summary.Refunds,
		summary.ActiveStatus, summary.Continuations, summary.FailureCode)
	return nil
}

func chargeStatus(count int) string {
	if count > 0 {
		return "confirmed"
	}
	return "none"
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
