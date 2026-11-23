package mysql_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestContinuationDriveFencing(t *testing.T) {
	continuationcontract.RunDriveFencing(t, openMySQL(t))
}

func TestContinuationDrivePause(t *testing.T) {
	continuationcontract.RunDrivePause(t, openMySQL(t))
}

func TestContinuationDriveStartedLeaseLoss(t *testing.T) {
	continuationcontract.RunDriveStartedLeaseLoss(t, openMySQL(t))
}

func TestContinuationDriveBudget(t *testing.T) {
	continuationcontract.RunDriveBudget(t, openMySQL(t))
}

func TestContinuationDriveWaiver(t *testing.T) {
	continuationcontract.RunDriveWaiver(t, openMySQL(t))
}

func TestContinuationDriveWaiverPendingAcceptance(t *testing.T) {
	continuationcontract.RunDriveWaiverPendingAcceptance(t, openMySQL(t), "cancelled")
}

func TestContinuationDriveAcceptedWinsWaiver(t *testing.T) {
	continuationcontract.RunDriveWaiverPendingAcceptance(t, openMySQL(t), "accepted")
}

func TestContinuationDriveRetryDeniedSuccessor(t *testing.T) {
	continuationcontract.RunDriveWaiverPendingAcceptance(t, openMySQL(t), "retry-denied")
}

func TestContinuationDriveDeadline(t *testing.T) {
	continuationcontract.RunDriveDeadline(t, openMySQL(t))
}

func TestContinuationDriveQueuedDeadline(t *testing.T) {
	continuationcontract.RunDriveQueuedDeadline(t, openMySQL(t))
}

func TestContinuationDriverEvaluation(t *testing.T) {
	continuationcontract.RunDriverEvaluation(t, openMySQL(t))
}

func TestContinuationDriverOneAttempt(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "accepted")
}

func TestContinuationDriverLostAcceptance(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "accepted-lost")
}

func TestContinuationDriverAbsentRetry(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "absent")
}

func TestContinuationDriverWaiveUnknown(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "waive-absent")
}

func TestContinuationDriverPreparationFailure(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "revision-error")
}

func TestContinuationDriverEvaluationTimeout(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "late-evaluation")
}

func TestContinuationDriverAcceptedAfterIntervention(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "denied-accepted")
}

func TestContinuationDriverStaleAbsence(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "stale-absence")
}

func TestContinuationDriverSelectionTimeout(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "late-selection")
}

func TestContinuationDriverStaleDenial(t *testing.T) {
	host, err := successorcontract.OpenHost(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, openMySQL(t), host, "stale-denial")
}
