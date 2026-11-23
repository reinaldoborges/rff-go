package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/continuationcontract"
	"github.com/reinaldoborges/rff-go/internal/successorcontract"
)

func TestContinuationDriveFencing(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "drive.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveFencing(t, store)
}

func TestContinuationDrivePause(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "pause.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDrivePause(t, store)
}

func TestContinuationDriveStartedLeaseLoss(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "lease-loss.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveStartedLeaseLoss(t, store)
}

func TestContinuationDriveBudget(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "budget.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveBudget(t, store)
}

func TestContinuationDriveWaiver(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "waiver.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveWaiver(t, store)
}

func TestContinuationDriveWaiverPendingAcceptance(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "waiver-pending.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveWaiverPendingAcceptance(t, store, "cancelled")
}

func TestContinuationDriveAcceptedWinsWaiver(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "accepted-waiver.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveWaiverPendingAcceptance(t, store, "accepted")
}

func TestContinuationDriveRetryDeniedSuccessor(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "retry-denial.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveWaiverPendingAcceptance(t, store, "retry-denied")
}

func TestContinuationDriveDeadline(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "deadline.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveDeadline(t, store)
}

func TestContinuationDriveQueuedDeadline(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "queued.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriveQueuedDeadline(t, store)
}

func TestContinuationDriverEvaluation(t *testing.T) {
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "driver.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	continuationcontract.RunDriverEvaluation(t, store)
}

func TestContinuationDriverOneAttempt(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "accepted")
}

func TestContinuationDriverLostAcceptance(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "accepted-lost")
}

func TestContinuationDriverAbsentRetry(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "absent")
}

func TestContinuationDriverWaiveUnknown(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "waive-absent")
}

func TestContinuationDriverPreparationFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "revision-error")
}

func TestContinuationDriverEvaluationTimeout(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "late-evaluation")
}

func TestContinuationDriverAcceptedAfterIntervention(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "denied-accepted")
}

func TestContinuationDriverStaleAbsence(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "stale-absence")
}

func TestContinuationDriverSelectionTimeout(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "late-selection")
}

func TestContinuationDriverStaleDenial(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open[string, string](filepath.Join(dir, "runtime.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	host, err := successorcontract.OpenHost(filepath.Join(dir, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = host.Close() })
	successorcontract.RunDriverOneAttempt(t, store, host, "stale-denial")
}
