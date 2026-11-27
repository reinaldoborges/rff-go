package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func createNotificationWork(t *testing.T, ctx context.Context, store *sqlite.Store[string, string], id string, now time.Time) {
	t.Helper()
	clock := fixedClock(now)
	_, _, err := store.Accept(ctx, id, id, func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: id, Key: id,
			Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
			Target: "ready", Status: transition.Waiting, Notify: true, AcceptedAt: now, Deadline: now.Add(time.Hour),
			Policy: fsm.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute, OverallTimeout: time.Hour},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireTransitionLease(ctx, id, id, "coordinator", clock, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchHook(ctx, fence, clock, transition.HookDispatch{Kind: transition.HookEnter, DueAt: now}); err != nil {
		t.Fatal(err)
	}
	hook, err := store.ClaimHook(ctx, "hook-worker", clock, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHook(ctx, hook, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.ReportHook(ctx, hook, transition.HookResult{Success: true}, clock); err != nil {
		t.Fatal(err)
	}
	operation, err := store.MarkWorkSuccess(ctx, fence, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitReceipt(ctx, fence, clock, transition.CommitReceipt{
		ExecutionID: id, OperationID: operation, Outcome: transition.CommitSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := store.ReadActiveExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	intent := transition.NotificationIntent[string, string]{
		ID: execution.NotificationIntentID, ExecutionID: execution.ID, CommitOperationID: execution.CommitOperationID,
		Entity: execution.Entity, Operation: execution.Operation, Previous: execution.Previous,
		HasPrevious: execution.HasPrevious, Target: execution.Target,
	}
	if err := outbox.Handoff(ctx, fence, intent); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationProjectionMismatchIsQuarantinedAndAuthorizedRepairIsAudited(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "notification-selection.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 123456789, time.UTC)
	createNotificationWork(t, ctx, store, "notification", now)
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `UPDATE rff_notification_work
		SET due_at_seconds = due_at_seconds - 1 WHERE execution_id = 'notification'`); err != nil {
		t.Fatalf("seed projection mismatch: %v", err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("mismatched candidate projection was not quarantined: %v", err)
	}
	maintenance, err := sqlite.OpenIntegrityMaintenance(path, func(_ context.Context, action, actor, _ string, findingID, locator int64) error {
		if actor != "authorized" || action == "repair" && (findingID <= 0 || locator <= 0) {
			return errors.New("unauthorized maintenance")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "authorized", "inspect notification projection")
	if err != nil || len(findings) != 1 || findings[0].Kind != "notification" || findings[0].Category != "projection_mismatch" {
		t.Fatalf("notification projection finding = %+v, %v", findings, err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE rff_notification_work
		SET due_at_seconds = due_at_seconds - 1 WHERE execution_id = 'notification'`); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.RebuildNotificationProjection(ctx, "authorized", "stale repair", findings[0]); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("stale notification projection observation was accepted: %v", err)
	}
	findings, err = maintenance.Findings(ctx, "authorized", "refresh notification projection observation")
	if err != nil || len(findings) != 1 {
		t.Fatalf("refreshed notification finding = %+v, %v", findings, err)
	}
	if err := maintenance.RebuildNotificationProjection(ctx, "unauthorized", "attempt repair", findings[0]); err == nil {
		t.Fatal("unauthorized notification projection repair succeeded")
	}
	if err := maintenance.RebuildNotificationProjection(ctx, "authorized", "rebuild notification projection", findings[0]); err != nil {
		t.Fatalf("authorized notification projection repair: %v", err)
	}
	audit, err := maintenance.Audit(ctx, "authorized", "verify repair audit", findings[0].ID)
	if err != nil || len(audit) != 1 || audit[0].Action != "rebuild_notification" {
		t.Fatalf("notification repair audit = %+v, %v", audit, err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "notification" || delivery.Attempt != 1 {
		t.Fatalf("repaired notification was not claimable: delivery=%+v error=%v", delivery, err)
	}
}

func TestNotificationCandidateIndexIsRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notification-index.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`DROP INDEX rff_notification_ready_lookup`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{}); !errors.Is(err, sqlite.ErrUnsupportedSchema) {
		t.Fatalf("store opened without required notification candidate index: %v", err)
	}
}

func TestNotificationClaimsUseFullIDOrderAndInclusiveBoundaries(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "notification-order.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 123456789, time.UTC)
	futureID := "0-future"
	createNotificationWork(t, ctx, store, futureID, now)
	outbox, err := sqlite.NewNotificationOutbox(store, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	future, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute)
	if err != nil || future.Intent.ExecutionID != futureID {
		t.Fatalf("initial future-work claim = %+v, %v", future, err)
	}
	due := now.Add(time.Minute)
	if err := outbox.RetryNotificationDelivery(ctx, future.Lease, fixedClock(now), due); err != nil {
		t.Fatal(err)
	}
	firstID, secondID := "a\x00a", "a\x00b"
	createNotificationWork(t, ctx, store, secondID, now)
	createNotificationWork(t, ctx, store, firstID, now)
	first, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute)
	if err != nil || first.Intent.ExecutionID != firstID || first.Attempt != 1 {
		t.Fatalf("first byte-ordered claim = %+v, %v", first, err)
	}
	second, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute)
	if err != nil || second.Intent.ExecutionID != secondID || second.Attempt != 1 {
		t.Fatalf("second byte-ordered claim = %+v, %v", second, err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("future and leased work was claimable early: %v", err)
	}
	equalityClock := fixedClock(due)
	atDue, err := outbox.ClaimNotification(ctx, "delivery", equalityClock, time.Minute)
	if err != nil || atDue.Intent.ExecutionID != futureID || atDue.Attempt != 2 {
		t.Fatalf("due-time equality was not eligible: delivery=%+v error=%v", atDue, err)
	}
	atLeaseExpiry, err := outbox.ClaimNotification(ctx, "delivery", equalityClock, time.Minute)
	if err != nil || atLeaseExpiry.Intent.ExecutionID != firstID || atLeaseExpiry.Attempt != 2 {
		t.Fatalf("lease-expiry equality was not eligible: delivery=%+v error=%v", atLeaseExpiry, err)
	}
}

func TestNotificationClaimsQuarantineSharedExecutionAndReservationFindings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "notification-shared-findings.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b", "c"} {
		createNotificationWork(t, ctx, store, id, now)
	}
	admin, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var executionLocator int64
	if err := admin.QueryRowContext(ctx, `SELECT row_id FROM rff_executions WHERE id = 'a'`).Scan(&executionLocator); err != nil {
		t.Fatal(err)
	}
	detected := now.Format(time.RFC3339Nano)
	if _, err := admin.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at)
		VALUES ('execution', ?, 0, 'invalid_execution', 'record', X'', ?)`, executionLocator, detected); err != nil {
		t.Fatal(err)
	}
	var reservationLocator int64
	if err := admin.QueryRowContext(ctx, `SELECT row_id FROM rff_active WHERE execution_id = 'c'`).Scan(&reservationLocator); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel, detected_at)
		VALUES ('reservation', ?, 0, 'invalid_reservation', 'active', X'', ?)`, reservationLocator, detected); err != nil {
		t.Fatal(err)
	}
	outbox, err := sqlite.NewNotificationOutbox(store, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "b" {
		t.Fatalf("shared execution finding blocked independent work: delivery=%+v error=%v", delivery, err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", fixedClock(now), time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("known reservation finding was hidden by notification selection: %v", err)
	}
	lease := transition.NotificationLease{ExecutionID: "c", IntentID: "c", Owner: "delivery", Generation: 1, ExpiresAt: now.Add(time.Minute)}
	if err := outbox.CompleteNotification(ctx, lease, fixedClock(now)); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("direct notification completion ignored reservation quarantine: %v", err)
	}
	if _, err := outbox.ReadNotificationCompatibility(ctx, "c"); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("notification compatibility read ignored reservation quarantine: %v", err)
	}
}

func TestCompetingNotificationClaimsHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "notification-competition.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	createNotificationWork(t, ctx, store, "notification", now)
	outbox, err := sqlite.NewNotificationOutbox(store, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		delivery transition.NotificationDelivery[string, string]
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		workers.Add(1)
		go func(owner string) {
			defer workers.Done()
			<-start
			delivery, err := outbox.ClaimNotification(ctx, owner, fixedClock(now), time.Minute)
			results <- result{delivery: delivery, err: err}
		}(owner)
	}
	close(start)
	workers.Wait()
	close(results)
	successes, empty := 0, 0
	for result := range results {
		if result.err == nil && result.delivery.Intent.ExecutionID == "notification" {
			successes++
		} else if errors.Is(result.err, transition.ErrNoNotificationActivity) {
			empty++
		} else {
			t.Fatalf("unexpected competing claim result: %+v", result)
		}
	}
	if successes != 1 || empty != 1 {
		t.Fatalf("competing claims = %d successes, %d empty responses", successes, empty)
	}
}
