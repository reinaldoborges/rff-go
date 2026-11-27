package mysql_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/mysql"
	"github.com/reinaldoborges/rff-go/fsm"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func createMySQLNotificationWork(t *testing.T, ctx context.Context, store *mysql.Store[string, string], id, target string, now time.Time) {
	t.Helper()
	clock := func() time.Time { return now }
	_, _, err := store.Accept(ctx, id, id, func(transition.AcceptanceView[string, string]) (*transition.Execution[string, string], error) {
		return &transition.Execution[string, string]{
			SchemaVersion: transition.CurrentSchemaVersion, ID: id, Entity: id, Key: id,
			Operation: transition.Initialize, MachineID: "task", MachineVersion: "1",
			Target: target, Status: transition.Waiting, Notify: true, AcceptedAt: now, Deadline: now.Add(time.Hour),
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
	outbox, err := mysql.NewNotificationOutbox(store, clock)
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

type selectiveMySQLStateCodec struct{ mysql.StringCodec }

func (selectiveMySQLStateCodec) Decode(data []byte) (string, error) {
	if string(data) == "incompatible" {
		return "", errors.New("state is unavailable")
	}
	return string(data), nil
}

func TestIncompatibleNotificationBeforeSuccessPersistsDiagnostic(t *testing.T) {
	ctx := context.Background()
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	createMySQLNotificationWork(t, ctx, store, "a", "incompatible", now)
	createMySQLNotificationWork(t, ctx, store, "b", "compatible", now)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, selectiveMySQLStateCodec{})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := mysql.NewNotificationOutbox(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "b" {
		t.Fatalf("later compatible notification was blocked: delivery=%+v error=%v", delivery, err)
	}
	diagnostic, err := outbox.ReadNotificationCompatibility(ctx, "a")
	if err != nil || diagnostic == nil || diagnostic.Component != transition.CompatibilityState {
		t.Fatalf("prior notification diagnostic was not persisted: diagnostic=%+v error=%v", diagnostic, err)
	}
	if err := outbox.CompleteNotification(ctx, delivery.Lease, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrCompatibility) ||
		errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("blocked notification was mistaken for an empty queue: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err = mysql.NewNotificationOutbox(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	delivery, err = outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "a" {
		t.Fatalf("restored notification was not claimable: delivery=%+v error=%v", delivery, err)
	}
	diagnostic, err = outbox.ReadNotificationCompatibility(ctx, "a")
	if err != nil || diagnostic != nil {
		t.Fatalf("restored notification retained compatibility block: diagnostic=%+v error=%v", diagnostic, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationProjectionMismatchIsQuarantinedAndAuthorizedRepairIsAudited(t *testing.T) {
	ctx := context.Background()
	admin, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 123456789, time.UTC)
	createMySQLNotificationWork(t, ctx, store, "notification", "ready", now)
	if _, err := admin.ExecContext(ctx, `UPDATE rff_notification_work
		SET due_at_seconds = due_at_seconds - 1 WHERE execution_id = 'notification'`); err != nil {
		t.Fatalf("seed projection mismatch: %v", err)
	}
	outbox, err := mysql.NewNotificationOutbox(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("mismatched candidate projection was not quarantined: %v", err)
	}
	maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(_ context.Context, action, actor, _ string, findingID, locator int64) error {
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
	delivery, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "notification" || delivery.Attempt != 1 {
		t.Fatalf("repaired notification was not claimable: delivery=%+v error=%v", delivery, err)
	}
}

func TestNotificationCandidateIndexIsRequired(t *testing.T) {
	admin, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(t.Context(), `ALTER TABLE rff_notification_work DROP INDEX rff_notification_ready_lookup`); err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{}); !errors.Is(err, mysql.ErrUnsupportedSchema) {
		t.Fatalf("store opened without required notification candidate index: %v", err)
	}
}

func TestNotificationClaimsUseFullIDOrderAndInclusiveBoundaries(t *testing.T) {
	ctx := context.Background()
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 123456789, time.UTC)
	clock := func() time.Time { return now }
	futureID := "0-future"
	createMySQLNotificationWork(t, ctx, store, futureID, "ready", now)
	outbox, err := mysql.NewNotificationOutbox(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	future, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || future.Intent.ExecutionID != futureID {
		t.Fatalf("initial future-work claim = %+v, %v", future, err)
	}
	due := now.Add(time.Minute)
	if err := outbox.RetryNotificationDelivery(ctx, future.Lease, clock, due); err != nil {
		t.Fatal(err)
	}
	firstID, secondID := "a\x00a", "a\x00b"
	createMySQLNotificationWork(t, ctx, store, secondID, "ready", now)
	createMySQLNotificationWork(t, ctx, store, firstID, "ready", now)
	first, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || first.Intent.ExecutionID != firstID || first.Attempt != 1 {
		t.Fatalf("first byte-ordered claim = %+v, %v", first, err)
	}
	second, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || second.Intent.ExecutionID != secondID || second.Attempt != 1 {
		t.Fatalf("second byte-ordered claim = %+v, %v", second, err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute); !errors.Is(err, transition.ErrNoNotificationActivity) {
		t.Fatalf("future and leased work was claimable early: %v", err)
	}
	now = due
	atDue, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || atDue.Intent.ExecutionID != futureID || atDue.Attempt != 2 {
		t.Fatalf("due-time equality was not eligible: delivery=%+v error=%v", atDue, err)
	}
	atLeaseExpiry, err := outbox.ClaimNotification(ctx, "delivery", clock, time.Minute)
	if err != nil || atLeaseExpiry.Intent.ExecutionID != firstID || atLeaseExpiry.Attempt != 2 {
		t.Fatalf("lease-expiry equality was not eligible: delivery=%+v error=%v", atLeaseExpiry, err)
	}
}

func TestNotificationClaimsQuarantineSharedExecutionAndReservationFindings(t *testing.T) {
	ctx := context.Background()
	admin, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b", "c"} {
		createMySQLNotificationWork(t, ctx, store, id, "ready", now)
	}
	var executionLocator int64
	if err := admin.QueryRowContext(ctx, `SELECT row_id FROM rff_executions WHERE id = 'a'`).Scan(&executionLocator); err != nil {
		t.Fatal(err)
	}
	detected := now.Format(time.RFC3339Nano)
	if _, err := admin.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel,
		detected_at, resolved_at, open_marker) VALUES ('execution', ?, 0, 'invalid_execution', 'record', _binary'', ?, '', 0)`,
		executionLocator, detected); err != nil {
		t.Fatal(err)
	}
	var reservationLocator int64
	if err := admin.QueryRowContext(ctx, `SELECT row_id FROM rff_active WHERE execution_id = 'c'`).Scan(&reservationLocator); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO rff_integrity_findings(kind, locator, finding_revision, category, field, scope_channel,
		detected_at, resolved_at, open_marker) VALUES ('reservation', ?, 0, 'invalid_reservation', 'active', _binary'', ?, '', 0)`,
		reservationLocator, detected); err != nil {
		t.Fatal(err)
	}
	outbox, err := mysql.NewNotificationOutbox(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "b" {
		t.Fatalf("shared execution finding blocked independent work: delivery=%+v error=%v", delivery, err)
	}
	if _, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("known reservation finding was hidden by notification selection: %v", err)
	}
	lease := transition.NotificationLease{ExecutionID: "c", IntentID: "c", Owner: "delivery", Generation: 1, ExpiresAt: now.Add(time.Minute)}
	if err := outbox.CompleteNotification(ctx, lease, func() time.Time { return now }); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("direct notification completion ignored reservation quarantine: %v", err)
	}
	if _, err := outbox.ReadNotificationCompatibility(ctx, "c"); !errors.Is(err, transition.ErrIntegrity) {
		t.Fatalf("notification compatibility read ignored reservation quarantine: %v", err)
	}
}

func TestNewNotificationExecutionFindingFailsBeforeLaterClaim(t *testing.T) {
	ctx := context.Background()
	admin, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	createMySQLNotificationWork(t, ctx, store, "a", "ready", now)
	createMySQLNotificationWork(t, ctx, store, "b", "ready", now)
	if _, err := admin.ExecContext(ctx, `UPDATE rff_executions SET record = X'00' WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	outbox, err := mysql.NewNotificationOutbox(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute)
	if !errors.Is(err, transition.ErrIntegrity) || delivery.Intent.ID != "" {
		t.Fatalf("new finding was hidden by a later claim: delivery=%+v error=%v", delivery, err)
	}
	maintenance, err := mysql.OpenIntegrityMaintenance(config.FormatDSN(), func(_ context.Context, action, actor, reason string, _, _ int64) error {
		if action != "inspect" || actor != "authorized" || reason != "finding check" {
			return errors.New("unauthorized integrity inspection")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	findings, err := maintenance.Findings(ctx, "authorized", "finding check")
	if err != nil || len(findings) != 1 || findings[0].Kind != "execution" {
		t.Fatalf("new execution finding was not committed: %+v, %v", findings, err)
	}
	delivery, err = outbox.ClaimNotification(ctx, "delivery", func() time.Time { return now }, time.Minute)
	if err != nil || delivery.Intent.ExecutionID != "b" {
		t.Fatalf("known quarantine blocked independent notification work: delivery=%+v error=%v", delivery, err)
	}
}

func TestCompetingNotificationClaimsHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	_, config := newMySQLDatabase(t)
	store, err := mysql.Open[string, string](config.FormatDSN(), mysql.StringCodec{}, mysql.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	createMySQLNotificationWork(t, ctx, store, "notification", "ready", now)
	outbox, err := mysql.NewNotificationOutbox(store, func() time.Time { return now })
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
			delivery, err := outbox.ClaimNotification(ctx, owner, func() time.Time { return now }, time.Minute)
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
