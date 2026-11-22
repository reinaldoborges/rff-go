package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestTimerSchedulePersistsAndDeduplicatesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "timers.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version != (transition.CodecVersion{Name: "event", Version: "1"}) {
			return nil, errors.New("unknown codec")
		}
		return stringValueCodec{}, nil
	}
	newScheduler := func() *transition.TimerScheduler[string] {
		t.Helper()
		scheduler, err := transition.NewTimerScheduler[string](store, resolve)
		if err != nil {
			t.Fatal(err)
		}
		return scheduler
	}
	request := transition.TimerRequest[string]{
		ID: "expiry-1", WorkflowID: "membership-1", Deadline: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Event: "expired", EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}
	first, created, err := newScheduler().Schedule(ctx, request)
	if err != nil || !created || first.Deadline != request.Deadline {
		t.Fatalf("schedule timer: timer=%+v created=%v error=%v", first, created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	replayed, created, err := newScheduler().Schedule(ctx, request)
	if err != nil || created || replayed != first {
		t.Fatalf("timer replay did not preserve original outcome: timer=%+v created=%v error=%v", replayed, created, err)
	}
	request.Event = "different"
	if _, _, err := newScheduler().Schedule(ctx, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("different timer payload did not conflict: %v", err)
	}
	request.Event = "expired"
	request.Deadline = request.Deadline.Add(time.Second)
	if _, _, err := newScheduler().Schedule(ctx, request); !errors.Is(err, transition.ErrConflict) {
		t.Fatalf("different timer deadline did not conflict: %v", err)
	}
}

func TestTimerDeliveryUsesPersistedDeadlineAndRecoversUnknownOutcome(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "delivery.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	request := transition.TimerRequest[string]{
		ID: "expiry", WorkflowID: "membership", Deadline: deadline, Event: "expired",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}
	if _, _, err := scheduler.Schedule(ctx, request); err != nil {
		t.Fatal(err)
	}
	clock := &timerTestClock{now: deadline.Add(-time.Nanosecond), ticks: make(chan time.Time, 1)}
	calls := 0
	delivered := make(map[string]transition.TimerDelivery[string])
	unknown := errors.New("response lost")
	handler := func(_ context.Context, delivery transition.TimerDelivery[string]) error {
		calls++
		delivered[delivery.ID] = delivery
		if calls == 1 {
			return unknown
		}
		return nil
	}
	newWorker := func() *transition.TimerWorker[string] {
		t.Helper()
		worker, err := transition.NewTimerWorker[string](store, resolve, handler, transition.TimerWorkerConfig{
			ID: "timer-worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}
	worker := newWorker()
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrNoDueTimer) || calls != 0 {
		t.Fatalf("timer fired before its deadline: calls=%d error=%v", calls, err)
	}
	clock.advance(time.Nanosecond)
	if err := worker.RunOne(ctx); !errors.Is(err, unknown) || calls != 1 {
		t.Fatalf("timer did not fire at deadline: calls=%d error=%v", calls, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	clock.advance(10 * time.Minute)
	if err := newWorker().RunOne(ctx); err != nil {
		t.Fatalf("recover timer delivery: %v", err)
	}
	if calls != 2 || len(delivered) != 1 || delivered["expiry"].Event != "expired" || !delivered["expiry"].Deadline.Equal(deadline) {
		t.Fatalf("delivery changed its identity or deadline: calls=%d deliveries=%+v", calls, delivered)
	}
	if err := newWorker().RunOne(ctx); !errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("completed timer was delivered again: %v", err)
	}
	scheduler, err = transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := scheduler.Schedule(ctx, request); err != nil || created {
		t.Fatalf("completed timer scheduling did not replay: created=%v error=%v", created, err)
	}
}

func TestIncompatibleTimerDoesNotStarveOtherDueTimersAndRecovers(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "compatibility.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	available := true
	resolve := func(version transition.CodecVersion) (transition.Codec[string], error) {
		if version.Name != "event" || (version.Version != "1" && version.Version != "2") ||
			version.Version == "1" && !available {
			return nil, transition.ErrCodecNotFound
		}
		return stringValueCodec{}, nil
	}
	clock := &timerTestClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), ticks: make(chan time.Time, 1)}
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	for _, timer := range []struct{ id, version string }{{"a", "1"}, {"b", "2"}} {
		if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
			ID: timer.id, WorkflowID: timer.id, Deadline: clock.Now(), Event: "expired",
			EventCodec: transition.CodecVersion{Name: "event", Version: timer.version},
		}); err != nil {
			t.Fatal(err)
		}
	}
	available = false
	var deliveries []transition.TimerDelivery[string]
	newWorker := func() *transition.TimerWorker[string] {
		t.Helper()
		worker, err := transition.NewTimerWorker[string](store, resolve, func(_ context.Context, delivery transition.TimerDelivery[string]) error {
			deliveries = append(deliveries, delivery)
			return nil
		}, transition.TimerWorkerConfig{ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}
	if err := newWorker().RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("unavailable timer codec was not reported: %v", err)
	}
	blocked, err := store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 1 || blocked[0].ID != "a" || blocked[0].Compatibility == nil ||
		blocked[0].Compatibility.Component != transition.CompatibilityEventCodec ||
		blocked[0].Compatibility.Name != "event" || blocked[0].Compatibility.Version != "1" {
		t.Fatalf("timer compatibility diagnostic was not retained: %+v, %v", blocked, err)
	}
	clock.advance(2 * time.Minute)
	if err := newWorker().RunOne(ctx); err != nil || len(deliveries) != 1 || deliveries[0].ID != "b" {
		t.Fatalf("unavailable timer blocked compatible timer: deliveries=%+v error=%v", deliveries, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := newWorker().RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) ||
		errors.Is(err, transition.ErrNoDueTimer) {
		t.Fatalf("still-incompatible timer was not retained as blocked after restart: %v", err)
	}
	blocked, err = store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 1 || blocked[0].ID != "a" {
		t.Fatalf("blocked timer did not survive restart: %+v, %v", blocked, err)
	}
	available = true
	if err := newWorker().RunOne(ctx); err != nil || len(deliveries) != 2 || deliveries[1].ID != "a" ||
		!deliveries[1].Deadline.Equal(deliveries[0].Deadline) {
		t.Fatalf("restored timer did not retain its deadline and deliver: deliveries=%+v error=%v", deliveries, err)
	}
	blocked, err = store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("restored timer retained compatibility block: %+v, %v", blocked, err)
	}
}

type unreadableTimerCodec struct{ stringValueCodec }

func (unreadableTimerCodec) Decode(context.Context, []byte) (string, error) {
	return "", errors.New("private event contents")
}

func TestUndecodableTimerRecordsSafeCompatibilityDiagnostic(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "undecodable.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	available := true
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) {
		if !available {
			return unreadableTimerCodec{}, nil
		}
		return stringValueCodec{}, nil
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := &timerTestClock{now: now, ticks: make(chan time.Time, 1)}
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "timer", WorkflowID: "workflow", Deadline: now, Event: "event",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	available = false
	deliveries := 0
	worker, err := transition.NewTimerWorker[string](store, resolve, func(context.Context, transition.TimerDelivery[string]) error {
		deliveries++
		return nil
	}, transition.TimerWorkerConfig{ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = worker.RunOne(ctx)
	var compatibility *transition.CompatibilityError
	if !errors.As(err, &compatibility) || compatibility.Diagnostic.Component != transition.CompatibilityEventCodec ||
		strings.Contains(err.Error(), "private event contents") || deliveries != 0 {
		t.Fatalf("undecodable event did not produce safe compatibility failure: %v", err)
	}
	blocked, err := store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 1 || blocked[0].Compatibility == nil {
		t.Fatalf("undecodable timer was not blocked: %+v, %v", blocked, err)
	}
	available = true
	if err := worker.RunOne(ctx); err != nil || deliveries != 1 {
		t.Fatalf("restored codec did not resume timer: deliveries=%d error=%v", deliveries, err)
	}
}

func TestTimerHandlerCompatibilityFailureRemainsRetryable(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "handler.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := &timerTestClock{now: now, ticks: make(chan time.Time, 1)}
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "timer", WorkflowID: "workflow", Deadline: now, Event: "event",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	worker, err := transition.NewTimerWorker[string](store, resolve, func(context.Context, transition.TimerDelivery[string]) error {
		calls++
		if calls == 1 {
			return transition.NewCompatibilityError(transition.CompatibilityDiagnostic{
				Component: transition.CompatibilityMachine, Name: "workflow", Version: "1",
			}, nil)
		}
		return nil
	}, transition.TimerWorkerConfig{ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); !errors.Is(err, transition.ErrCompatibility) {
		t.Fatalf("handler failure was not reported: %v", err)
	}
	blocked, err := store.BlockedTimers(ctx)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("handler failure was mistaken for a timer codec failure: %+v, %v", blocked, err)
	}
	clock.advance(time.Minute)
	if err := worker.RunOne(ctx); err != nil || calls != 2 {
		t.Fatalf("handler failure did not retry after lease expiry: calls=%d error=%v", calls, err)
	}
}

func TestLegacyTimerRecordSurvivesSchemaMigration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(transition.CodecVersion) (transition.Codec[string], error) { return stringValueCodec{}, nil }
	scheduler, err := transition.NewTimerScheduler[string](store, resolve)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := scheduler.Schedule(ctx, transition.TimerRequest[string]{
		ID: "timer", WorkflowID: "workflow", Deadline: now, Event: "event",
		EventCodec: transition.CodecVersion{Name: "event", Version: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE rff_timers SET record = CAST(json_set(CAST(record AS TEXT), '$.SchemaVersion', 1) AS BLOB);
		UPDATE rff_schema_version SET version = 8`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	clock := &timerTestClock{now: now, ticks: make(chan time.Time, 1)}
	var delivered transition.TimerDelivery[string]
	worker, err := transition.NewTimerWorker[string](store, resolve, func(_ context.Context, event transition.TimerDelivery[string]) error {
		delivered = event
		return nil
	}, transition.TimerWorkerConfig{ID: "worker", Clock: clock, LeaseTTL: time.Minute, RenewalInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOne(ctx); err != nil || delivered.ID != "timer" || delivered.Event != "event" ||
		!delivered.Deadline.Equal(now) {
		t.Fatalf("legacy timer did not survive migration: %+v, %v", delivered, err)
	}
}
