package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

type lifecycleInputCodec struct{}

func (lifecycleInputCodec) Encode(_ context.Context, v string) ([]byte, error) { return []byte(v), nil }
func (lifecycleInputCodec) Decode(_ context.Context, v []byte) (string, error) { return string(v), nil }
func (lifecycleInputCodec) Equal(_ context.Context, v []byte, x string) (bool, error) {
	return string(v) == x, nil
}

func TestLifecycleInputRecoversAcceptedInputAtStartup(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}

	defer store.Close()
	lost := errors.New("lost response")
	calls := 0
	ingress, err := transition.NewInputIngress[string](store, "orders",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(_ context.Context, input transition.InputDelivery[string]) (transition.InputOutcome, error) {
			calls++
			if input.ID != "input-1" || input.Value != "buy" {
				t.Fatalf("unexpected delivery: %+v", input)
			}
			if calls == 1 {
				return transition.InputOutcome{}, lost
			}
			return transition.InputOutcome{Receipt: "order-1"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[string]{ID: "input-1", WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}}
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, lost) {
		t.Fatalf("accept: %v", err)
	}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
		Inputs:       []transition.LifecycleInput{ingress},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)
	outcome, err := ingress.Submit(ctx, request)
	if err != nil || outcome.Receipt != "order-1" || calls != 2 {
		t.Fatalf("replayed outcome=%+v calls=%d err=%v", outcome, calls, err)
	}
}
func TestLifecycleInputSlowInitialHandlerDoesNotBlockIndependentPaths(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	block := make(chan struct{})
	entered := make(chan struct{})
	accepting := true
	ingress, err := transition.NewInputIngress[string](store, "slow",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(ctx context.Context, _ transition.InputDelivery[string]) (transition.InputOutcome, error) {
			if accepting {
				return transition.InputOutcome{}, errors.New("initial failure")
			}
			close(entered)
			select {
			case <-block:
				return transition.InputOutcome{Receipt: "ok"}, nil
			case <-ctx.Done():
				return transition.InputOutcome{}, ctx.Err()
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[string]{ID: "slow", WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}}
	_, _ = ingress.Submit(ctx, request)
	accepting = false
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	scans := &lifecycleTestStore{}
	l, err := transition.NewLifecycle[string, string, string, int](scans, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond, Inputs: []transition.LifecycleInput{ingress},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- l.Start(ctx) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("input handler did not start")
	}
	defer func() { close(block); <-started; _ = l.Shutdown(ctx) }()
	deadline := time.Now().Add(500 * time.Millisecond)
	for (scans.scans.Load() < 2 || scans.claims.Load() < 1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if scans.scans.Load() < 2 || scans.claims.Load() < 1 {
		t.Fatalf("independent paths stalled: scans=%d claims=%d", scans.scans.Load(), scans.claims.Load())
	}
}

func TestLifecycleInputProcessRestart(t *testing.T) {
	path := os.Getenv("RFF_INPUT_RESTART_DB")
	stage := os.Getenv("RFF_INPUT_RESTART_STAGE")
	if stage == "" {
		path = filepath.Join(t.TempDir(), "input.db")
		for _, stage := range []string{"accept", "recover"} {
			cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycleInputProcessRestart$")
			cmd.Env = append(os.Environ(), "RFF_INPUT_RESTART_DB="+path, "RFF_INPUT_RESTART_STAGE="+stage)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s process: %v\n%s", stage, err, output)
			}
		}
		return
	}
	ctx := context.Background()
	store, err := sqlite.Open[string, string](path, sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hostDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer hostDB.Close()
	if _, err := hostDB.Exec(`CREATE TABLE IF NOT EXISTS host_receipts (input_id TEXT PRIMARY KEY, receipt TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	ingress, err := transition.NewInputIngress[string](store, "restart",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(_ context.Context, input transition.InputDelivery[string]) (transition.InputOutcome, error) {
			calls.Add(1)
			if input.ID != "restart-id" || input.WorkflowID != "order" || input.Value != "buy" || input.Kind != transition.Command {
				return transition.InputOutcome{}, errors.New("wrong delivery identity")
			}
			if _, err := hostDB.Exec(`INSERT OR IGNORE INTO host_receipts (input_id, receipt) VALUES (?, ?)`, input.ID, "stable-receipt"); err != nil {
				return transition.InputOutcome{}, err
			}
			if stage == "accept" {
				return transition.InputOutcome{}, errors.New("unknown outcome")
			}
			return transition.InputOutcome{Receipt: "stable-receipt"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[string]{ID: "restart-id", WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}}
	if stage == "accept" {
		if _, err := ingress.Submit(ctx, request); err == nil {
			t.Fatal("expected unknown host outcome")
		}
		return
	}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond, Inputs: []transition.LifecycleInput{ingress},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)
	outcome, err := ingress.Submit(ctx, request)
	if err != nil || outcome.Receipt != "stable-receipt" || calls.Load() != 1 {
		t.Fatalf("replay: %+v calls=%d err=%v", outcome, calls.Load(), err)
	}
	var receipts int
	if err := hostDB.QueryRow(`SELECT COUNT(*) FROM host_receipts`).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("host effects: count=%d err=%v", receipts, err)
	}
	request.Value = "different"
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, transition.ErrConflict) || calls.Load() != 1 {
		t.Fatalf("changed payload: calls=%d err=%v", calls.Load(), err)
	}
}

func TestLifecycleInputRetriesWithoutWakeupAndReportsFailure(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	transient := errors.New("private handler error")
	var failing atomic.Bool
	failing.Store(true)
	var calls atomic.Int32
	ingress, err := transition.NewInputIngress[string](store, "retry",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			calls.Add(1)
			if failing.Load() {
				return transition.InputOutcome{}, transient
			}
			return transition.InputOutcome{Receipt: "done"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 20 * time.Millisecond, Inputs: []transition.LifecycleInput{ingress},
		ClassifyError: func(err error) transition.PathErrorClass {
			if errors.Is(err, transient) {
				return transition.PathErrorRecoverable
			}
			return transition.PathErrorUnclassified
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)
	request := transition.InputRequest[string]{ID: "retry-id", WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}}
	if _, err := ingress.Submit(ctx, request); !errors.Is(err, transient) {
		t.Fatalf("initial handler failure: %v", err)
	}
	select {
	case <-l.Errors():
	case <-time.After(5 * time.Second):
		t.Fatal("periodic scan did not report failure")
	}
	status := l.Status()
	var input transition.PathStatus
	for _, path := range status.Paths {
		if path.Name == "input:retry" {
			input = path
		}
	}
	if input.Classification != transition.PathErrorRecoverable || !input.Running || status.Ready || !errors.Is(l.Err(), transient) {
		t.Fatalf("failure status: %+v", status)
	}
	failing.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		record, err := store.ReadInput(ctx, request.ID)
		if err == nil && record.Outcome != nil && l.Status().Ready {
			if record.Outcome.Receipt != "done" || calls.Load() < 3 {
				t.Fatalf("outcome=%+v calls=%d", record.Outcome, calls.Load())
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("input did not recover: %+v", l.Status())
}

func TestLifecycleInputShutdownStopsFurtherDeliveries(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var phase atomic.Int32
	var delivered atomic.Int32
	entered := make(chan struct{})
	ingress, err := transition.NewInputIngress[string](store, "shutdown",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(ctx context.Context, _ transition.InputDelivery[string]) (transition.InputOutcome, error) {
			switch phase.Load() {
			case 0:
				return transition.InputOutcome{}, errors.New("pending")
			case 1:
				if delivered.Add(1) == 1 {
					close(entered)
					<-ctx.Done()
				}
				return transition.InputOutcome{Receipt: "done"}, nil
			default:
				delivered.Add(1)
				return transition.InputOutcome{Receipt: "done"}, nil
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	request := transition.InputRequest[string]{WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}}
	for _, id := range []string{"a", "b"} {
		request.ID = id
		if _, err := ingress.Submit(ctx, request); err == nil {
			t.Fatal("expected pending input")
		}
	}

	phase.Store(1)
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	config := transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond, Inputs: []transition.LifecycleInput{ingress},
	}
	newLifecycle := func() *transition.Lifecycle[string, string, string, int] {
		l, err := transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, config)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	l := newLifecycle()
	started := make(chan error, 1)
	go func() { started <- l.Start(ctx) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	if err := l.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	<-started
	if delivered.Load() != 1 {
		t.Fatalf("delivered after shutdown: %d", delivered.Load())
	}
	phase.Store(2)
	l = newLifecycle()
	if err := l.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)
	for _, id := range []string{"a", "b"} {
		request.ID = id
		outcome, err := ingress.Submit(ctx, request)
		if err != nil || outcome.Receipt != "done" {
			t.Fatalf("recover %s: %+v %v", id, outcome, err)
		}
	}
}
func TestLifecycleInputFailureIsIsolatedAndExplicitlyRestarted(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	failure := errors.New("private unrecoverable failure")
	var badEnabled, goodEnabled atomic.Bool
	var goodCalls atomic.Int32
	makeIngress := func(channel string, enabled *atomic.Bool) *transition.InputIngress[string] {
		ingress, err := transition.NewInputIngress[string](store, channel,
			func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
			func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
				if channel == "good" {
					goodCalls.Add(1)
				}
				if !enabled.Load() {
					return transition.InputOutcome{}, failure
				}
				return transition.InputOutcome{Receipt: channel}, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		return ingress
	}
	bad := makeIngress("bad", &badEnabled)
	good := makeIngress("good", &goodEnabled)
	request := transition.InputRequest[string]{WorkflowID: "order", Kind: transition.Command, Value: "buy", Codec: transition.CodecVersion{Name: "order", Version: "1"}}
	request.ID = "bad-id"
	_, _ = bad.Submit(ctx, request)
	request.ID = "good-id"
	_, _ = good.Submit(ctx, request)
	goodEnabled.Store(true)
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	scans := &lifecycleTestStore{}
	l, err := transition.NewLifecycle[string, string, string, int](scans, runner, runner, lifecycleTestHost{}, transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond, Inputs: []transition.LifecycleInput{bad, good},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(ctx); !errors.Is(err, failure) {
		t.Fatalf("initial failure not reported: %v", err)
	}
	defer l.Shutdown(ctx)
	status := l.Status()
	var badStatus transition.PathStatus
	for _, path := range status.Paths {
		if path.Name == "input:bad" {
			badStatus = path
		}
	}
	if status.Ready || badStatus.Running || badStatus.Classification != transition.PathErrorUnclassified {
		t.Fatalf("incorrect failure status: %+v", status)
	}
	if goodCalls.Load() != 2 {
		t.Fatalf("independent input did not progress: calls=%d", goodCalls.Load())
	}
	outcome, err := good.Submit(ctx, request)
	if err != nil || outcome.Receipt != "good" || goodCalls.Load() != 2 {
		t.Fatalf("independent input replay: %+v %v", outcome, err)
	}
	badEnabled.Store(true)
	if err := l.RestartPath("input:bad"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !l.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.Status().Ready {
		t.Fatalf("explicit restart did not recover: %+v", l.Status())
	}
	request.ID = "bad-id"
	outcome, err = bad.Submit(ctx, request)
	if err != nil || outcome.Receipt != "bad" {
		t.Fatalf("restarted input: %+v %v", outcome, err)
	}
}

func TestLifecycleInputConfigurationIsExplicit(t *testing.T) {
	runner, _, _ := newFailingHookRunner(t, errorHandler{enter: func(context.Context) error { return nil }})
	config := transition.LifecycleConfig{
		Coordinator:  transition.CoordinatorConfig{ID: "coordinator", LeaseTTL: time.Second, Clock: time.Now},
		Worker:       transition.WorkerConfig{ID: "worker", Capacity: 1, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, Clock: transition.SystemWorkerClock{}},
		PollInterval: 10 * time.Millisecond,
	}
	newLifecycle := func(config transition.LifecycleConfig) (*transition.Lifecycle[string, string, string, int], error) {
		return transition.NewLifecycle[string, string, string, int](&lifecycleTestStore{}, runner, runner, lifecycleTestHost{}, config)
	}
	l, err := newLifecycle(config)
	if err != nil || len(l.Status().Paths) != 2 {
		t.Fatalf("disabled input created a path: %v %+v", err, l)
	}
	var missing *transition.InputIngress[string]
	config.Inputs = []transition.LifecycleInput{missing}
	if _, err := newLifecycle(config); err == nil {
		t.Fatal("nil input ingress accepted")
	}
	store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "input.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ingress, err := transition.NewInputIngress[string](store, "same",
		func(transition.CodecVersion) (transition.Codec[string], error) { return lifecycleInputCodec{}, nil },
		func(context.Context, transition.InputDelivery[string]) (transition.InputOutcome, error) {
			return transition.InputOutcome{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	config.Inputs = []transition.LifecycleInput{ingress, ingress}
	if _, err := newLifecycle(config); err == nil {
		t.Fatal("duplicate channel accepted")
	}
}
