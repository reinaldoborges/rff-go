package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type reconciliationTicker struct {
	ticks   chan time.Time
	stopped bool
}

func (t *reconciliationTicker) Ticks() <-chan time.Time { return t.ticks }
func (t *reconciliationTicker) Stop()                   { t.stopped = true }

func TestReconciliationCoalescesWakeupsAndReturnsScanError(t *testing.T) {
	queue := transition.NewWakeupQueue()
	ticker := &reconciliationTicker{ticks: make(chan time.Time)}
	want := errors.New("scan failed")
	scans := 0
	err := transition.RunReconciliation(context.Background(), queue, ticker, func(context.Context) error {
		scans++
		if scans == 1 {
			for i := 0; i < 100; i++ {
				queue.Notify()
			}
			return nil
		}
		return want
	})
	if !errors.Is(err, want) || scans != 2 || !ticker.stopped {
		t.Fatalf("scan error or ticker cleanup lost: scans=%d stopped=%t err=%v", scans, ticker.stopped, err)
	}
}

func TestReconciliationReportsClosedTicker(t *testing.T) {
	ticks := make(chan time.Time)
	close(ticks)
	ticker := &reconciliationTicker{ticks: ticks}
	err := transition.RunReconciliation(context.Background(), transition.NewWakeupQueue(), ticker, func(context.Context) error {
		return nil
	})
	if err == nil || !ticker.stopped {
		t.Fatalf("closed ticker not reported or stopped: stopped=%t err=%v", ticker.stopped, err)
	}
}
