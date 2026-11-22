package runtime

import (
	"errors"
	"sync"
	"time"
)

var ErrInvalidWorkerStatus = errors.New("invalid worker status")
var ErrStaleWorkerHeartbeat = errors.New("worker heartbeat is older than its previous report")

// ExecutionCapacity describes currently available worker slots.
type ExecutionCapacity struct {
	Available int
	Degraded  bool
}

// WorkerMonitor retains the latest host-reported heartbeat for each worker.
// Its clock must be concurrency-safe, nonblocking, and not re-enter the monitor.
type WorkerMonitor struct {
	mu         sync.Mutex
	clock      func() time.Time
	staleAfter time.Duration
	workers    map[string]WorkerStatus
}

func NewWorkerMonitor(clock func() time.Time, staleAfter time.Duration) (*WorkerMonitor, error) {
	if clock == nil || staleAfter <= 0 {
		return nil, ErrInvalidWorkerStatus
	}
	return &WorkerMonitor{clock: clock, staleAfter: staleAfter, workers: make(map[string]WorkerStatus)}, nil
}

// Report records a worker snapshot. The host schedules periodic reports.
func (m *WorkerMonitor) Report(id string, status WorkerStatus) error {
	if id == "" || status.Capacity <= 0 || status.Active < 0 || status.Active > status.Capacity ||
		status.Heartbeat.IsZero() || status.Ready && status.Draining {
		return ErrInvalidWorkerStatus
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	if now.IsZero() || status.Heartbeat.After(now) {
		return ErrInvalidWorkerStatus
	}
	if previous, ok := m.workers[id]; ok && status.Heartbeat.Before(previous.Heartbeat) {
		return ErrStaleWorkerHeartbeat
	}
	m.workers[id] = status
	return nil
}

// Status excludes draining, unready, and stale workers from available capacity.
func (m *WorkerMonitor) Status() ExecutionCapacity {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	available := 0
	for _, worker := range m.workers {
		if !now.IsZero() && !worker.Heartbeat.After(now) && now.Sub(worker.Heartbeat) < m.staleAfter &&
			worker.Ready && !worker.Draining {
			available += worker.Capacity - worker.Active
		}
	}
	return ExecutionCapacity{Available: available, Degraded: available == 0}
}
