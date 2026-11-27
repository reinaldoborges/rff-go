package runtime

import (
	"context"
	"errors"
	"time"
)

// ErrIntegrity indicates a detected or quarantined durable record.
var ErrIntegrity = errors.New("durable record integrity finding")

// ErrIncompleteInputList indicates that pending input discovery cannot prove a complete result.
var ErrIncompleteInputList = errors.New("pending input list is incomplete")

// ErrIntegrityPageRequired indicates that finding or audit history must be paged.
var ErrIntegrityPageRequired = errors.New("integrity history requires pagination")

// MaxIntegrityPageSize bounds maintenance reads.
const MaxIntegrityPageSize = 256

// IntegrityFinding contains only safe operator-visible metadata.
type IntegrityFinding struct {
	ID                int64
	Locator           int64
	Revision          int64
	Kind              string
	Category          string
	Field             string
	DetectedAt        time.Time
	ResolvedAt        *time.Time
	DependencyDigest  [32]byte
	DependenciesKnown bool
}

// IntegrityAuthorizer must authorize every maintenance action. Actor and reason
// must be safe to persist in an audit record and must not contain secrets.
// Inspection is store-wide (both IDs zero); audit is scoped by finding ID;
// resolution is scoped by finding ID and locator.
type IntegrityAuthorizer func(ctx context.Context, action, actor, reason string, findingID, locator int64) error

// IntegrityAudit is a safe, host-supplied maintenance audit entry.
type IntegrityAudit struct {
	ID         int64
	FindingID  int64
	Revision   int64
	Action     string
	Actor      string
	Reason     string
	OccurredAt time.Time
}

// IntegritySweepCoverage reports only records visited within the sweep's
// fixed high-water mark. It does not imply a snapshot or discovery guarantee.
type IntegritySweepCoverage struct {
	Kind             string
	HighWaterMark    int64
	InspectedThrough int64
	Inspected        int64
}

// IntegritySweepProgress is the durable state of one bounded sweep. Complete
// means all captured high-water marks were reached; Incomplete means at least
// one inspected record could not be fully checked for compatible behavior.
type IntegritySweepProgress struct {
	SweepID    int64
	StartedAt  time.Time
	UpdatedAt  time.Time
	Complete   bool
	Healthy    bool
	Incomplete bool
	Coverage   []IntegritySweepCoverage
}

// ArchivedExecutionEvidence contains the original unreadable bytes preserved
// during an authorized execution restoration. Read it only through the
// worker-free integrity-maintenance API.
type ArchivedExecutionEvidence struct {
	FindingID        int64
	Locator          int64
	Revision         int64
	RecordDigest     [32]byte
	DependencyDigest [32]byte
	Record           []byte
}
