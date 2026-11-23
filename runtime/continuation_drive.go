package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/reinaldoborges/rff-go/fsm"
)

var ErrContinuationNotDue = errors.New("continuation retry is not due")
var ErrContinuationIntervention = errors.New("continuation requires intervention")

// ContinuationLease fences a single evaluator. Its expiration does not imply
// that an external successor acceptance has failed.
type ContinuationLease struct {
	ExecutionID string
	Owner       string
	Generation  uint64
	ExpiresAt   time.Time
}

type ContinuationPhase string

const (
	ContinuationWaiting       ContinuationPhase = "waiting"
	ContinuationDriving       ContinuationPhase = "driving"
	ContinuationDecided       ContinuationPhase = "decided"
	ContinuationStopped       ContinuationPhase = "stopped"
	ContinuationIntervention  ContinuationPhase = "intervention"
	ContinuationAccepting     ContinuationPhase = "accepting"
	ContinuationWaiverPending ContinuationPhase = "waiver-pending"
	ContinuationWaived        ContinuationPhase = "waived"
	ContinuationAccepted      ContinuationPhase = "accepted"
	ContinuationSuperseded    ContinuationPhase = "superseded"
)

// ContinuationDecision is an encoded, replayable application decision.
type ContinuationDecision struct {
	Continue bool
	Event    EncodedValue
	Input    EncodedValue
}

type continuationEncodedValue struct {
	Name    []byte
	Version []byte
	Data    []byte
}

type continuationDecisionRecord struct {
	Continue bool
	Event    continuationEncodedValue
	Input    continuationEncodedValue
}

func (d ContinuationDecision) MarshalJSON() ([]byte, error) {
	return json.Marshal(continuationDecisionRecord{
		Continue: d.Continue,
		Event:    continuationEncodedValue{Name: []byte(d.Event.Codec.Name), Version: []byte(d.Event.Codec.Version), Data: d.Event.Data},
		Input:    continuationEncodedValue{Name: []byte(d.Input.Codec.Name), Version: []byte(d.Input.Codec.Version), Data: d.Input.Data},
	})
}

func (d *ContinuationDecision) UnmarshalJSON(data []byte) error {
	var record continuationDecisionRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrCompatibility
	}
	*d = ContinuationDecision{
		Continue: record.Continue,
		Event:    EncodedValue{Codec: CodecVersion{Name: string(record.Event.Name), Version: string(record.Event.Version)}, Data: record.Event.Data},
		Input:    EncodedValue{Codec: CodecVersion{Name: string(record.Input.Name), Version: string(record.Input.Version)}, Data: record.Input.Data},
	}
	return nil
}

type ContinuationAttempt struct {
	StartedAt time.Time
	EndedAt   time.Time
	Failure   fsm.Failure
}

type ContinuationControlAction string

const (
	RetryContinuation ContinuationControlAction = "retry"
	WaiveContinuation ContinuationControlAction = "waive"
)

type ContinuationControl struct {
	ID        string
	Action    ContinuationControlAction
	Principal string
	Reason    string
}

type ContinuationControlAudit struct {
	ID        []byte
	Action    ContinuationControlAction
	Principal []byte
	Reason    []byte
	At        time.Time
}

// ContinuationProgress is internal recovery data. Host authorization is
// required before it is disclosed externally.
type ContinuationProgress struct {
	Policy               fsm.Policy `json:"-"`
	Version              uint
	Phase                ContinuationPhase
	Attempts             uint
	CycleStarted         time.Time
	Deadline             time.Time
	StartedAt            time.Time
	AttemptPaused        time.Duration
	StartedGeneration    uint64
	PausedAt             time.Time
	NextAttemptAt        time.Time
	CompatibilityBlocked bool
	History              []ContinuationAttempt
	Controls             []ContinuationControlAudit
	Owner                []byte
	Generation           uint64
	LeaseExpires         time.Time
	PermitExpires        time.Time
	Decision             *ContinuationDecision
	SuccessorID          []byte
}

func (p ContinuationProgress) AttemptDeadline(policy fsm.Policy) time.Time {
	return p.StartedAt.Add(policy.AttemptTimeout).Add(p.AttemptPaused)
}
