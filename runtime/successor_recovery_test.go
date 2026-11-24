package runtime_test

import (
	"context"
	"errors"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

type successorRecoveryStore struct {
	inspection transition.SuccessorInspection[string, string]
	finalized  int
}

func (s *successorRecoveryStore) InspectSuccessor(context.Context, transition.ContinuationMarker[string, string]) (transition.SuccessorInspection[string, string], error) {
	return s.inspection, nil
}

func (s *successorRecoveryStore) FinalizeSuccessor(_ context.Context, receipt transition.SuccessorReceipt[string, string]) (transition.Execution[string, string], error) {
	s.finalized++
	s.inspection.Phase = transition.SuccessorAccepted
	return receipt.Binding.Execution, nil
}

func (s *successorRecoveryStore) CancelSuccessor(context.Context, transition.SuccessorCancellation[string, string]) error {
	return errors.New("unexpected cancellation")
}

func (s *successorRecoveryStore) RecordSuccessorDenial(context.Context, transition.SuccessorBinding[string, string], string) error {
	s.inspection.Phase = transition.SuccessorDenied
	return nil
}

type successorRecoveryHost struct {
	result  transition.SuccessorHostResult[string, string]
	lookup  error
	deliver int
}

func TestSuccessorDenialRequiresInterventionButStillReconciles(t *testing.T) {
	marker := continuationMarker("origin")
	binding := transition.SuccessorBinding[string, string]{
		Origin: marker, OriginRevision: []byte("revision"), Execution: transition.Execution[string, string]{ID: "next"},
	}

	t.Run("supersession requires host fence", func(t *testing.T) {
		marker := continuationMarker("origin")
		binding := transition.SuccessorBinding[string, string]{
			Origin: marker, OriginRevision: []byte("revision"), Execution: transition.Execution[string, string]{ID: "next"},
		}
		store := &successorRecoveryStore{inspection: transition.SuccessorInspection[string, string]{Binding: binding, Phase: transition.SuccessorPrepared}}
		host := &successorRecoveryHost{result: transition.SuccessorHostResult[string, string]{Outcome: transition.SuccessorHostSuperseded}}
		recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
		if err != nil {
			t.Fatal(err)
		}
		if err := recovery.Reconcile(context.Background(), marker, true); !errors.Is(err, transition.ErrCompatibility) {
			t.Fatalf("supersession without cancellation evidence must block: %v", err)
		}
	})
	store := &successorRecoveryStore{inspection: transition.SuccessorInspection[string, string]{Binding: binding, Phase: transition.SuccessorPrepared}}
	host := &successorRecoveryHost{result: transition.SuccessorHostResult[string, string]{Outcome: transition.SuccessorHostDenied, Principal: "worker"}}
	recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Reconcile(context.Background(), marker, true); !errors.Is(err, transition.ErrSuccessorDenied) || store.inspection.Phase != transition.SuccessorDenied {
		t.Fatalf("denial must enter intervention: %v", err)
	}
	host.result = transition.SuccessorHostResult[string, string]{Outcome: transition.SuccessorHostAbsent}
	if err := recovery.Reconcile(context.Background(), marker, true); !errors.Is(err, transition.ErrSuccessorDenied) || host.deliver != 0 {
		t.Fatalf("denial must block new delivery: %v", err)
	}
	host.result = transition.SuccessorHostResult[string, string]{
		Outcome: transition.SuccessorHostAccepted,
		Receipt: &transition.SuccessorReceipt[string, string]{Binding: binding, Principal: "worker", Decision: "allowed"},
	}
	if err := recovery.Reconcile(context.Background(), marker, false); err != nil || store.finalized != 1 {
		t.Fatalf("accepted receipt must win over denial: %v", err)
	}
}

func (h *successorRecoveryHost) LookupSuccessor(context.Context, transition.SuccessorBinding[string, string]) (transition.SuccessorHostResult[string, string], error) {
	return h.result, h.lookup
}

func (h *successorRecoveryHost) AcceptSuccessor(context.Context, transition.SuccessorBinding[string, string]) (transition.SuccessorHostResult[string, string], error) {
	h.deliver++
	return h.result, nil
}

func TestSuccessorRecoveryReconcilesBeforeAnyNewDelivery(t *testing.T) {
	marker := continuationMarker("origin")
	binding := transition.SuccessorBinding[string, string]{
		Origin: marker, OriginRevision: []byte("revision"), Execution: transition.Execution[string, string]{ID: "next"},
	}
	store := &successorRecoveryStore{inspection: transition.SuccessorInspection[string, string]{
		Binding: binding, Phase: transition.SuccessorPrepared,
	}}
	host := &successorRecoveryHost{}
	recovery, err := transition.NewSuccessorRecovery[string, string](store, host)
	if err != nil {
		t.Fatal(err)
	}
	host.lookup = errors.New("host unavailable")
	if err := recovery.Reconcile(context.Background(), marker, true); err == nil || host.deliver != 0 || store.finalized != 0 {
		t.Fatalf("unknown lookup must not be treated as absence: err=%v deliveries=%d", err, host.deliver)
	}
	host.lookup = nil
	wrong := binding
	wrong.Origin.ExecutionID = "other"
	host.result = transition.SuccessorHostResult[string, string]{
		Outcome: transition.SuccessorHostAccepted,
		Receipt: &transition.SuccessorReceipt[string, string]{Binding: wrong, Principal: "worker", Decision: "allowed"},
	}
	if err := recovery.Reconcile(context.Background(), marker, false); !errors.Is(err, transition.ErrConflict) || store.finalized != 0 {
		t.Fatalf("receipt for another successor must not be finalized: %v", err)
	}
	host.result = transition.SuccessorHostResult[string, string]{
		Outcome: transition.SuccessorHostAccepted,
		Receipt: &transition.SuccessorReceipt[string, string]{Binding: binding, Principal: "worker", Decision: "allowed"},
	}
	if err := recovery.Reconcile(context.Background(), marker, false); err != nil {
		t.Fatal(err)
	}
	if host.deliver != 0 || store.finalized != 1 {
		t.Fatalf("accepted host receipt was not finalized without new authorization: deliveries=%d finalized=%d", host.deliver, store.finalized)
	}
}
