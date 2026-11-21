package runtime_test

import (
	"context"
	"testing"
)

func TestUnknownAcceptanceOutcomeReconcilesWithoutReselecting(t *testing.T) {
	f := newAcceptanceFixture(t)
	f.store.failAfterCommit = true
	if _, _, err := f.acceptor.Accept(context.Background(), f.request); err == nil {
		t.Fatal("lost response was not reported")
	}
	record, created, err := f.acceptor.Accept(context.Background(), f.request)
	if err != nil || created || record.ID != "execution-1" || f.selections.Load() != 1 {
		t.Fatalf("replay did not reconcile the committed acceptance: record=%+v created=%v error=%v", record, created, err)
	}
}
