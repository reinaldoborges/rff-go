package runtime_test

import (
	"context"
	"testing"

	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestFailuresBeforeAcceptanceDoNotReserveEntity(t *testing.T) {
	t.Run("storage failure", func(t *testing.T) {
		f := newAcceptanceFixture(t)
		f.store.fail = true
		if _, _, err := f.acceptor.Accept(context.Background(), f.request); err == nil {
			t.Fatal("storage failure was not reported")
		}
		if _, created, err := f.acceptor.Accept(context.Background(), f.request); err != nil || !created {
			t.Fatalf("failed write left an acceptance behind: created=%v error=%v", created, err)
		}
	})
	t.Run("invalid transition", func(t *testing.T) {
		f := newAcceptanceFixture(t)
		request := f.request
		request.Operation = transition.Transition
		request.Previous, request.HasPrevious = "open", true
		request.Event, request.HasEvent = "invalid", true
		request.EventCodec = transition.CodecVersion{Name: "event", Version: "1"}
		if _, _, err := f.acceptor.Accept(context.Background(), request); err == nil {
			t.Fatal("invalid transition was not rejected")
		}
		request.Event = "finish"
		if _, created, err := f.acceptor.Accept(context.Background(), request); err != nil || !created {
			t.Fatalf("rejected selection left an acceptance behind: created=%v error=%v", created, err)
		}
	})
}
