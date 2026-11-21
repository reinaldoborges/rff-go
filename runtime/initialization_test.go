package runtime_test

import (
	"context"
	"testing"
)

func TestInitializationDoesNotPersistAnAbsentPreviousState(t *testing.T) {
	f := newAcceptanceFixture(t)
	request := f.request
	request.Previous = "unrelated"
	record, _, err := f.acceptor.Accept(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if record.HasPrevious || record.Previous != "" {
		t.Fatalf("absent previous state leaked into the record: %+v", record)
	}
}
