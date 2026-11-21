package runtime_test

import (
	"testing"

	"github.com/reinaldoborges/rff-go/internal/acceptancecontract"
)

func TestAcceptanceStoreContract(t *testing.T) {
	acceptancecontract.Run(t, func(t *testing.T) acceptancecontract.Fixture {
		store := newMemoryStore()
		return acceptancecontract.Fixture{
			Store:           store,
			FailNextCommit:  func() { store.fail = true },
			FailAfterCommit: func() { store.failAfterCommit = true },
		}
	})
}
