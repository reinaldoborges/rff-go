package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/reinaldoborges/rff-go/adapters/sqlite"
	"github.com/reinaldoborges/rff-go/internal/runtimecontract"
	transition "github.com/reinaldoborges/rff-go/runtime"
)

func TestRuntimeOperationContract(t *testing.T) {
	runtimecontract.RunOperation(t, func(t *testing.T) runtimecontract.OperationStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationReconciliationContract(t *testing.T) {
	runtimecontract.RunOperationReconciliation(t, func(t *testing.T) runtimecontract.OperationReconciliationStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "reconciliation.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationDispatchClassificationContract(t *testing.T) {
	runtimecontract.RunOperationDispatchClassification(t, func(t *testing.T) runtimecontract.OperationStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "classification.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationPrerequisitesContract(t *testing.T) {
	runtimecontract.RunOperationPrerequisites(t, func(t *testing.T) runtimecontract.OperationPrerequisiteStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "prerequisites.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationSafeRepeatContract(t *testing.T) {
	runtimecontract.RunOperationSafeRepeat(t, func(t *testing.T) runtimecontract.OperationRepeatStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "repeat.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationSuccessRace(t *testing.T) {
	runtimecontract.RunOperationSuccessRace(t, func(t *testing.T) runtimecontract.OperationStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationAbort(t *testing.T) {
	runtimecontract.RunOperationAbort(t, func(t *testing.T) runtimecontract.OperationControlStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationAbortRace(t *testing.T) {
	runtimecontract.RunOperationAbortRace(t, func(t *testing.T) runtimecontract.OperationControlStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeConcurrentOperations(t *testing.T) {
	runtimecontract.RunConcurrentOperations(t, func(t *testing.T) runtimecontract.OperationStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeOperationMigration(t *testing.T) {
	runtimecontract.RunOperationMigration(t, func(t *testing.T) runtimecontract.OperationMigrationStore {
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "operations.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestRuntimeStoreContract(t *testing.T) {
	runtimecontract.Run(t, func(t *testing.T) runtimecontract.Store {
		t.Helper()
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "contract.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
}

func TestRuntimeTimerCompatibilityContract(t *testing.T) {
	runtimecontract.RunTimerCompatibility(t, func(t *testing.T) runtimecontract.TimerStore {
		t.Helper()
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "contract.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
}

func TestRuntimeInputIngressContract(t *testing.T) {
	runtimecontract.RunInputIngress(t, func(t *testing.T) transition.InputStore {
		t.Helper()
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "contract.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
}

func TestRuntimeAuthorizedControlContract(t *testing.T) {
	runtimecontract.RunAuthorizedControl(t, func(t *testing.T) runtimecontract.ControlStore {
		t.Helper()
		store, err := sqlite.Open[string, string](filepath.Join(t.TempDir(), "contract.db"), sqlite.StringCodec{}, sqlite.StringCodec{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
}
