# RF-DSF Go implementation

Experimental Go implementation of the RF-DSF first slice. The `fsm` package
provides typed state-machine selection; the durable runtime is in progress.

Run the standalone, runtime-independent FSM example with:

```sh
go run ./examples/fsm
```

The [home repository](https://github.com/reinaldoborges/rff-home) owns the
[architecture](https://github.com/reinaldoborges/rff-home/blob/main/docs/architecture.md),
design decisions, and implementation issues.

The checks require Go 1.27.3, CGO, and a supported C compiler for the SQLite
adapter and race detection.
See [AGENTS.md](AGENTS.md) for the verification commands.

## Transition acceptance

The `runtime` package defines an acceptor and a host-injected atomic acceptance
store. Applications supply versioned codecs, an execution-ID generator, and a
clock. Codecs must preserve semantic equality and report when it cannot be
verified.

The hook runner rehydrates pinned machine and codec versions and invokes state
hooks without rerunning selection. It does not schedule work, fence attempts, or
persist hook outcomes.

`HookWorker` claims hook activities through a host-injected activity store,
renews their leases while working and reporting, and supports draining.
`Coordinator.Reconcile` scans persisted active executions; the host must call it
regularly, including without wakeups, and often enough to renew its leases.
`Coordinator.Status` reports the last scan separately from worker capacity.
`CommitCoordinator` separately scans commit-pending work against a host-owned
commit boundary. The host must enforce transition fences and atomically persist
its final receipt with domain state or definitive rejection; errors leave the
outcome unknown for later reconciliation. Use the same coordinator owner when
driving both scans in one process.
`NotificationCoordinator` similarly reconciles a host-owned, fenced, idempotent
outbox handoff. Unknown handoff outcomes are retried; definitive failures require
an authorized retry or waiver. The host must schedule this scan and provide the
outbox. Use the same coordinator owner for all scans.
`NotificationWorker` claims and delivers intents independently, renews claims
while delivering, and schedules retries with its own configured timing. The
host schedules worker calls and must make downstream delivery idempotent by
intent ID. A delivery error is returned even when retry scheduling succeeds.
When a worker cannot rehydrate a pinned hook before starting it, the adapter
persists a safe compatibility diagnostic without consuming an attempt.
`CompatibilityCoordinator` revalidates these blocks after behavior is restored;
the host must schedule that scan and use the same coordinator owner.
SQLite's `ReadCompatibility` can inspect a diagnostic even when an identifier
or state value cannot be decoded.
`InspectionStore` lists retained executions and returns payload-free protocol
summaries, including compatibility diagnostics and hook attempt history. The
host must authorize access and protect application-provided failure details.
`TimerScheduler` persists typed timer events with stable IDs and original
deadlines. The host schedules `TimerWorker.RunOne` calls and supplies a durable
handler that deduplicates by timer ID. Delivery is at least once, and errors
leave claims to expire for recovery. Timer scheduling is not atomic with
application state: hosts must reconcile missing timers from durable workflow
state after a crash.

Adapter tests can use `internal/acceptancecontract` with an isolated store and
injected failures before commit and after commit with a lost response. The
in-memory test fake exercises that contract without claiming durability:

```sh
go test ./runtime -run TestAcceptanceStoreContract
```

## SQLite acceptance store

`adapters/sqlite` provides a host-injected file-backed acceptance store.
`sqlite.Open` takes a file path and canonical, round-tripping codecs for entity
and state identifiers; `sqlite.StringCodec` supports string identifiers.
Close the store when finished. It uses SQLite immediate transactions, a
five-second busy timeout, foreign keys, and `synchronous=FULL`. Durability
depends on the filesystem and hardware honoring SQLite's sync and locking
requirements. In-memory databases are not supported by this adapter.

The adapter owns its `rff_` tables, scans existing records on open, and fails
without writes for unsupported schemas. It migrates version 1 through 6 stores
transactionally, preserving accepted records, reservations, and leases.
It persists hook dispatch, worker claims, started attempts, and safe outcomes.
The coordinator can reconcile expired attempts, retry hooks, enter intervention,
record a commit-pending work-success marker, and finish authorized abort
recovery. `AuthorizedControls` and `AuthorizedMigration` call host authorization
for every request, including replay; hosts own actor attribution and audit.
The host may use low-level control stores directly only after authorization.
Typed external-input ingress is not yet implemented.
The host provides domain commits; automatic rejection recovery is supported.
`sqlite.NewNotificationOutbox` provides fenced local handoff and durable delivery
claims, including recovery after claim expiry. Incompatible delivery records
are inspectable through `ReadNotificationCompatibility` and do not prevent
other deliveries. For domain data stored in the
same SQLite file, `Store.CommitAtomic` lets the host check and update its domain
tables in the adapter transaction, atomically recording the receipt and enabled
notification intent. Its callback must use only the provided transaction and
must not control or retain it. Rejection discards callback writes. Separate
stores use the host commit and handoff reconciliation interfaces instead.
`ExecutionMigrator` supports host-authorized pre-commit conversion with pinned
destination validation, original-request replay, and worker fencing. Converters
receive the old encoded state; domain verification and audit remain host-owned.
Migration preserves policy and deadlines, and fenced started attempts are
recorded as unknown. Some compatibility discovery paths remain incomplete.

```sh
go test ./adapters/sqlite
go test ./adapters/sqlite -run TestAcceptanceStoreContract
go test ./adapters/sqlite -run 'TestCoordinator|TestAbortRecovery|TestRecoveryDeadline|TestAuthorizedRetry|TestSQLiteWorker'
go test ./adapters/sqlite -run 'TestNotificationHandoff|TestNotificationRepair'
go test ./adapters/sqlite -run TestNotificationDelivery
go test ./adapters/sqlite -run TestAtomicCommit
go test ./adapters/sqlite -run TestIncompatibleExecution
go test ./adapters/sqlite -run TestMigration
go test ./adapters/sqlite -run TestTimer
go test ./runtime -run TestNotificationWorker
go test ./runtime
```
