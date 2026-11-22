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
verified. `CodecRegistry` provides concurrency-safe exact-version lookup and
rejects duplicate codec registrations; applications may also supply their own
resolver.

The hook runner rehydrates pinned machine and codec versions and invokes state
hooks without rerunning selection. It does not schedule work, fence attempts, or
persist hook outcomes.

`HookWorker` claims hook activities through a host-injected activity store,
renews their leases while working and reporting, and supports draining.
`Coordinator.Reconcile` scans persisted active executions; the host must call it
regularly, including without wakeups, and often enough to renew its leases.
`Coordinator.Status` reports the last scan separately from worker capacity.
For capacity reporting, configure a `WorkerMonitor` with an explicit stale
threshold and have the host periodically report each worker's `Status()`.
Unready, draining, and stale workers contribute no available slots; no slots
is degraded execution capacity, not failed persistence or acceptance.
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
state after a crash. Unavailable timer event codecs block only the affected
timer and persist a diagnostic, available through `BlockedTimers`. The worker
revalidates blocked timers before each scan and resumes them when their pinned
codec can decode the original event. When no other timer can be claimed, it
reports the remaining incompatibility rather than an empty queue.

Adapter tests can use `internal/acceptancecontract` with an isolated store and
injected failures before commit and after commit with a lost response. The
in-memory test fake exercises that contract without claiming durability:

```sh
go test ./runtime -run TestAcceptanceStoreContract
```

`internal/runtimecontract` provides reusable execution, worker-loss,
commit-rejection, notification-handoff and repair, timer-compatibility,
typed-ingress, authorized-control, migration, and capacity contracts over the
runtime ports.
Run them against the SQLite adapter with:

```sh
go test ./adapters/sqlite -run 'TestRuntime(Store|TimerCompatibility|InputIngress|AuthorizedControl|NotificationRepair|Migration|Capacity)Contract'
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
without writes for unsupported schemas. It migrates version 1 through 8 stores
transactionally, preserving accepted records, reservations, and leases.
Older adapter versions cannot open a migrated store.
`Store.AcceptAtomic` lets a same-file host authorize and write its own audit in
the acceptance transaction; its callback must leave replays read-only.
It persists hook dispatch, worker claims, started attempts, and safe outcomes.
The coordinator can reconcile expired attempts, retry hooks, enter intervention,
record a commit-pending work-success marker, and finish authorized abort
recovery. `AuthorizedHookControls`, `AuthorizedControls`, and
`AuthorizedMigration` call host authorization for every request, including
replay; hosts own actor attribution and audit. New hook controls validate
pinned behavior under the transition fence, while notification controls check
stored identifier compatibility.
`InputIngress` persists typed commands, events, and human decisions before
calling the host, deduplicates their semantic payloads, and replays durable
outcomes. The host must authorize every submission and persist enough approved
context to reconcile it after restart. Its handler must atomically record and
deduplicate outcomes with domain effects by input ID; transient errors leave
inputs pending for host-scheduled reconciliation.
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
go test ./adapters/sqlite -run 'TestTypedInput|TestTypedCommand'
go test ./runtime -run TestNotificationWorker
go test ./runtime
go test ./internal/taskmanager
```

## Local task-manager fixture

The fixture CLI is for local development and conformance testing only. Seeded
identities are opaque test values, not authentication. The test-only operator
can inspect and control executions but has no task-manager permissions.
Each invocation opens the same file-backed fixture; `drive` performs one
reconciliation round and must be run again when more work or timers are due.
No background service or real notification provider is started.
Task and Membership runtime identities are stored separately. Existing active
work and interrupted input replays are preserved on upgrade. Once a fixture
database has been opened by this version, do not use older fixture binaries
against it: they do not recognize the identity mapping.

```sh
go run ./cmd/taskmanager -db ./fixture.db seed-user owner invitee
go run ./cmd/taskmanager -db ./fixture.db seed-operator operator
go run ./cmd/taskmanager -db ./fixture.db create-group owner group owner-membership
go run ./cmd/taskmanager -db ./fixture.db start-invitation owner group invitee membership invite-1 2099-01-01T00:00:00Z
go run ./cmd/taskmanager -db ./fixture.db drive
go run ./cmd/taskmanager -db ./fixture.db signal-invitation invitee membership decision-1 accept
go run ./cmd/taskmanager -db ./fixture.db drive
go run ./cmd/taskmanager -db ./fixture.db start-task invitee group task "Example task" create-task-1
go run ./cmd/taskmanager -db ./fixture.db drive
go run ./cmd/taskmanager -db ./fixture.db signal-task invitee task start-1 start
go run ./cmd/taskmanager -db ./fixture.db drive
go run ./cmd/taskmanager -db ./fixture.db list operator
go run ./cmd/taskmanager -db ./fixture.db show operator EXECUTION_ID
```

Commands print JSON on success and return nonzero on errors. Invitation signals
are `accept`, `decline`, or `revoke`; task signals are `start`, `complete`, or
`cancel`. `control OPERATOR EXECUTION_ID REQUEST_ID ACTION` supports `retry`,
`abort`, `notification-retry`, and `notification-waive`, subject to runtime
phase gates and fixture authorization. The fixture records operator actions in
its application-owned audit.

Additional commands are `group ACTOR GROUP`, `membership ACTOR MEMBERSHIP`,
`task ACTOR TASK`, `update-task ACTOR TASK TITLE`,
`assign-task ACTOR TASK MEMBERSHIP`, `remove-member ACTOR MEMBERSHIP KEY`,
`transfer ACTOR GROUP NEW_OWNER KEY`, and `close-group ACTOR GROUP`. Pass an
empty membership argument to unassign a task. Assignment history retains the
original Membership identity after unassignment or removal.

Run the CLI and fixture checks, including a real subprocess recovery test, with:

```sh
go test ./cmd/taskmanager ./internal/taskmanager
go test ./internal/taskmanager -run TestFixtureRecoversExpiryAndNotificationAcrossProcessRestart
go test ./adapters/sqlite -run TestAcceptedAndStartedWorkSurvivesActualProcessLoss
./scripts/check.sh
```

These are local checks, not a claim that the full acceptance matrix or CI gate
has passed.
