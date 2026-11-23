# RF-DSF Go implementation

Experimental Go implementation of the RF-DSF first slice. The `fsm` package
provides typed state-machine selection; the single-node durable runtime uses
host-injected persistence, with SQLite as its first adapter. The
[first-slice acceptance gate](https://github.com/reinaldoborges/rff-home/issues/22)
has passed; this is not a production-readiness claim.

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
An attempt timeout is reported without waiting for a hook that ignores context
cancellation; such a hook may continue external side effects, which the host
must make idempotent.

Hooks may explicitly call `runtime.TrackOperation` with the hook's context.
The worker supplies the capability only when its store implements
`OperationStore`; invoking it without an active capability fails rather than
calling the downstream adapter. Supply a stable operation key, kind, behavior
and semantic comparator versions, safe input, a result codec version, and
preflight and validation callbacks. The comparator must be side-effect-free
and must not re-enter the store. A result is either typed data, an explicit
`NoResult`, or a host-classified definitive failure with a safe `FailureCode`.
The host must not persist secrets or raw errors. Its adapter maps the supplied
execution ID and key to a collision-free, provider-compatible idempotency
identity and uses distinct keys for different record/purpose pairs and
compensation. Business deduplication across executions remains host-owned.
Recover remains host-defined work under the existing transition recovery
budget; ordinary retry does not compensate automatically. Mark a tracked
compensation with `Compensation: true` and durably declare its prerequisites
with `Prerequisites: []string{"charge"}`. Use a non-nil empty slice only when
the host has explicitly determined that the compensation is independent. A
nil prerequisite list is an incomplete declaration and fails closed when
tracked effects remain unresolved; missing, unsettled, conflicting, or cyclic
prerequisites block dispatch and replay. The host remains responsible for
declaring the complete dependency set and deciding whether settled outcomes
make compensation appropriate, including equivalent gates for untracked calls.

The adapters durably record possible dispatch before the call and its confirmed
result before returning. Matching calls replay the recorded result; conflicts,
missing behavior, codecs, unverifiable equality, stale attempts and uncertain
results fail explicitly. A dispatch error is unknown unless the host's
`ClassifyDispatch` callback supplies verifiable provider evidence. The
`Reconcile` callback may establish success, a definitive failure, or
authoritative absence. Absence permits redispatch only when evidence also
proves every earlier call is quiescent. Unknown mutations require a
`SafeRepeatGuarantee` bound to the exact identity, provider scope, a
host-verified non-secret input fingerprint, and the overlap horizon; evidence
and guarantees are revalidated at use.
Hosts must also reject repeats of reads when a new response could change the
original result. Evidence records should contain only safe provenance and
validity bounds, never credentials, secrets or raw errors.

An irreconcilable operation remains unresolved and blocks work success and
final abort, including after process restart. `AuthorizedOperationResolver`
provides an authorization, audit and idempotency boundary for evidence-backed
operator resolutions during intervention. Resolution fences stale reports but
does not schedule or resume work; a separately authorized retry or abort must
pass the existing transition phase gates. `OperationInspector` exposes
retained safe data; the host must authorize access. Compensation-prerequisite
enforcement remains separate and is not provided here. Fencing cannot stop an
external call already in flight, and the runtime makes no exactly-once effect
guarantee.

An execution migration with tracked operations requires the optional operation
compatibility verifier passed to `NewExecutionMigrator`. It receives the
recorded bindings and outcomes and must prove that the destination can honor
them, including provider identity, result codecs, immutable references,
prerequisites and evidence. Set `OperationRequest.Provider` to a stable,
non-secret provider identity before dispatch; migration blocks if any recorded
operation lacks it or if compatibility cannot be proved. Evidence for a
pinned provider must identify the same provider.
Successful migration fences the prior coordinator and hook claims; the
coordinator must acquire a fresh transition lease before resuming.
The shared migration and process-restart contracts run against both adapters
with a disposable MySQL DSN:
`go test ./adapters/sqlite ./adapters/mysql -run '^(TestRuntimeMigrationContract|TestRuntimeOperationMigration|TestRuntimeMigrationWithoutProvider|TestContinuationMigrationContract|TestMigrationOperationProcessRestart|TestMigrationWriteProcessCrash)$' -count=1`.
`Coordinator.Reconcile` scans persisted active executions; the host must call it
regularly, including without wakeups, and often enough to renew its leases.
`RunReconciliation` can schedule these scans using a bounded `WakeupQueue` and
a polling ticker. Notify the queue after durable work changes; notifications
are coalesced hints and can be lost. The loop performs an initial scan, owns
and stops the ticker, and returns on a scan error or cancellation. The host
must observe errors and restart it with a new ticker when appropriate.
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
`ContinuationDiscovery` and the adapters' durable marker stores retain
Continuation intents for a host-scheduled `ContinuationDriver`.
Acceptance pins an opted-in target's policy; the shared-store commit writes a
marker atomically. Separate-store hosts must attest to atomically retaining a
replayable marker before commit, and schedule discovery. Receipt reconciliation
waits for the matching runtime marker. Listing a marker does not establish that
its origin is ready for evaluation; `ContinuationReady` checks the successful
receipt and original reservation release. Missing origins return
`ErrContinuationOriginMissing`; discovery leaves those host markers
unacknowledged for retry, and readiness diagnoses previously stored orphans.
The host must keep polling durable markers. The driver evaluates only the
pinned machine version, persists its first decision, and uses the pinned
attempt, deadline, and retry policy. Reservation waits pause an already
started attempt. Exhaustion and definitive denial require an authorized retry;
an authorized waiver fences evaluation and reconciles acceptance before it
finishes. Use `ContinuationControls` for authorized control and inspection.
The successor-acceptance substrate reserves a stable binding before a
separate-store host records its authorization, origin revision, receipt and
durable writer fence. Schedule `SuccessorRecovery.Reconcile` to resolve
uncertain outcomes; lack of a response is not evidence that acceptance failed.
For a shared database, `AcceptSuccessorAtomicDecision` runs the host's origin
check, authorization, audit and fence in the adapter transaction and retains
definitive denial or supersession. Host integration
must enforce the fence for every state writer and release it only with a
fenced cancellation or safe completion. A host unable to do so must reject
acceptance. The receipt timestamp starts the execution deadline; host and
runtime clocks must be compatible.

Preparation, inspection, finalization and atomic adapter callbacks are
trusted internal primitives, not externally authorized endpoints. Use
`Acceptor.ReplaySuccessor` and `Acceptor.InspectSuccessor` with host disclosure
authorization, and authorize any other disclosure separately. Internal recovery must remain
available without replay permission. A `ContinuationDeliveryHost` must check
the delivery permit's expiry inside the acceptance transaction using a clock
consistent with the runtime. Its lookup must serialize with in-flight
acceptance, and cancellation must atomically return existing acceptance or
supersession, or persist a tombstone that rejects every delayed delivery for
that identity. An unknown result is not proof of absence. Do not route
Continuation delivery through an unfenced legacy acceptance path.
Its source must retain unacknowledged markers across restarts and provide
pagination cursors that remain valid when earlier markers are acknowledged.
The separate-host cancellation restart contract can be run on both adapters
with `go test ./adapters/sqlite ./adapters/mysql -run '^TestSuccessorCancellationProcessRestart$' -count=1`
and a disposable MySQL DSN. Its SQLite host is a test fixture, not a
production fencing implementation.
Driver, control, and process-restart coverage for both adapters can be run
with `go test ./internal/successorcontract ./adapters/sqlite ./adapters/mysql -run '^(TestDeliveryPermitCheckedAtHostAcceptance|TestWaiverTombstoneRacesAcceptance|TestContinuationDriv)' -count=1`
and a disposable MySQL DSN.
`NotificationWorker` claims and delivers intents independently, renews claims
while delivering, and schedules retries with its own configured timing. The
host schedules worker calls and must make downstream delivery idempotent by
intent ID. A delivery error is returned even when retry scheduling succeeds.
Timed-out deliveries may continue if the provider ignores context cancellation;
their late results cannot complete the timed-out claim.
When a worker cannot rehydrate a pinned hook before starting it, the adapter
persists a safe compatibility diagnostic without consuming an attempt.
`CompatibilityCoordinator` revalidates these blocks after behavior is restored;
it also discovers missing pinned behavior for waiting, quiesced retry/recovery,
and intervention work, fencing any unstarted claim. It does not interrupt
started or successfully completed hooks. The host must schedule that scan and
use the same coordinator owner.
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

`internal/runtimecontract` provides reusable execution, wakeup, worker-loss,
commit-rejection, notification-handoff, delivery and repair,
timer-delivery and compatibility, typed-ingress, authorized-control, migration, capacity,
retry-budget, pinned versions, and execution-compatibility contracts over the
runtime ports.
Run them against the SQLite adapter with:

```sh
go test ./adapters/sqlite -run 'TestRuntime.*Contract'
```

## SQLite acceptance store

`adapters/sqlite` provides a host-injected file-backed acceptance store.
`sqlite.Open` takes a file path and canonical, round-tripping codecs for entity
and state identifiers; `sqlite.StringCodec` supports string identifiers.
Close the store when finished. It uses SQLite immediate transactions, a
five-second busy timeout, foreign keys, and `synchronous=FULL`. Durability
depends on the filesystem and hardware honoring SQLite's sync and locking
requirements. In-memory databases are not supported by this adapter.

The adapter owns its `rff_` tables and rejects unsupported schemas without
rewriting them. Fresh stores include the operation tables; pre-operation
stores without them are incompatible and are not migrated or backfilled.
Operation-only version 10 stores are upgraded transactionally with the
Continuation table; continuation-only version 10 stores are incompatible.
Stores without the successor table are incompatible and are not backfilled.
Unreadable operation records are isolated to the affected identity or scan.
Older adapter versions cannot open the new schema.
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
For same-file hosts, `Store.AcceptInputAtomic` atomically records the input
outcome with execution acceptance or a classified definitive rejection.
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
recorded as unknown. Compatibility discovery is not exhaustive: in-flight and
completed work depends on a worker or coordinator encountering missing behavior.

```sh
go test ./adapters/sqlite
go test ./adapters/sqlite -run TestAcceptanceStoreContract
go test ./adapters/sqlite -run 'TestCoordinator|TestAbortRecovery|TestRecoveryDeadline|TestAuthorizedRetry|TestSQLiteWorker'
go test ./adapters/sqlite -run 'TestNotificationHandoff|TestNotificationRepair'
go test ./adapters/sqlite -run TestNotificationDelivery
go test ./adapters/sqlite -run TestAtomicCommit
go test ./adapters/sqlite -run TestIncompatibleExecution
go test ./adapters/sqlite -run TestMigration
go test ./runtime ./adapters/sqlite -run 'TestTrackedOperation|TestRuntimeOperation|TestRuntimeConcurrentOperations|TestOperationProcessRestart|TestOperationRecordIsolation' -count=1
go test ./adapters/sqlite -run TestTimer
go test ./adapters/sqlite -run 'TestTypedInput|TestTypedCommand'
go test ./runtime -run TestNotificationWorker
go test ./runtime
go test ./internal/taskmanager
```

## MySQL acceptance store

`adapters/mysql` provides the same runtime persistence ports on MySQL 8.4 LTS
or later, using InnoDB. `mysql.Open` takes a standard `database/sql` MySQL DSN
that selects an application database and the same canonical identifier codecs
as the SQLite adapter. The adapter opens and owns its connection pool; close
the store when finished. Its versioned `rff_` schema is independent of
SQLite's schema: it does not import, migrate, or read data from SQLite.
The host owns application-domain tables.

Adapter transactions acquire an InnoDB serialization row lock, including
fenced leases and same-transaction host callbacks. The serialized transaction
boundary deliberately mirrors SQLite's immediate transaction semantics and
coordinates multiple adapter instances/processes. Identifier bytes are stored
without prefix truncation; bounded prefix indexes only accelerate lookups, with
exact-value checks and the serialization lock preserving full-key semantics.
An incomplete, incompatible, or unsupported MySQL adapter schema is rejected;
MySQL does not automatically migrate older adapter schemas, including
version 2 stores from either feature branch.
Run the shared runtime persistence contract entry points and the
MySQL-specific schema and concurrency tests locally with
`./scripts/check-mysql.sh`, which requires `RFF_MYSQL_DSN` to point to a
disposable MySQL 8.4 database with privileges to create and drop isolated test
databases. The standard `./scripts/check.sh` and CI remain MySQL-independent.
With that DSN set, run the focused dual-adapter operation checks using:

```sh
go test ./runtime -run 'TestTrackedUnknownOperation|TestUnknownMutation|TestAuthoritativeAbsence|TestTrackedReadRepeat|TestExpiredEvidence' -count=1
go test ./adapters/sqlite ./adapters/mysql -run 'TestRuntimeOperation(Reconciliation)?Contract|TestOperationProcessRestart|TestOperationRecordIsolation' -count=1
```

## Local task-manager fixture

The fixture CLI is for local development and conformance testing only. Seeded
identities are opaque test values, not authentication. The test-only operator
can inspect and control executions but has no task-manager permissions.
Each invocation opens the same file-backed fixture; `drive` performs one
reconciliation round and must be run again when more work or timers are due.
No background service or real notification provider is started.
The typed fixture's `MigrateExecution` demonstrates operator-authorized migration
of pre-commit task work to behavior version 2. It verifies the authoritative
task source while SQLite holds the same-file write transaction; this does not
make migrations across separate stores atomic.
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

`internal/fixturecontract` provides reusable typed-API checks for invitation,
membership history, expiry, closure, access, task progression, removal,
ownership transfer, accepted-work invariants, and operator controls against a
fixture implementation, as well as notification failure and expiry behavior.

Run the CLI and fixture checks, including a real subprocess recovery test, with:

```sh
go test ./internal/taskmanager -run 'Test.*FixtureContract'
go test ./cmd/taskmanager ./internal/taskmanager
go test ./internal/taskmanager -run 'Test(FixtureRecoversExpiryAndNotificationAcrossProcessRestart|OwnershipTransferSurvivesProcessLoss)'
go test ./adapters/sqlite -run 'Test(AcceptedAndStartedWork|DomainCommit)SurvivesActualProcessLoss'
./scripts/check.sh
```

Pull-request CI runs `./scripts/check.sh`, including these conformance and
process-restart tests. Passing checks alone is not a claim of production
readiness or exactly-once external delivery; the home repository's
[acceptance matrix](https://github.com/reinaldoborges/rff-home/blob/main/docs/first-slice-acceptance.md)
defines the complete single-node gate.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
See [third-party notices](THIRD_PARTY_NOTICES.md) when redistributing builds
that include the SQLite adapter.
