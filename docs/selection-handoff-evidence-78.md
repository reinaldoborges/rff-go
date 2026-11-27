# Indexed selection handoff evidence

For [rff-home#78](https://github.com/reinaldoborges/rff-home/issues/78);
decisions [#63](https://github.com/reinaldoborges/rff-home/issues/63),
[#66](https://github.com/reinaldoborges/rff-home/issues/66), and
[#67](https://github.com/reinaldoborges/rff-home/issues/67).

## Revision and environment

The requested base was clean at `25a48a47eb9c47a78b3adefc95b31c6b027b387d`;
it contains the original pin `4ce0e74b4a6e78f955cd5fef858402f0d7a868c7`.
That integration merge has parents `c37cf3a5a3c028e021d849067ef3616916b9ddd1`
(optional sweeps) and `514e29cabcce45aa8553e6e774e5fbba857b1e90` (unreadable
execution recovery). The pin is therefore not the checkout baseline.

The recorded environment was Linux, Go 1.27.3, CGO, SQLite 3.54.1
(`github.com/mattn/go-sqlite3` v1.14.53), MySQL 8.4.13
(`github.com/go-sql-driver/mysql` v1.9.3), and `STRICT_TRANS_TABLES`.
The isolated MySQL 8.4 service used the documented root test account; the
new-connection strict-mode test also requires permission to change global
SQL mode and kill the test account's own sessions.

## Reproduction

Use fresh disposable stores and the MySQL setup in
[`testing-runtime-conformance.md`](testing-runtime-conformance.md). With
`RFF_MYSQL_DSN` set and the server healthy:

```sh
go test ./adapters/sqlite ./adapters/mysql -run '^TestSelectionHandoffPlans$' -count=1 -v
go test ./adapters/sqlite ./adapters/mysql -run '^TestRuntimeIndexed(Hook|Timer)SelectionContract$' -count=1 -v
./scripts/check-mysql.sh
./scripts/check.sh
```

In the recorded run, the Go commands ran inside the `golang:1.27.3` image
with CGO enabled. [`selection-plans-78.log`](selection-plans-78.log) captures
the first command's actual SQL and plans. These commands create disposable
databases; MySQL tests are **unverified** if `RFF_MYSQL_DSN` is absent and
tests skip. The scripts' usual container wrapper is documented in the
conformance guide.

## Fixture and schema fingerprints

The plan fixture uses adapter APIs to create decodable records: two completed
hooks with pending notifications; 24 SQLite or 256 MySQL additional active
hooks and timers (half due, half future); and the same number of pending inputs
(half per exact channel). The clock is fixed at
`2026-01-01T12:00:00.123456789Z`. All selection queries are extracted from
the corresponding adapter function, with real parameters, and checked for
their essential eligibility predicates and orderings. Additional shared
contracts exercise specific lease, compatibility and corruption cases.

The logged SHA-256 fingerprints cover JSON encoding of ordered initialized
schema definitions, not table contents. SQLite hashes `(type, name, table,
SQL)` from `sqlite_master`, excluding `sqlite_%` objects (table SQL retains
their declared constraints). MySQL hashes ordered `SHOW CREATE TABLE` output
with data-dependent `AUTO_INCREMENT` counters removed, followed by ordered
trigger name, table, timing, event and body from `information_schema`. JSON
is produced by Go `encoding/json`. The fixture and hashing code are in
`internal/selectionevidence/evidence.go`.

| Adapter/version | Objects | SHA-256 |
| --- | ---: | --- |
| SQLite 3.54.1 | 48 | `e08b06199517d4fd4a080219910a93828d33a55e754cff74ea02efdeac788ba8` |
| MySQL 8.4.13 | 28 | `5785e98d73201db568d3408bb7eb93531b8e401e836e02e6b706eab441f7a50e` |

Fingerprints are version-sensitive, not universal schema identifiers. SQLite
uses `rff_hook_ready_lookup(available_at_seconds, available_at_nanoseconds,
execution_id)` for incomplete, unstarted work;
`rff_timer_deadline_lookup(deadline_seconds, deadline_nanoseconds, id)` for
incomplete, compatible timers; `rff_notification_ready_lookup(execution_id)`
for incomplete notifications; and
`rff_input_selection_lookup(channel, outcome_present, id)`.
MySQL's corresponding indexes are `(completed, started, available_at_seconds,
available_at_nanoseconds, execution_id)`, `(completed, compatibility_blocked,
deadline_seconds, deadline_nanoseconds, id)`, `(completed, execution_id,
due_at_seconds, due_at_nanoseconds, lease_owner_present,
lease_expires_at_seconds, lease_expires_at_nanoseconds)`, and
`(channel(256), outcome_present, id)`, respectively. MySQL's separate unique
ID indexes cover full `VARBINARY(256)` IDs; the channel prefix is a performance
filter, not equality authority. Open findings have indexed kind/locator lookup.

## Plan observations

The full captured predicates, parameterized SQL, and plans are in the log.
The test does not require a fixed index, join order, estimate, or filesort.

| Path | SQLite 3.54.1 capture | MySQL 8.4.13 capture |
| --- | --- | --- |
| Hook | indexed execution-status lookups, hook/active ID lookups and indexed finding probes; temporary ordering B-tree | ordered hook execution-ID index scan, keyed joins and finding probes |
| Timer | deadline-index search with seconds upper bound; indexed finding probe | deadline-index range and indexed finding probe |
| Notification | ready-index range on execution ID | execution-ID index range |
| Input | channel/outcome selection-index search | channel/outcome selection-index lookup, with full channel predicate |

The original SQLite timer query produced a `Rewind`/`Next` walk of the entire
unfinished timer index when no deadline was due; the pre-fix plan was `SCAN t
USING INDEX rff_timer_deadline_lookup`. The added logically redundant
`deadline_seconds <= ?` upper bound permits an indexed range while preserving
the original nanosecond comparison. The plan test rejects that specifically
demonstrated unbounded bytecode shape, not alternative plans.

On the small 24-hook MySQL fixture, a no-due `EXPLAIN ANALYZE` observed an
ordered ID-index walk of 26 hook rows; that is actual work for that plan, not
an estimate. With the recorded 256-hook fixture, the no-due plan instead used
the ready-index time range and returned zero rows from that range, with a sort
permitted. This demonstrates a version/cardinality-dependent optimizer choice;
it does not establish a general bound or latency guarantee. The raw plan
contains estimates and timings which are observations, not measured SLOs.

## Acceptance-to-evidence map

Unless stated otherwise, the named adapter tests run on **both** adapters:

| Acceptance | Executable evidence |
| --- | --- |
| Runtime-valid eligibility, ordering, boundaries | `TestRuntimeIndexedHookSelectionContract`, `TestRuntimeIndexedTimerSelectionContract`, `TestNotificationClaimsUseFullIDOrderAndInclusiveBoundaries`, `TestPendingInputSelectionContract`, and `TestRuntimeTimerCompatibilityContract`. Fixtures cover early/late/none, started/completed/blocked work, active and expired leases, due/lease equality, timer ties, future nanoseconds, exact channels and unbounded input results. |
| Atomic claims and fencing | The hook and timer contracts' `competing claims` and expiry cases; `TestCompetingNotificationClaimsHaveOneWinner`; `TestRuntimeStoreContract/worker_loss_and_stale_fence` checks unknown attempts and redelivery. Coordination uses channels and fixed clocks, not sleeps to prove races. Fences cannot stop already-running external calls. |
| IDs, bytes and strict mode | `TestDurableIdentityByteExactRoundTrip`, `TestDirectWritesRejectInvalidDurableIDs`, `TestDirectAcceptanceRejectsInvalidExecutionID`, `TestExistingWorkWriteEntryPointsRejectInvalidIDs`, and runtime `TestTimerSchedulerRejectsInvalidIdentitiesBeforePersistence`, `TestInputIngressRejectsInvalidIdentitiesBeforePersistence`, `TestAcceptorRejectsInvalidGeneratedExecutionID`, and `TestRuntimePassesValidDurableIDsUnchanged`. They cover empty/invalid UTF-8, 256/257-byte boundaries, embedded NUL, case, normalization, multibyte and shared-prefix IDs. `TestPendingInputSelectionContract` covers full-channel prefix collisions. MySQL `TestStrictModeOnNewAndReusedConnections` forces a new connection and tests session reset, in addition to `TestOpenRejectsNonStrictConnection`. |
| Findings, quarantine and repair | Hook, timer and notification projection mismatch tests; `TestPendingInputKnownChannelQuarantineAndRepair`, `TestPendingInputUnknownChannelQuarantineIsIncomplete`, `TestTimerQuarantineRequiresFreshAuthorizedResolution`/`TestTimerIntegrityMaintenance`, and `TestRestoreUnreadableExecutionPreservesStartedWorkAndEvidence`. Maintenance is worker-free, authorized and audited; unreadable authority is restored only from trusted bytes and unknown effects remain unresolved. |
| Optional sweeps and lifecycle | `TestIntegritySweepResumesBoundedCoverageAndContinuesPastFindings`, `TestIntegritySweepCheckpointFailureDoesNotCommitFindingOrProgress`, `TestIntegritySweepTreatsUnknownTimerBehaviorAsIncompleteNotCorrupt`, `TestConcurrentIntegritySweepersShareOneDurableCursor`; runtime `TestLifecycleOptionalIntegritySweepDoesNotGateReadiness` and `TestLifecycleRetriesRecoverableIntegritySweepFailures`. Host scheduling calls the same worker-free `RunIntegritySweep` API exercised by adapter tests. |
| Fail-closed schema and record rejection | SQLite `TestUnsupportedSchemaFailsWithoutWriting` separately covers malformed projections, missing/malformed indexes and unreadable execution records with whole-file comparison. MySQL `TestSelectionSchemaFailuresDoNotRewriteData` checks altered table DDL and retained authority for separate malformed projection and missing/malformed index cases; `TestRestoreUnreadableExecutionPreservesStartedWorkAndEvidence` separately checks failed initialization and preserved unreadable bytes. MySQL's checks are not a whole-database write audit. |
| Hidden corruption limitation | `TestHiddenHookProjectionCanChangeFirstSelection`, `TestHiddenTimerProjectionCanChangeEarliestSelection` and `TestPendingInputSelectionContract` separately demonstrate a later selection or incomplete successful list when a projection hides an eligible authoritative row. Detected mismatches and known/unknown-channel quarantine are tested separately above. |

Candidate validation cannot inspect rows filtered out by SQL. Undetected
projection corruption may hide work, change the globally first/earliest
selection, or allow an incomplete successful input list. Optional sweeps
cover inspected rows within captured high-water marks; they are not current
global snapshots and are not required for ordinary readiness. Findings,
authorized repair, fencing, and at-least-once delivery do not imply exactly
once external effects. No benchmark or production-readiness claim is made.

## Lifecycle failure investigation

Two earlier standard `go test ./...` attempts failed in
`TestEmbeddedLifecycleDurableProgressAndShutdown`:

```text
lifecycle_test.go:169: concurrent hooks did not start: map[b:true], error transition: temporary lifecycle store failure
    hook-worker-1: renew hook lease: hook activity lease is stale
lifecycle_test.go:204: b did not commit: transition: reconcile execution "2026-11-27T20:00:48.624380635Zb": transition lease is stale
    hook-worker-0: renew hook lease: hook activity lease is stale
    hook-worker-1: renew hook lease: hook activity lease is stale
```

The second attempt ran after the prior MySQL gate stopped; the isolated test
passed. To reproduce, run the test at both the integration base and the
evidence branch with the Go container constrained to `--cpus=1`, concurrently:

```sh
go test ./adapters/sqlite -run '^TestEmbeddedLifecycleDurableProgressAndShutdown$' -count=20 -timeout=180s
```

On the unchanged base, four of 20 iterations failed; on the evidence branch,
two failed. Both included `b did not commit` with a stale hook lease. The
test and production runtime/adapter code were identical at those revisions.
Changing only the test's worker lease from 150 ms to one second on the evidence
branch produced 20 passes under the same constraint; that diagnostic edit was
reverted. The evidence supports a baseline timing-sensitive test under
contention rather than an evidence-branch regression. It does not prove a
specific scheduling or SQLite-lock delay, and a passing run cannot rule out
future lease starvation. The test deliberately uses real time and 30 ms renewal
intervals; no runtime safety guarantee was relaxed.

A subsequent standard parallel run failed again with:

```text
lifecycle_test.go:204: b did not commit: transition: temporary lifecycle store failure
    hook-worker-0: report hook outcome: hook activity lease is stale
```

Because that baseline fixture repeatedly blocked the standard gate, its worker
lease was increased to one second. The test still waits for the interrupted
started attempt to expire and be recovered as unknown; the adjustment is only
a test timing margin, not a change to runtime lease or recovery semantics.

## Final validation

On the integration tip above with these changes applied:

- The MySQL 8.4.13 gate `./scripts/check-mysql.sh` passed: adapter tests,
  adapter race tests, host and batch restart scenarios, and the lifecycle
  process-recovery scenario. The gate used the isolated strict-mode service.
- The standard formatting check, `go vet ./...`, `go test ./...`, and
  `go test -race ./...` passed at normal package parallelism after the
  lifecycle fixture adjustment. They ran as the individual commands from
  `scripts/check.sh` in the pinned container; no MySQL DSN was set for this
  baseline, so MySQL integration is covered by the separate gate above.
- The plan capture and the shared hook/timer selection tests passed separately
  against both adapters. The test-only lease adjustment was SQLite-specific
  and followed the MySQL gate; it did not alter the MySQL-tested code.
