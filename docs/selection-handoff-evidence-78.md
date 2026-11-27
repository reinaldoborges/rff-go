# Selection handoff evidence: incomplete

This is a partial evidence record, **not an approved handoff** for
[rff-home#78](https://github.com/reinaldoborges/rff-home/issues/78).

## Baseline

The requested integration branch was clean at
`25a48a47eb9c47a78b3adefc95b31c6b027b387d`, which contains the pinned
`4ce0e74b4a6e78f955cd5fef858402f0d7a868c7`. The integration commit is the
merge of `c37cf3a5a3c028e021d849067ef3616916b9ddd1` (optional integrity
sweeps) and `514e29cabcce45aa8553e6e774e5fbba857b1e90` (unreadable execution
recovery). The baseline therefore differs from the original pin.

## Evidence added

Both adapters use fresh stores with records created through runtime/adapter
interfaces. The hidden-hook fixture dispatches two eligible executions `a` and
`b`, then corrupts only `a`'s completed projection. Claiming returns `b`
without detecting the omitted earlier work. The hidden-timer fixture schedules
an earlier and a later timer, then moves only the earlier timer's projected
deadline into the future. Claiming returns the later timer without detecting
the omitted earlier one. The existing pending-input contract separately
demonstrates an incomplete successful listing after an undetected corrupt
channel projection.

These are explicitly accepted limitations, not guarantees of completeness
under corruption. Candidate validation cannot inspect rows filtered out by
SQL. Optional sweeps have bounded, checkpointed coverage and are not a current
global snapshot; no sweep is required for ordinary readiness.

SQLite initialization now has separate cases for an altered input projection
schema, a missing timer index, and a malformed timer index. Its existing
unreadable execution case is separate. All compare the database file before
and after rejected initialization. MySQL has separate altered projection,
missing-index and malformed-index cases that check the affected table DDL and
a retained authoritative input record after rejection. This is not a
whole-database write audit. Existing unreadable execution recovery tests
exercise failed normal initialization and preservation of the corrupt bytes.

## Reproduction and observed results

Go `1.27.3`, `github.com/mattn/go-sqlite3 v1.14.53`,
`github.com/go-sql-driver/mysql v1.9.3`, and MySQL `8.4.13` were used.
The MySQL server global mode included `STRICT_TRANS_TABLES`.
With the disposable MySQL service and `RFF_MYSQL_DSN` configured as in
[`testing-runtime-conformance.md`](testing-runtime-conformance.md), run:

```sh
go test ./adapters/sqlite ./adapters/mysql \
  -run '^(TestHidden(Hook|Timer)ProjectionCanChange.*|TestUnsupportedSchemaFailsWithoutWriting|TestSelectionSchemaFailuresDoNotRewriteData)$' \
  -count=1 -v
go test -race ./adapters/sqlite ./adapters/mysql \
  -run '^(TestHidden(Hook|Timer)ProjectionCanChange.*|TestUnsupportedSchemaFailsWithoutWriting|TestSelectionSchemaFailuresDoNotRewriteData)$' \
  -count=1
```

Both commands passed on both adapters with the Go container and MySQL service.
The documented `scripts/check-mysql.sh` gate also passed, including MySQL
adapter tests and race tests, host and batch process-restart scenarios, and the
lifecycle process-recovery scenario. It began while these evidence changes
were being written, so this is not a clean final-revision gate.
The normal full-suite attempt also ran `go vet ./...` successfully, but
`go test ./...` failed in `TestEmbeddedLifecycleDurableProgressAndShutdown`
with a stale hook lease while the MySQL suite was concurrently running; its
isolated rerun passed. A second full-suite attempt after the MySQL gate also
failed in the same lifecycle test, reporting stale transition and hook leases.
These failures block a clean standard baseline; the isolated pass does not
establish that they are harmless. After stopping MySQL, both
`go test -p 1 ./...` and `go test -race -p 1 ./...` passed. Those runs
serialized packages and did not set a MySQL DSN, so they do not replace the
standard parallel baseline or the separate MySQL gate.

## Handoff blockers

Representative query-plan captures for every selection path and adapter,
actual initialized-schema fingerprints, a complete acceptance-to-test mapping,
and a clean standard full-suite result are still required. MySQL strict mode on
every pooled connection and the rest of the issue's behavioral edge cases
have not been independently established by this partial record. The
observations above must not be interpreted as latency evidence, a production
readiness claim, or proof of a fixed optimizer choice.
