# RF-DSF Go implementation

Experimental Go implementation of the RF-DSF first slice. The `fsm` package
provides typed state-machine selection; the durable runtime is not yet available.

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
without writes for unsupported schemas. It migrates version 1 acceptance stores
to version 2 transactionally, preserving accepted records and reservations.
It currently implements acceptance, active-execution discovery, and transition
leases only, not worker execution, domain commits, or notifications.

```sh
go test ./adapters/sqlite
go test ./adapters/sqlite -run TestAcceptanceStoreContract
```
