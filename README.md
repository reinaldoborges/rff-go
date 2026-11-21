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

The checks require Go 1.27.3; race detection also requires a C compiler.
See [AGENTS.md](AGENTS.md) for the verification commands.

## Transition acceptance

The `runtime` package defines an acceptor and a host-injected atomic acceptance
store. Applications supply versioned codecs, an execution-ID generator, and a
clock. Codecs must preserve semantic equality and report when it cannot be
verified. The repository does not yet include a durable adapter.

Adapter tests can use `internal/acceptancecontract` with an isolated store and
injected failures before commit and after commit with a lost response. The
in-memory test fake exercises that contract without claiming durability:

```sh
go test ./runtime -run TestAcceptanceStoreContract
```
