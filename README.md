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
