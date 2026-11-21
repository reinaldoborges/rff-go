# RF-DSF Go implementation

Experimental Go implementation of the RF-DSF first slice. The `fsm` package
is a placeholder; it does not yet implement state-machine behavior.

The [home repository](https://github.com/reinaldoborges/rff-home) owns the
[architecture](https://github.com/reinaldoborges/rff-home/blob/main/docs/architecture.md),
design decisions, and implementation issues.

Go 1.27.3 and a C compiler are required for all checks, including race detection.
See [AGENTS.md](AGENTS.md) for the verification commands.
