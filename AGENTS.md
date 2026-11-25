# Implementation guidance

Follow [Effective Go](https://go.dev/doc/effective_go) for idiomatic Go
style and implementation guidance.

For Go validation, container checks, and optional MySQL verification, see
[`docs/testing-runtime-conformance.md`](docs/testing-runtime-conformance.md).

For implementation work, read the home repository's
[architecture baseline](https://github.com/reinaldoborges/rff-home/blob/main/docs/architecture.md)
and [implementation contract](https://github.com/reinaldoborges/rff-home/blob/main/docs/implementation-agent-contract.md),
then follow the design revisions and acceptance criteria pinned by the home issue.
Link implementation PRs to the home issue and relevant decisions. Raise missing
or conflicting design decisions in the home issue before affected work proceeds.

Report checks that cannot run. Documentation-only changes need applicable
documentation checks rather than unrelated Go checks.
