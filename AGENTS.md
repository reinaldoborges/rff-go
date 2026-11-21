# Implementation guidance

Use Go 1.27.3. Race detection also requires a supported C compiler.

For implementation work, read the home repository's
[architecture baseline](https://github.com/reinaldoborges/rff-home/blob/main/docs/architecture.md)
and [implementation contract](https://github.com/reinaldoborges/rff-home/blob/main/docs/implementation-agent-contract.md),
then follow the design revisions and acceptance criteria pinned by the home issue.
Link implementation PRs to the home issue and relevant decisions. Raise missing
or conflicting design decisions in the home issue before affected work proceeds.

Run the applicable issue-specific checks and the repository baseline from the
repository root:

```sh
./scripts/check.sh
```

The script checks Go formatting, runs `go vet ./...`, `go test ./...`, and
`go test -race ./...`.

Report checks that cannot run. Documentation-only changes need applicable
documentation checks rather than unrelated Go checks.
