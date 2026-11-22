# Implementation guidance

Use the Go version pinned in `go.mod`, with CGO enabled and a supported C
compiler for the SQLite adapter and race detection.

For implementation work, read the home repository's
[architecture baseline](https://github.com/reinaldoborges/rff-home/blob/main/docs/architecture.md)
and [implementation contract](https://github.com/reinaldoborges/rff-home/blob/main/docs/implementation-agent-contract.md),
then follow the design revisions and acceptance criteria pinned by the home issue.
Link implementation PRs to the home issue and relevant decisions. Raise missing
or conflicting design decisions in the home issue before affected work proceeds.

For repeated local checks, reuse a running Go validation container with this
checkout mounted at `/work`. If none exists, choose a unique container name for
this checkout and start one from the repository root:

```sh
CHECKS_CONTAINER="rff-go-checks-$(id -u)-$$"
docker run -d --name "$CHECKS_CONTAINER" \
  --mount "type=bind,src=$PWD,dst=/work" -w /work \
  "golang:$(awk '/^go / {print $2; exit}' go.mod)" sleep infinity
printf 'Validation container: %s\n' "$CHECKS_CONTAINER"
```

Verify the container is running, its `/work` mount is this checkout, and
`docker exec "$CHECKS_CONTAINER" go version` matches `go.mod`. Record its name,
set `CHECKS_CONTAINER` to that name in later shells, and reuse it across test
rounds; leave it running for subsequent work. Run
issue-specific checks and the repository baseline inside it, for example:

```sh
docker exec "$CHECKS_CONTAINER" ./scripts/check.sh
```

Report checks that cannot run. Documentation-only changes need applicable
documentation checks rather than unrelated Go checks.
