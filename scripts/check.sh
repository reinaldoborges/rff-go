#!/bin/sh
set -eu

files="$(mktemp "${TMPDIR:-/tmp}/rff-go-files.XXXXXXXX")"
trap 'rm -f "$files"' EXIT
git ls-files -z --cached --others --exclude-standard -- '*.go' > "$files"
unformatted="$(xargs -0 sh -c '
  count=$#
  while [ "$count" -gt 0 ]; do
    file=$1
    shift
    if [ -f "$file" ]; then
      set -- "$@" "$file"
    fi
    count=$((count - 1))
  done
  if [ "$#" -gt 0 ]; then
    exec gofmt -l "$@"
  fi
' sh < "$files")"
if [ -n "$unformatted" ]; then
  printf 'Unformatted Go files:\n%s\n' "$unformatted" >&2
  exit 1
fi

go vet ./...
go test ./...
go test -race ./...
