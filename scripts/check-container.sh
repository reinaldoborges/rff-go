#!/bin/sh
set -eu

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)"
cd "$root"
common="$(git rev-parse --path-format=absolute --git-common-dir)"
version="$(awk '/^go / { print $2; exit }' go.mod)"
if [ -z "$version" ]; then
  printf 'go.mod does not specify a Go version\n' >&2
  exit 1
fi

case "${1:-}" in
  ""|--full)
    if [ "$#" -gt 1 ]; then
      printf 'Usage: %s [--full | -- command [args...]]\n' "$0" >&2
      exit 2
    fi
    cache="${XDG_CACHE_HOME:-${HOME:?HOME or XDG_CACHE_HOME is required}/.cache}/rff-go"
    umask 077
    mkdir -p "$cache"
    exec 9>"$cache/full-check.lock"
    flock -x 9
    set -- ./scripts/check.sh
    ;;
  --)
    shift
    if [ "$#" -eq 0 ]; then
      printf 'A command is required after --\n' >&2
      exit 2
    fi
    ;;
  *)
    printf 'Usage: %s [--full | -- command [args...]]\n' "$0" >&2
    exit 2
    ;;
esac

uid="$(id -u)"
if [ "${RFF_MYSQL_DSN+x}" = x ]; then
  set -- -e RFF_MYSQL_DSN "golang:$version" "$@"
else
  set -- "golang:$version" "$@"
fi
docker run --rm \
  --mount "type=bind,src=$root,dst=/work" \
  --mount "type=bind,src=$common,dst=$common,readonly" \
  --mount "type=volume,src=rff-go-mod-$uid,dst=/go/pkg/mod" \
  --mount "type=volume,src=rff-go-build-$uid,dst=/root/.cache/go-build" \
  -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory \
  -e GIT_CONFIG_VALUE_0=/work -w /work "$@"
