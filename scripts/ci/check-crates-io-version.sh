#!/usr/bin/env bash
set -euo pipefail

# Read-only exact stable-version observation: 0 present, 1 absent, 2 unknown/invalid.
absent=false
# Filesystem/prerequisite failures must never escape as the "absent" status.
trap 'if [[ $? != 0 && "$absent" != true ]]; then exit 2; fi' EXIT
evidence=''
if [[ "${1:-}" == --metadata ]]; then
    [[ $# == 4 && -n "$2" ]] || {
        echo 'usage: check-crates-io-version.sh [--metadata NEW-DIRECTORY] CRATE X.Y.Z' >&2
        exit 2
    }
    evidence="$2"
    shift 2
fi
if [[ $# != 2 ]]; then
    echo 'usage: check-crates-io-version.sh [--metadata NEW-DIRECTORY] CRATE X.Y.Z (exit: 0 present, 1 absent, 2 unknown)' >&2
    exit 2
fi
crate="$1"
version="$2"
if [[ ! "$crate" =~ ^[A-Za-z][A-Za-z0-9_-]*$ ||
      ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    echo 'registry observation requires a crate name and canonical stable X.Y.Z version' >&2
    exit 2
fi
request_args=(--output /dev/null)
if [[ -n "$evidence" ]]; then
    command -v jq >/dev/null 2>&1 || { echo 'metadata observation requires jq' >&2; exit 2; }
    curl_version="$(curl --disable --version)" || exit 2
    # Earlier curl versions cannot cap responses without a Content-Length.
    if [[ ! "$curl_version" =~ ^curl[[:space:]]([0-9]+)\.([0-9]+)\. ]] ||
        (( 10#${BASH_REMATCH[1]} < 8 || (10#${BASH_REMATCH[1]} == 8 && 10#${BASH_REMATCH[2]} < 4) )); then
        echo 'bounded metadata observation requires curl 8.4.0 or newer' >&2
        exit 2
    fi
    [[ "$evidence" == /* ]] || evidence="$PWD/$evidence"
    umask 077
    mkdir "$evidence" || { echo 'metadata evidence directory must be new, with an existing parent' >&2; exit 2; }
    printf '%s\n' "https://crates.io/api/v1/crates/$crate/$version" > "$evidence/request.url"
    printf '%s\n' "$curl_version" > "$evidence/curl-version.txt"
    : > "$evidence/response.json"
    request_args=(--output "$evidence/response.json" --max-filesize 1048576 --stderr "$evidence/curl.stderr")
    printf 'Registry observation evidence retained: %s\n' "$evidence" >&2
fi
# Disable implicit curl configuration; do not retry, poll or select credentials.
transport=0
status="$(curl --disable --silent --show-error --location \
    --proto '=https' --proto-redir '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 30 \
    "${request_args[@]}" --write-out '%{http_code}' \
    --user-agent 'dragginzgame-shared-tooling (https://github.com/dragginzgame/shared-tooling)' \
    "https://crates.io/api/v1/crates/$crate/$version")" || transport=$?
if [[ -n "$evidence" ]]; then
    printf '%s\n' "$status" > "$evidence/http-status"
    printf '%s\n' "$transport" > "$evidence/curl-exit"
fi
if [[ "$transport" != 0 ]]; then
    echo "registry observation unavailable for $crate $version (transport failure)" >&2
    exit 2
fi
case "$status" in
    200) ;;
    404) absent=true; exit 1 ;;
    *) echo "registry observation unavailable for $crate $version (HTTP $status)" >&2; exit 2 ;;
esac
[[ -n "$evidence" ]] || exit 0
# Bound parser input independently of the transport and reject JSON streams.
if [[ "$(wc -c < "$evidence/response.json")" -gt 1048576 ]] ||
    ! jq -ces --arg crate "$crate" --arg version "$version" '
        select(length == 1) | .[0] | select(type == "object") |
        .version | select(type == "object") |
        select(.crate == $crate and .num == $version) |
        select(.checksum | type == "string") |
        select(.checksum | length == 64 and test("^[0-9a-f]{64}$")) |
        select(.yanked | type == "boolean") |
        {crate, version: .num, checksum, yanked}
    ' "$evidence/response.json" > "$evidence/metadata.json" 2> "$evidence/metadata.stderr"; then
    echo "registry metadata unavailable or invalid for $crate $version" >&2
    exit 2
fi
cat "$evidence/metadata.json"
