#!/usr/bin/env bash
set -euo pipefail

# Read-only exact stable-version observation: 0 present, 1 absent, 2 unknown/invalid.
if [[ $# != 2 ]]; then
    echo 'usage: check-crates-io-version.sh CRATE X.Y.Z (exit: 0 present, 1 absent, 2 unknown)' >&2
    exit 2
fi
crate="$1"
version="$2"
if [[ ! "$crate" =~ ^[A-Za-z][A-Za-z0-9_-]*$ ||
      ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    echo 'registry observation requires a crate name and canonical stable X.Y.Z version' >&2
    exit 2
fi
# Disable implicit curl configuration; do not retry, poll or select credentials.
status="$(curl --disable --silent --show-error --location \
    --proto '=https' --proto-redir '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 30 \
    --output /dev/null --write-out '%{http_code}' \
    --user-agent 'dragginzgame-shared-tooling (https://github.com/dragginzgame/shared-tooling)' \
    "https://crates.io/api/v1/crates/$crate/$version")" || {
    echo "registry observation unavailable for $crate $version (transport failure)" >&2
    exit 2
}
case "$status" in
    200) exit 0 ;;
    404) exit 1 ;;
    *) echo "registry observation unavailable for $crate $version (HTTP $status)" >&2; exit 2 ;;
esac
