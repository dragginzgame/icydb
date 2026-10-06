#!/usr/bin/env bash
set -euo pipefail

# IcyDB retains exact client/server alignment; provisioning belongs upstream.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
locked="$(awk '
  /^\[\[package\]\]$/ { selected = 0 }
  /^name = "pocket-ic"$/ { selected = 1 }
  selected && /^version = / { gsub(/"/, "", $3); print $3 }
' "$ROOT/Cargo.lock")"
[[ "$locked" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo 'Cargo.lock must select exactly one PocketIC client' >&2; exit 1;
}
awk -F '\t' -v locked="$locked" '
  $1 == "pocket-ic" { count++; if ($2 != locked) bad = 1 }
  END { if (count != 3 || bad) exit 1 }
' "$ROOT/ci/ic-tools.tsv" || {
  echo "PocketIC server pins must match locked client $locked" >&2; exit 1;
}
echo "PocketIC client/server alignment verified: $locked"
