#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCK="$ROOT/Cargo.lock"

if [[ ! -f "$LOCK" ]]; then
  echo "Cargo.lock not found at $LOCK" >&2
  exit 1
fi

awk '
BEGIN {
  split("candid digest ic-cdk ic-cdk-executor ic-cdk-macros ic-memory ic-stable-structures ic0 ic_principal icrc-ledger-types sha2", sensitive)
  for (idx in sensitive) {
    sensitive_set[sensitive[idx]] = 1
  }
  banned_set["canic-cdk"] = 1
}

function strip_value(line) {
  sub(/^[^"]*"/, "", line)
  sub(/".*$/, "", line)
  return line
}

function record_package() {
  if (name == "") {
    return
  }
  if (name in banned_set) {
    banned[name] = version
  }
  if (name in sensitive_set) {
    key = name SUBSEP version
    if (!(key in seen_version)) {
      seen_version[key] = 1
      version_count[name] += 1
      versions[name] = versions[name] " " version
    }
  }
}

$0 == "[[package]]" {
  record_package()
  name = ""
  version = ""
  next
}

$1 == "name" {
  name = strip_value($0)
  next
}

$1 == "version" {
  version = strip_value($0)
  next
}

END {
  record_package()

  failed = 0
  for (crate in banned) {
    printf "banned dependency resolved: %s %s\n", crate, banned[crate] > "/dev/stderr"
    failed = 1
  }
  for (crate in version_count) {
    if (version_count[crate] > 1) {
      printf "duplicate sensitive dependency versions for %s:%s\n", crate, versions[crate] > "/dev/stderr"
      failed = 1
    }
  }
  if (failed) {
    print "Dependency graph invariant check failed." > "/dev/stderr"
    exit 1
  }
}
' "$LOCK"

echo "Dependency graph invariants passed."
