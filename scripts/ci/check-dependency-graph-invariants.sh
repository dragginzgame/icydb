#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCK="$ROOT/Cargo.lock"
MANIFEST="$ROOT/Cargo.toml"
INTERNAL_WORKSPACE_PACKAGES=(
  icydb
  icydb-core
  icydb-diagnostic-code
  icydb-model
  icydb-model-macros
  icydb-schema
)

if [[ ! -f "$LOCK" || ! -f "$MANIFEST" ]]; then
  echo "Cargo manifest or lockfile is missing under $ROOT" >&2
  exit 1
fi

workspace_version="$(
  awk '
    /^\[workspace\.package\]$/ { in_workspace_package = 1; next }
    /^\[/ { in_workspace_package = 0 }
    in_workspace_package && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' "$MANIFEST"
)"
if [[ -z "$workspace_version" ]]; then
  echo "Unable to resolve the workspace package version from $MANIFEST" >&2
  exit 1
fi

for package in "${INTERNAL_WORKSPACE_PACKAGES[@]}"; do
  workspace_requirement="$(
    awk -v package="$package" '
      /^\[workspace\.dependencies\]$/ { in_workspace_dependencies = 1; next }
      /^\[/ { in_workspace_dependencies = 0 }
      in_workspace_dependencies && $1 == package { print; exit }
    ' "$MANIFEST"
  )"
  if [[ "$workspace_requirement" != *"version = \"=$workspace_version\""* ]]; then
    echo "$package must be pinned to exact workspace version =$workspace_version" >&2
    exit 1
  fi
done

non_workspace_internal_edges="$({
  rg -n --no-heading --color=never \
    '^[[:space:]]*icydb(-[a-z0-9-]+)?[[:space:]]*=' \
    crates/*/Cargo.toml || true
} | rg -v 'workspace[[:space:]]*=[[:space:]]*true' || true)"
if [[ -n "$non_workspace_internal_edges" ]]; then
  echo "Published IcyDB crates must inherit exact internal versions from the workspace:" >&2
  printf '%s\n' "$non_workspace_internal_edges" >&2
  exit 1
fi

workspace_time_version="$(
  awk '
    /^\[workspace\.dependencies\]$/ { in_workspace_dependencies = 1; next }
    /^\[/ { in_workspace_dependencies = 0 }
    in_workspace_dependencies && /^time[[:space:]]*=/ {
      line = $0
      sub(/^.*version[[:space:]]*=[[:space:]]*"/, "", line)
      sub(/".*$/, "", line)
      print line
    }
  ' "$MANIFEST"
)"
if [[ -z "$workspace_time_version" || "$workspace_time_version" == *$'\n'* ]]; then
  echo "Unable to resolve one workspace time dependency version from $MANIFEST" >&2
  exit 1
fi

awk -v required_time="$workspace_time_version" '
BEGIN {
  split("candid digest ic-cdk ic-cdk-executor ic-cdk-macros ic-memory ic-stable-structures ic0 ic_principal icrc-ledger-types sha2", sensitive)
  for (idx in sensitive) {
    sensitive_set[sensitive[idx]] = 1
  }
  banned_set["canic-cdk"] = 1
  banned_set["ic-agent"] = 1
  # Keep the resolved release aligned with the workspace dependency authority.
  required_exact["time"] = required_time
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
  if (name in required_exact && version != required_exact[name]) {
    exact_mismatch[name] = version
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
  for (crate in exact_mismatch) {
    printf "pinned dependency drifted: %s resolved %s, expected %s\n", crate, exact_mismatch[crate], required_exact[crate] > "/dev/stderr"
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
