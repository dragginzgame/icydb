#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCK="${ROOT}/Cargo.lock"

if [[ ! -f "${LOCK}" ]]; then
  echo "Cargo.lock not found at ${LOCK}" >&2
  exit 1
fi

# These crates are version-sensitive in the canister/runtime dependency graph.
# Duplicates here usually mean two IC-facing dependencies are pulling different
# SDK or crypto stacks into the same build.
sensitive=(
  candid
  digest
  ic-cdk
  ic-cdk-executor
  ic-cdk-macros
  ic-memory
  ic-stable-structures
  ic0
  ic_principal
  icrc-ledger-types
  sha2
)

# Broad facades are intentionally not part of IcyDB's live dependency graph.
banned=(
  canic-cdk
)

declare -A sensitive_set=()
declare -A banned_set=()
declare -A seen_versions=()
declare -A found_banned=()

for crate in "${sensitive[@]}"; do
  sensitive_set["${crate}"]=1
done

for crate in "${banned[@]}"; do
  banned_set["${crate}"]=1
done

name=""
version=""

record_package() {
  if [[ -z "${name}" ]]; then
    return
  fi

  if [[ -n "${banned_set[${name}]:-}" ]]; then
    found_banned["${name}"]="${version}"
  fi

  if [[ -n "${sensitive_set[${name}]:-}" ]]; then
    seen_versions["${name}|${version}"]=1
  fi
}

while IFS= read -r line; do
  case "${line}" in
    "[[package]]")
      record_package
      name=""
      version=""
      ;;
    "name = "*)
      name="${line#name = \"}"
      name="${name%\"}"
      ;;
    "version = "*)
      version="${line#version = \"}"
      version="${version%\"}"
      ;;
  esac
done <"${LOCK}"

record_package

failed=0

for crate in "${banned[@]}"; do
  if [[ -n "${found_banned[${crate}]:-}" ]]; then
    echo "banned dependency resolved: ${crate} ${found_banned[${crate}]}" >&2
    failed=1
  fi
done

for crate in "${sensitive[@]}"; do
  versions=()
  for key in "${!seen_versions[@]}"; do
    if [[ "${key}" == "${crate}|"* ]]; then
      versions+=("${key#${crate}|}")
    fi
  done

  if (( ${#versions[@]} > 1 )); then
    printf 'duplicate sensitive dependency versions for %s:' "${crate}" >&2
    printf ' %s' "${versions[@]}" >&2
    printf '\n' >&2
    failed=1
  fi
done

if (( failed != 0 )); then
  echo "Dependency graph invariant check failed." >&2
  exit 1
fi

echo "Dependency graph invariants passed."
