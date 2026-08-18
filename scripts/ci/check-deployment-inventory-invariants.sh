#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "deployment inventory invariant checks"

status=0
policy_source="testing/integration/src/canister_artifact.rs"

maintained_names="$({
  awk '
    /pub const MAINTAINED_CANISTER_POLICIES:/ { in_policies = 1; next }
    in_policies && /^];/ { exit }
    in_policies && /canister: "/ {
      name = $0
      sub(/^.*canister: "/, "", name)
      sub(/".*$/, "", name)
      print name
    }
  ' "$policy_source"
})"

configured_names="$({
  awk '
    /^canisters:/ { in_canisters = 1; next }
    /^networks:/ { exit }
    in_canisters && /^  - name: / {
      name = $0
      sub(/^  - name: /, "", name)
      print name
    }
  ' icp.yaml
})"

configured_build_names="$({
  awk '
    /^canisters:/ { in_canisters = 1; next }
    /^networks:/ { exit }
    in_canisters && /- scripts\/app\/build\.sh / {
      command = $0
      sub(/^.*- scripts\/app\/build\.sh /, "", command)
      print command
    }
  ' icp.yaml
})"

if [[ -z "$configured_names" ]]; then
  echo "[ERROR] icp.yaml declares no deployable canisters." >&2
  status=1
fi

if [[ "$configured_names" != "$configured_build_names" ]]; then
  echo "[ERROR] every icp.yaml canister must invoke exactly 'scripts/app/build.sh <same-name>' in declaration order." >&2
  status=1
fi

duplicate_names="$({ printf '%s\n' "$configured_names" | sort | uniq -d; })"
if [[ -n "$duplicate_names" ]]; then
  echo "[ERROR] icp.yaml contains duplicate canister declarations:" >&2
  printf '%s\n' "$duplicate_names" >&2
  status=1
fi

while IFS= read -r canister; do
  if ! rg -q -x --fixed-strings "$canister" <<<"$maintained_names"; then
    echo "[ERROR] icp.yaml canister '$canister' is absent from MAINTAINED_CANISTER_POLICIES." >&2
    status=1
  fi
done <<<"$configured_names"

environment_names="$({
  awk '
    /^    canisters: \[/ {
      names = $0
      sub(/^    canisters: \[/, "", names)
      sub(/\]$/, "", names)
      count = split(names, canisters, /, */)
      for (item = 1; item <= count; item += 1) {
        print canisters[item]
      }
    }
  ' icp.yaml
})"

while IFS= read -r canister; do
  if [[ -n "$canister" ]] && ! rg -q -x --fixed-strings "$canister" <<<"$configured_names"; then
    echo "[ERROR] icp.yaml environment references undeclared canister '$canister'." >&2
    status=1
  fi
done <<<"$environment_names"

if [[ $status -ne 0 ]]; then
  exit "$status"
fi

echo "[OK] Deployment inventory invariants verified."
