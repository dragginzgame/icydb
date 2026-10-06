#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

cd "$ROOT"
export PATH="$ROOT/.tools/host/bin:$PATH"

BUMP_TYPE=${1:-patch}
INTERNAL_WORKSPACE_PACKAGES=(
  icydb
  icydb-core
  icydb-diagnostic-code
  icydb-model
  icydb-model-macros
  icydb-schema
)

LOCKFILE_SNAPSHOT_DIR=""
LOCKFILE_SNAPSHOT=""

cleanup_lockfile_snapshot() {
  local status=$?
  if [[ -n "$LOCKFILE_SNAPSHOT_DIR" && -d "$LOCKFILE_SNAPSHOT_DIR" ]]; then
    if [[ "$status" == 0 ]]; then
      find "$LOCKFILE_SNAPSHOT_DIR" -depth -delete
    else
      echo "Failed version preparation retained: $LOCKFILE_SNAPSHOT_DIR" >&2
    fi
  fi
}

trap cleanup_lockfile_snapshot EXIT

if ! cargo set-version --help >/dev/null 2>&1; then
  echo "❌ cargo set-version not available. Install cargo-edit or upgrade Rust." >&2
  exit 1
fi

# Current version (from [workspace.package])
PREV=$(bash scripts/ci/read-cargo-workspace-version.sh --stable "$ROOT/Cargo.toml") || exit 1

# Keep the tested dependency graph fixed while changing workspace versions.
# cargo-edit may re-resolve registry or target-specific edges while updating
# the lockfile, so retain the validated lock and change only exact workspace
# package version declarations after the manifests have moved.
if [[ -f Cargo.lock ]]; then
  LOCKFILE_SNAPSHOT_DIR="$(mktemp -d)"
  LOCKFILE_SNAPSHOT="$LOCKFILE_SNAPSHOT_DIR/Cargo.lock"
  cp Cargo.lock "$LOCKFILE_SNAPSHOT"
  cargo metadata --locked --offline --no-deps --format-version 1 > "$LOCKFILE_SNAPSHOT_DIR/metadata.json"
fi

PLANNED="$(bash scripts/ci/next-release-version.sh "$PREV" "$BUMP_TYPE")" || exit 1
[[ -z "${RELEASE_VERSION:-}" || "$RELEASE_VERSION" == "$PLANNED" ]] || exit 1
cargo set-version --workspace "$PLANNED" --offline >/dev/null

# New version
NEW=$(bash scripts/ci/read-cargo-workspace-version.sh --stable "$ROOT/Cargo.toml") || exit 1

if [[ "$PREV" == "$NEW" ]]; then
  echo "Version unchanged ($NEW)"
  exit 0
fi

# Published IcyDB packages share private generated-code and schema contracts.
# Keep every registry-facing intra-workspace edge on the exact release rather
# than allowing Cargo's default caret range to split the family by patch.
for package in "${INTERNAL_WORKSPACE_PACKAGES[@]}"; do
  sed -E \
    "s#^(${package}[[:space:]]*=[[:space:]]*\\{[^}]*version[[:space:]]*=[[:space:]]*\\\")[^\\\"]+(\\\"[^}]*\\})#\\1=$NEW\\2#" \
    Cargo.toml > "$LOCKFILE_SNAPSHOT_DIR/manifest"
  cat "$LOCKFILE_SNAPSHOT_DIR/manifest" > Cargo.toml
  if ! grep -E "^${package}[[:space:]]*=" Cargo.toml |
    grep -Fq "version = \"=$NEW\""
  then
    echo "Failed to pin $package to exact workspace version =$NEW" >&2
    exit 1
  fi
done

# Pinning exceptions describe the existing coupled release constraint; they do
# not select another version. Reuse the release projection used by admission.
packages="$(printf '%s\n' "${INTERNAL_WORKSPACE_PACKAGES[@]}" | jq -Rn '[inputs]')"
jq --arg previous "$PREV" --arg release "$NEW" --argjson packages "$packages" \
  -f scripts/release/pin-exceptions.jq ci/dependency-pinning-exceptions.json \
  > "$LOCKFILE_SNAPSHOT_DIR/pin-exceptions"
cat "$LOCKFILE_SNAPSHOT_DIR/pin-exceptions" > ci/dependency-pinning-exceptions.json

if [[ -n "$LOCKFILE_SNAPSHOT" ]]; then
  cp "$LOCKFILE_SNAPSHOT" Cargo.lock
  # Cargo metadata owns the local roster; the shared transformer owns exact
  # lockfile identities and preserves every external dependency selection.
  jq -r --arg previous "$PREV" '
    .workspace_members as $members | .packages[] |
    select(.id as $id | $members | index($id)) |
    select(.version == $previous) | .name
  ' "$LOCKFILE_SNAPSHOT_DIR/metadata.json" > "$LOCKFILE_SNAPSHOT_DIR/local-packages"
  owned=()
  while IFS= read -r package; do owned[${#owned[@]}]="$package"; done < "$LOCKFILE_SNAPSHOT_DIR/local-packages"
  perl scripts/ci/rewrite-local-lock-versions.pl "$LOCKFILE_SNAPSHOT" "$PREV" "$NEW" \
    "${owned[@]}" > "$LOCKFILE_SNAPSHOT_DIR/new-lock"
  cat "$LOCKFILE_SNAPSHOT_DIR/new-lock" > Cargo.lock
  cargo metadata --locked --offline --no-deps --format-version 1 >/dev/null
fi

scripts/ci/sync-release-surface-version.sh "$NEW"

if git rev-parse "v$NEW" >/dev/null 2>&1; then
  echo "❌ Tag v$NEW already exists. Aborting." >&2
  exit 1
fi

echo "✅ Bumped: $PREV → $NEW"
