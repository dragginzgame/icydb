#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

cd "$ROOT"

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
  if [[ -n "$LOCKFILE_SNAPSHOT_DIR" && -d "$LOCKFILE_SNAPSHOT_DIR" ]]; then
    find "$LOCKFILE_SNAPSHOT_DIR" -depth -delete
  fi
}

trap cleanup_lockfile_snapshot EXIT

if ! cargo set-version --help >/dev/null 2>&1; then
  echo "❌ cargo set-version not available. Install cargo-edit or upgrade Rust." >&2
  exit 1
fi

# Current version (from [workspace.package])
PREV=$(cargo get workspace.package.version)

# Keep the tested dependency graph fixed while changing workspace versions.
# cargo-edit may re-resolve registry or target-specific edges while updating
# the lockfile, so retain the validated lock and change only exact workspace
# package version declarations after the manifests have moved.
if [[ -f Cargo.lock ]]; then
  LOCKFILE_SNAPSHOT_DIR="$(mktemp -d)"
  LOCKFILE_SNAPSHOT="$LOCKFILE_SNAPSHOT_DIR/Cargo.lock"
  cp Cargo.lock "$LOCKFILE_SNAPSHOT"
fi

cargo set-version --workspace --bump "$BUMP_TYPE" --offline >/dev/null

# New version
NEW=$(cargo get workspace.package.version)

if [[ "$PREV" == "$NEW" ]]; then
  echo "Version unchanged ($NEW)"
  exit 0
fi

# Published IcyDB packages share private generated-code and schema contracts.
# Keep every registry-facing intra-workspace edge on the exact release rather
# than allowing Cargo's default caret range to split the family by patch.
for package in "${INTERNAL_WORKSPACE_PACKAGES[@]}"; do
  sed -i -E \
    "s#^(${package}[[:space:]]*=[[:space:]]*\\{[^}]*version[[:space:]]*=[[:space:]]*\\\")[^\\\"]+(\\\"[^}]*\\})#\\1=$NEW\\2#" \
    Cargo.toml
  if ! grep -E "^${package}[[:space:]]*=" Cargo.toml |
    grep -Fq "version = \"=$NEW\""
  then
    echo "Failed to pin $package to exact workspace version =$NEW" >&2
    exit 1
  fi
done

if [[ -n "$LOCKFILE_SNAPSHOT" ]]; then
  cp "$LOCKFILE_SNAPSHOT" Cargo.lock
  escaped_prev="${PREV//./\.}"
  sed -i "s/^version = \"$escaped_prev\"$/version = \"$NEW\"/" Cargo.lock
  cargo metadata --locked --offline --no-deps --format-version 1 >/dev/null
fi

scripts/ci/sync-release-surface-version.sh "$NEW"

if git rev-parse "v$NEW" >/dev/null 2>&1; then
  echo "❌ Tag v$NEW already exists. Aborting." >&2
  exit 1
fi

echo "✅ Bumped: $PREV → $NEW"
echo "Next:"
echo "  git diff"
echo "  make release-stage"
echo "  make release-commit"
echo "  make release-push"
