#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

BUMP_TYPE=${1:-patch}

if ! cargo set-version --help >/dev/null 2>&1; then
  echo "❌ cargo set-version not available. Install cargo-edit or upgrade Rust." >&2
  exit 1
fi

# Current version (from [workspace.package])
PREV=$(cargo metadata --no-deps --format-version=1 \
  | jq -r '.workspace_metadata.workspace.package.version // .packages[0].version')

# Bump
cargo set-version --workspace --bump "$BUMP_TYPE" >/dev/null

# New version
NEW=$(cargo metadata --no-deps --format-version=1 \
  | jq -r '.workspace_metadata.workspace.package.version // .packages[0].version')

if [[ "$PREV" == "$NEW" ]]; then
  echo "Version unchanged ($NEW)"
  exit 0
fi

[[ -f Cargo.lock ]] && cargo generate-lockfile >/dev/null

git add Cargo.toml Cargo.lock $(git ls-files -m -- */Cargo.toml || true)

if git rev-parse "v$NEW" >/dev/null 2>&1; then
  echo "❌ Tag v$NEW already exists. Aborting." >&2
  exit 1
fi

git commit -m "Release $NEW"
git tag -a "v$NEW" -m "Release $NEW"
git push --follow-tags

echo "✅ Bumped: $PREV → $NEW"
