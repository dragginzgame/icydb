#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

cd "$ROOT"

BUMP_TYPE=${1:-patch}

if ! cargo set-version --help >/dev/null 2>&1; then
  echo "❌ cargo set-version not available. Install cargo-edit or upgrade Rust." >&2
  exit 1
fi

# Current version (from [workspace.package])
PREV=$(cargo get workspace.package.version)

# Keep the tested dependency graph fixed while changing workspace versions.
# Registry updates belong in an ordinary reviewed code commit, not in the
# mechanical release transition after the full gate has passed.
cargo set-version --workspace --bump "$BUMP_TYPE" --offline >/dev/null

# New version
NEW=$(cargo get workspace.package.version)

if [[ "$PREV" == "$NEW" ]]; then
  echo "Version unchanged ($NEW)"
  exit 0
fi

[[ -f Cargo.lock ]] && cargo generate-lockfile --offline >/dev/null

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
