#!/usr/bin/env bash
set -euo pipefail

ROOT="${ICYDB_RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)}"
RELEASE_RECEIPT_DIR="${RELEASE_RECEIPT_DIR:-$ROOT/.cache/release-receipts}"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

# shellcheck source=scripts/ci/release-version-transition.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/release-version-transition.sh"

cd "$ROOT"

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <patch|minor|major> <tested-commit>" >&2
  exit 2
fi
BUMP_TYPE="$1"
TESTED_COMMIT="$2"

case "$BUMP_TYPE" in
  patch | minor | major) ;;
  *)
    echo "Unsupported release bump: $BUMP_TYPE" >&2
    exit 2
    ;;
esac

CURRENT_COMMIT="$(git rev-parse --verify HEAD)"
if [[ ! "$TESTED_COMMIT" =~ ^[0-9a-f]{40}$ || "$CURRENT_COMMIT" != "$TESTED_COMMIT" ]]; then
  echo "Current HEAD is not the candidate commit that completed the full gate" >&2
  exit 1
fi
if ! git diff --quiet --ignore-submodules HEAD --; then
  echo "Version bump requires a clean tracked candidate" >&2
  exit 1
fi

if ! cargo set-version --help >/dev/null 2>&1; then
  echo "❌ cargo set-version not available. Install cargo-edit or upgrade Rust." >&2
  exit 1
fi

# Current version (from [workspace.package])
PREV=$(cargo get workspace.package.version)
NEW="$(release_next_version "$BUMP_TYPE" "$PREV")"

if git rev-parse --verify "refs/tags/v$NEW" >/dev/null 2>&1; then
  echo "❌ Tag v$NEW already exists. Aborting before version mutation." >&2
  exit 1
fi

mapfile -d '' -t VERSION_SURFACES < <(
  git ls-files -z -- Cargo.toml Cargo.lock README.md '*/Cargo.toml'
)
if [[ "${#VERSION_SURFACES[@]}" -eq 0 ]]; then
  echo "No tracked release version surfaces found" >&2
  exit 1
fi

ROLLBACK_REQUIRED=0
rollback_release_bump() {
  local status=$?

  trap - EXIT
  if [[ "$status" -ne 0 && "$ROLLBACK_REQUIRED" -eq 1 ]]; then
    if git restore --source="$TESTED_COMMIT" --worktree -- "${VERSION_SURFACES[@]}"; then
      echo "Rolled back failed $BUMP_TYPE release mutation to $TESTED_COMMIT" >&2
    else
      echo "Failed to roll back release version surfaces" >&2
      status=1
    fi
  fi
  exit "$status"
}
trap rollback_release_bump EXIT
ROLLBACK_REQUIRED=1

# Bump
cargo set-version --workspace --bump "$BUMP_TYPE" >/dev/null

ACTUAL_NEW=$(cargo get workspace.package.version)
if [[ "$ACTUAL_NEW" != "$NEW" ]]; then
  echo "Expected cargo to bump $PREV to $NEW; found $ACTUAL_NEW" >&2
  exit 1
fi

[[ -f Cargo.lock ]] && cargo generate-lockfile >/dev/null

"$ROOT/scripts/ci/sync-release-surface-version.sh" "$NEW"
RELEASE_RECEIPT_DIR="$RELEASE_RECEIPT_DIR" \
  "$ROOT/scripts/ci/release-candidate-receipt.sh" record "$BUMP_TYPE" "$TESTED_COMMIT"
ROLLBACK_REQUIRED=0
trap - EXIT

echo "✅ Bumped: $PREV → $NEW"
echo "Next:"
echo "  git diff"
echo "  make release-stage"
echo "  make release-commit"
echo "  make release-push"
