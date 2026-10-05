#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAKEFILE="$ROOT/Makefile"
CLEANUP_SCRIPT="scripts/ci/cleanup-release-workspace.sh"
CARGO_CLEAN_RECIPE="\$(CARGO_WORK_ENV) cargo clean"
CLEANUP_TEST_ROOT="$(mktemp -d)"

cleanup() {
  find "$CLEANUP_TEST_ROOT" -depth -delete
}
trap cleanup EXIT

target_recipe() {
  local target="$1"
  awk -v target="$target" '
    index($0, target ":") == 1 {
      in_target = 1
      next
    }
    in_target && /^[^[:space:]#][^=]*:/ {
      exit
    }
    in_target {
      print
    }
  ' "$MAKEFILE"
}

if ! target_recipe validate | awk '
  /\$\(VALIDATION_RUNNER\).*--fail-fast/ { preflight_runner_line = NR }
  /\$\(VALIDATION_RUNNER\)/ && $0 !~ /--fail-fast/ { long_runner_line = NR }
  $1 == "fmt-check" { fmt_line = NR }
  $1 == "lint-workflows" { workflow_line = NR }
  $1 == "shellcheck" { shell_line = NR }
  $1 == "check-invariants" { invariants_line = NR }
  $1 == "check-feature-matrix" { features_line = NR }
  $1 == "check" { check_line = NR }
  $1 == "clippy" { clippy_line = NR }
  $1 == "test" { test_line = NR }
  END {
    exit !(preflight_runner_line > 0 && fmt_line > preflight_runner_line &&
           workflow_line > fmt_line && shell_line > workflow_line &&
           invariants_line > shell_line && check_line > invariants_line &&
           clippy_line > check_line && long_runner_line > clippy_line &&
           features_line > long_runner_line && test_line > features_line)
  }
'; then
  echo "validate must fail-fast through clippy before accumulating later long-target failures" >&2
  exit 1
fi

if ! target_recipe clippy | awk '
  /cargo clippy --workspace --all-targets/ { workspace_line = NR }
  /cargo clippy -p icydb-core --no-default-features --features sql/ { sql_line = NR }
  END {
    exit !(workspace_line > 0 && sql_line > workspace_line)
  }
'; then
  echo "clippy must lint the complete workspace and test surface before feature-only lanes" >&2
  exit 1
fi

if ! target_recipe ci-core | awk '
  $1 == "_ci-core-sql-clippy" { sql_line = NR }
  $1 == "_ci-core-no-default-test" { test_line = NR }
  END {
    exit !(sql_line > 0 && test_line > sql_line)
  }
'; then
  echo "ci-core must complete clippy lanes before executable tests" >&2
  exit 1
fi

if ! target_recipe ci-workspace | awk '
  $1 == "_ci-workspace-clippy" { workspace_line = NR }
  $1 == "_ci-workspace-integration-clippy" { integration_line = NR }
  $1 == "_ci-workspace-tests" { test_line = NR }
  END {
    exit !(workspace_line > 0 && integration_line > workspace_line &&
           test_line > integration_line)
  }
'; then
  echo "ci-workspace must complete clippy lanes before executable tests" >&2
  exit 1
fi

for target in release-patch release-minor release-major release-resume; do
  if target_recipe "$target" | awk -v cleanup="$CLEANUP_SCRIPT" 'index($0, cleanup) { found = 1 } END { exit !found }'; then
    echo "release cleanup must not run from $target" >&2
    exit 1
  fi
done

if ! target_recipe clean | grep -Fq "$CARGO_CLEAN_RECIPE"; then
  echo "clean must own repo-local Cargo build-cache deletion" >&2
  exit 1
fi

if ! target_recipe release-clean | grep -Fq "$CLEANUP_SCRIPT"; then
  echo "release-clean must remove transient release state" >&2
  exit 1
fi

if grep -Eq 'cargo[[:space:]]+clean|TARGET_DIR|CARGO_HOME' "$ROOT/$CLEANUP_SCRIPT"; then
  echo "release workspace cleanup must preserve Cargo build state" >&2
  exit 1
fi

for target in \
  release-clean release-patch release-minor release-major release-resume \
  release-preflight release-verify release-prepare-version release-prepared-check \
  release-files release-commit-check release-committed-check release-tagged-check release-push-check \
  package publish all; do
  if target_recipe "$target" | grep -Eq \
    'cargo[[:space:]]+clean|\$\(MAKE\).* clean([;[:space:]]|$)'; then
    echo "$target must preserve Cargo build state; cleanup is manual" >&2
    exit 1
  fi
done

automatic_cleanup_calls="$(
  awk -v cleanup="$CLEANUP_SCRIPT" '
    /^\t/ && index($0, cleanup) { count += 1 }
    END { print count + 0 }
  ' "$MAKEFILE"
)"
if [[ "$automatic_cleanup_calls" -ne 1 ]]; then
  echo "expected cleanup only in release-clean; found $automatic_cleanup_calls calls" >&2
  exit 1
fi

CLEANUP_FIXTURE_ROOT="$CLEANUP_TEST_ROOT/repository"
CLEANUP_FIXTURE_SCRIPT="$CLEANUP_FIXTURE_ROOT/$CLEANUP_SCRIPT"
mkdir -p \
  "$(dirname "$CLEANUP_FIXTURE_SCRIPT")" \
  "$CLEANUP_FIXTURE_ROOT/target/icydb" \
  "$CLEANUP_FIXTURE_ROOT/.cache/release-tmp" \
  "$CLEANUP_FIXTURE_ROOT/.cache/icydb-sqlite-comparison"
cp "$ROOT/$CLEANUP_SCRIPT" "$CLEANUP_FIXTURE_SCRIPT"
touch \
  "$CLEANUP_FIXTURE_ROOT/target/icydb/build-cache-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/release-tmp/release-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/icydb-sqlite-comparison/sqlite-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/pocket_ic_fixture.port" \
  "$CLEANUP_FIXTURE_ROOT/.cache/unrelated-cache-sentinel"
bash "$CLEANUP_FIXTURE_SCRIPT"

if [[ ! -f "$CLEANUP_FIXTURE_ROOT/target/icydb/build-cache-sentinel" ]]; then
  echo "explicit release cleanup removed the Cargo build cache" >&2
  exit 1
fi
if [[ ! -f "$CLEANUP_FIXTURE_ROOT/.cache/unrelated-cache-sentinel" ]]; then
  echo "explicit release cleanup removed unrelated cache state" >&2
  exit 1
fi
for removed in \
  "$CLEANUP_FIXTURE_ROOT/.cache/release-tmp/release-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/icydb-sqlite-comparison/sqlite-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/pocket_ic_fixture.port"; do
  if [[ -e "$removed" ]]; then
    echo "explicit release cleanup retained transient state: $removed" >&2
    exit 1
  fi
done
echo "transient release cleanup behavior passed"

echo "validation ordering and explicit release cleanup invariants passed"
