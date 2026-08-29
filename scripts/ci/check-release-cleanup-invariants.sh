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
  /\$\(VALIDATION_RUNNER\)/ { runner_line = NR }
  $1 == "fmt-check" { fmt_line = NR }
  $1 == "lint-workflows" { workflow_line = NR }
  $1 == "shellcheck" { shell_line = NR }
  $1 == "check-invariants" { invariants_line = NR }
  $1 == "check-feature-matrix" { features_line = NR }
  $1 == "check" { check_line = NR }
  $1 == "clippy" { clippy_line = NR }
  $1 == "test" { test_line = NR }
  END {
    exit !(runner_line > 0 && fmt_line > runner_line &&
           workflow_line > fmt_line && shell_line > workflow_line &&
           invariants_line > shell_line &&
           clippy_line > invariants_line && features_line > clippy_line &&
           check_line > features_line && test_line > check_line)
  }
'; then
  echo "validate must run static checks, clippy, feature checks, check, and tests in order" >&2
  exit 1
fi

if ! target_recipe clippy | awk '
  /cargo clippy --workspace --all-targets/ { workspace_line = NR }
  /cargo clippy -p icydb-core --no-default-features --features sql/ { sql_line = NR }
  /cargo clippy -p icydb-core --no-default-features --features diagnostics/ {
    diagnostics_line = NR
  }
  END {
    exit !(workspace_line > 0 && sql_line > workspace_line &&
           diagnostics_line > sql_line)
  }
'; then
  echo "clippy must lint the complete workspace and test surface before feature-only lanes" >&2
  exit 1
fi

if ! target_recipe ci-core | awk '
  $1 == "_ci-core-sql-clippy" { sql_line = NR }
  $1 == "_ci-core-diagnostics-clippy" { diagnostics_line = NR }
  $1 == "_ci-core-no-default-test" { test_line = NR }
  END {
    exit !(sql_line > 0 && diagnostics_line > sql_line &&
           test_line > diagnostics_line)
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

for target in patch minor major release-stage release-commit; do
  if target_recipe "$target" | awk -v cleanup="$CLEANUP_SCRIPT" 'index($0, cleanup) { found = 1 } END { exit !found }'; then
    echo "release cleanup must not run from $target" >&2
    exit 1
  fi
done

if ! target_recipe clean | grep -Fq "$CARGO_CLEAN_RECIPE"; then
  echo "clean must own repo-local Cargo build-cache deletion" >&2
  exit 1
fi

if ! target_recipe release-clean | awk -v cleanup="$CLEANUP_SCRIPT" '
  /\$\(MAKE\).* clean([[:space:]]|$)/ { cargo_clean_line = NR }
  index($0, cleanup) { transient_cleanup_line = NR }
  END {
    exit !(cargo_clean_line > 0 && transient_cleanup_line > cargo_clean_line)
  }
'; then
  echo "release-clean must delete the Cargo build cache before transient release state" >&2
  exit 1
fi

if grep -Eq 'cargo[[:space:]]+clean|TARGET_DIR|CARGO_HOME' "$ROOT/$CLEANUP_SCRIPT"; then
  echo "automatic release cleanup must preserve Cargo build state" >&2
  exit 1
fi

for target in patch minor major; do
  if ! target_recipe "$target" | awk -v target="$target" '
    /candidate_commit=.*git rev-parse --verify HEAD/ { candidate_line = NR }
    /release-candidate-receipt\.sh verify-tested-tree.*candidate_commit/ {
      verified_count += 1
      if (verified_count == 1) first_verified_line = NR
      second_verified_line = NR
    }
    /\$\(MAKE\).*validate([;[:space:]]|$)/ { validate_line = NR }
    /\$\(MAKE\).*ensure-clean/ { clean_line = NR }
    index($0, "bump-version.sh " target) { bump_line = NR }
    /release-candidate-receipt\.sh record.*candidate_commit/ { receipt_line = NR }
    END {
      exit !(candidate_line > 0 && first_verified_line > candidate_line &&
             validate_line > first_verified_line &&
             second_verified_line > validate_line && verified_count == 2 &&
             clean_line == 0 && bump_line > second_verified_line &&
             receipt_line > bump_line)
    }
  '; then
    echo "$target must reject dirty candidates before validation, reverify afterward, then bump and record the transition" >&2
    exit 1
  fi
done

if [[ -e "$ROOT/scripts/ci/release-version-transition.sh" ]] ||
   grep -Eq 'rollback_release_bump|Rolled back failed|git restore --source=.*TESTED_COMMIT' \
     "$MAKEFILE" \
     "$ROOT/scripts/ci/bump-version.sh" \
     "$ROOT/scripts/ci/release-candidate-receipt.sh"; then
  echo "release tooling must not restore or roll back a failed version mutation automatically" >&2
  exit 1
fi

if ! grep -Eq 'cargo set-version .*--offline' "$ROOT/scripts/ci/bump-version.sh" ||
   ! grep -Fq 'cp "$LOCKFILE_SNAPSHOT" Cargo.lock' "$ROOT/scripts/ci/bump-version.sh" ||
   ! grep -Eq 'sed -i .*version = ' "$ROOT/scripts/ci/bump-version.sh" ||
   ! grep -Eq 'cargo metadata --locked --offline --no-deps' "$ROOT/scripts/ci/bump-version.sh" ||
   grep -Eq 'cargo (generate-lockfile|update)' "$ROOT/scripts/ci/bump-version.sh"; then
  echo "release version mutation must preserve the tested lockfile dependency graph offline" >&2
  exit 1
fi

if ! target_recipe release-stage | grep -Fq "CHANGELOG.md 'docs/changelog/*.md'"; then
  echo "release staging must include permitted root and detailed changelog edits" >&2
  exit 1
fi

if ! target_recipe release-commit | awk '
  /release-candidate-receipt\.sh verify-staged/ { staged_line = NR }
  /git commit -m/ { commit_line = NR }
  /release-candidate-receipt\.sh verify-commit/ { verified_line = NR }
  /\$\(MAKE\).*ensure-clean/ { clean_line = NR }
  /\$\(MAKE\).*validate/ { validate_line = NR }
  /git tag -a/ { tag_line = NR }
  /record-release-gate-receipt\.sh/ { receipt_line = NR }
  END {
    exit !(staged_line > 0 && commit_line > staged_line &&
           verified_line > commit_line && clean_line == 0 && validate_line == 0 &&
           tag_line > verified_line &&
           receipt_line > tag_line)
  }
'; then
  echo "release-commit must verify the staged transition, commit, reverify, tag, and record its receipt without retesting" >&2
  exit 1
fi

for target in release-patch release-minor release-major; do
  bump_target="${target#release-}"
  if ! grep -Fxq "$target:" "$MAKEFILE" ||
     ! target_recipe "$target" | awk -v bump_target="$bump_target" '
       $0 ~ ("\\$\\(MAKE\\).* " bump_target "([[:space:]]|$)") { bump_line = NR }
       /\$\(MAKE\).* release-stage([[:space:]]|$)/ { stage_line = NR }
       /\$\(MAKE\).* release-commit([[:space:]]|$)/ { commit_line = NR }
       /\$\(MAKE\).* release-push([[:space:]]|$)/ { push_line = NR }
       END {
         exit !(bump_line > 0 && stage_line > bump_line &&
                commit_line > stage_line && push_line > commit_line)
       }
     '; then
    echo "$target must run bump, stage, commit, and push sequentially" >&2
    exit 1
  fi
done

if [[ "$(grep -c 'record-release-gate-receipt\.sh' "$MAKEFILE")" -ne 1 ]]; then
  echo "the Makefile must record the release-gate receipt exactly once" >&2
  exit 1
fi

if ! target_recipe release-push | awk -v cleanup="$CLEANUP_SCRIPT" '
  /git push --no-follow-tags --atomic origin/ { push_line = NR }
  /HEAD:refs\/heads\/\$\$branch/ { branch_ref_line = NR }
  /refs\/tags\/v\$\$version:refs\/tags\/v\$\$version/ { tag_ref_line = NR }
  index($0, cleanup) { cleanup_line = NR }
  END {
    exit !(push_line > 0 && branch_ref_line > push_line &&
           tag_ref_line > branch_ref_line && cleanup_line > tag_ref_line)
  }
'; then
  echo "release-push must atomically push only the current branch and exact tag before cleanup" >&2
  exit 1
fi

if target_recipe release-push | grep -Eq -- '--follow-tags|--tags([[:space:]]|$)'; then
  echo "release-push must not publish historical annotated tags implicitly" >&2
  exit 1
fi

if target_recipe release-push | grep -Eq 'cargo[[:space:]]+clean|\$\(MAKE\).* clean([[:space:]]|$)'; then
  echo "release-push must preserve the validated Cargo build cache" >&2
  exit 1
fi

automatic_cleanup_calls="$(
  awk -v cleanup="$CLEANUP_SCRIPT" '
    /^\t/ && index($0, cleanup) { count += 1 }
    END { print count + 0 }
  ' "$MAKEFILE"
)"
if [[ "$automatic_cleanup_calls" -ne 2 ]]; then
  echo "expected cleanup only in release-clean and release-push; found $automatic_cleanup_calls calls" >&2
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
  echo "automatic release cleanup removed the Cargo build cache" >&2
  exit 1
fi
if [[ ! -f "$CLEANUP_FIXTURE_ROOT/.cache/unrelated-cache-sentinel" ]]; then
  echo "automatic release cleanup removed unrelated cache state" >&2
  exit 1
fi
for removed in \
  "$CLEANUP_FIXTURE_ROOT/.cache/release-tmp/release-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/icydb-sqlite-comparison/sqlite-sentinel" \
  "$CLEANUP_FIXTURE_ROOT/.cache/pocket_ic_fixture.port"; do
  if [[ -e "$removed" ]]; then
    echo "automatic release cleanup retained transient state: $removed" >&2
    exit 1
  fi
done
echo "transient release cleanup behavior passed"

"$ROOT/scripts/ci/test-release-candidate-receipt.sh"
"$ROOT/scripts/ci/test-delete-github-tags-up-to.sh"

echo "release workflow and cleanup invariants passed"
