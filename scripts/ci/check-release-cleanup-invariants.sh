#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAKEFILE="$ROOT/Makefile"
CLEANUP_SCRIPT="scripts/ci/cleanup-release-workspace.sh"

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
  /\$\(MAKE\).* fmt-check([[:space:]]|$)/ { fmt_line = NR }
  /\$\(MAKE\).* check-invariants([[:space:]]|$)/ { invariants_line = NR }
  /\$\(MAKE\).* check-feature-matrix([[:space:]]|$)/ { features_line = NR }
  /\$\(MAKE\).* check([[:space:]]|$)/ { check_line = NR }
  /\$\(MAKE\).* clippy([[:space:]]|$)/ { clippy_line = NR }
  /\$\(MAKE\).* test([[:space:]]|$)/ { test_line = NR }
  END {
    exit !(fmt_line > 0 && invariants_line > fmt_line &&
           features_line > invariants_line && check_line > features_line &&
           clippy_line > check_line && test_line > clippy_line)
  }
'; then
  echo "validate must compose formatting, invariants, feature checks, check, clippy, and tests in order" >&2
  exit 1
fi

for target in patch minor major release-stage release-commit; do
  if target_recipe "$target" | awk -v cleanup="$CLEANUP_SCRIPT" 'index($0, cleanup) { found = 1 } END { exit !found }'; then
    echo "release cleanup must not run from $target" >&2
    exit 1
  fi
done

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
   ! grep -Eq 'cargo generate-lockfile --offline' "$ROOT/scripts/ci/bump-version.sh"; then
  echo "release version mutation must preserve the tested dependency graph offline" >&2
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
  /git push --follow-tags/ { push_line = NR }
  index($0, cleanup) { cleanup_line = NR }
  END {
    exit !(push_line > 0 && cleanup_line > push_line)
  }
'; then
  echo "release-push must clean only after git push --follow-tags succeeds" >&2
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

"$ROOT/scripts/ci/test-release-candidate-receipt.sh"

echo "release workflow and cleanup invariants passed"
