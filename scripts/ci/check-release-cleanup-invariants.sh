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

for target in patch minor major release-stage release-commit; do
  if target_recipe "$target" | awk -v cleanup="$CLEANUP_SCRIPT" 'index($0, cleanup) { found = 1 } END { exit !found }'; then
    echo "release cleanup must not run from $target" >&2
    exit 1
  fi
done

for target in patch minor major; do
  if ! target_recipe "$target" | awk -v target="$target" '
    /candidate_commit=.*git rev-parse --verify HEAD/ { candidate_line = NR }
    /\$\(MAKE\).*test([;[:space:]]|$)/ { test_line = NR }
    /\$\(MAKE\).*ensure-clean/ && test_line > 0 { clean_line = NR }
    /if .*git rev-parse --verify HEAD.*candidate_commit/ { unchanged_line = NR }
    index($0, "bump-version.sh " target) { bump_line = NR }
    /release-candidate-receipt\.sh record.*candidate_commit/ { receipt_line = NR }
    END {
      exit !(candidate_line > 0 && test_line > candidate_line &&
             clean_line > test_line && unchanged_line > clean_line &&
             bump_line > unchanged_line && receipt_line > bump_line)
    }
  '; then
    echo "$target must test a clean candidate before bumping and recording its exact transition" >&2
    exit 1
  fi
done

if ! target_recipe release-commit | awk '
  /release-candidate-receipt\.sh verify-staged/ { staged_line = NR }
  /git commit -m/ { commit_line = NR }
  /\$\(MAKE\).*ensure-clean/ { clean_line = NR }
  /release-candidate-receipt\.sh verify-commit/ { verified_line = NR }
  /\$\(MAKE\).*test/ { test_line = NR }
  /git tag -a/ { tag_line = NR }
  /record-release-gate-receipt\.sh/ { receipt_line = NR }
  END {
    exit !(staged_line > 0 && commit_line > staged_line &&
           clean_line > commit_line && verified_line > clean_line &&
           test_line == 0 && tag_line > verified_line &&
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

PRE_PUSH="$ROOT/.githooks/pre-push"
if ! awk '
  /verify-release-gate-receipt\.sh/ { verify_line = NR }
  /make -C .* test/ { test_line = NR }
  END { exit !(verify_line > 0 && test_line > verify_line) }
' "$PRE_PUSH"; then
  echo "pre-push must verify an exact release receipt before its ordinary full gate" >&2
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
