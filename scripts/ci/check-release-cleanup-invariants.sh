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
  if target_recipe "$target" | awk '/\$\(MAKE\).*(test|clippy)([[:space:]]|$)/ { found = 1 } END { exit !found }'; then
    echo "$target must not run the release gate before the versioned commit exists" >&2
    exit 1
  fi
done

if ! target_recipe release-commit | awk '
  /git commit -m/ { commit_line = NR }
  /\$\(MAKE\).*ensure-clean/ { clean_line = NR }
  /\$\(MAKE\).*test/ { test_line = NR }
  /git tag -a/ { tag_line = NR }
  /record-release-gate-receipt\.sh/ { receipt_line = NR }
  END {
    exit !(commit_line > 0 && clean_line > commit_line &&
           test_line > clean_line &&
           tag_line > test_line && receipt_line > tag_line)
  }
'; then
  echo "release-commit must commit, prove clean state, test, tag, and record its receipt in that order" >&2
  exit 1
fi

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

echo "release workflow and cleanup invariants passed"
