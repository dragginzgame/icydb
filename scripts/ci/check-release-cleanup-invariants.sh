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

echo "release cleanup invariants passed"
