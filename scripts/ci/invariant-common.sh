# shellcheck shell=bash

COMMON_GLOBS=(
  --glob '!**/tests/**'
  --glob '!**/tests.rs'
  --glob '!**/*_tests.rs'
  --glob '!**/test_*.rs'
)

require_rg() {
  local check_name="$1"

  if ! command -v rg >/dev/null 2>&1; then
    echo "[ERROR] ripgrep (rg) is required for $check_name." >&2
    echo "[ERROR] Install it with your system package manager, then run 'make update-dev' to verify local prerequisites." >&2
    exit 1
  fi
}

# Matches and no matches are both successful searches. Discovery/read/regex
# failures terminate even a grouped pipeline inside command substitution.
rg_checked() {
  local scan_status=0
  rg "$@" || scan_status=$?
  case "$scan_status" in
    0|1) return 0 ;;
    *)
      echo "[ERROR] Invariant search failed with status $scan_status." >&2
      exit "$scan_status"
      ;;
  esac
}

run_rg() {
  local pattern=$1
  shift
  rg_checked -n --with-filename --no-heading --color=never "$pattern" "$@" "${COMMON_GLOBS[@]}"
}

strip_comment_only() {
  awk -F: '{
    code=$0
    sub(/^[^:]+:[0-9]+:/, "", code)
    if (code ~ /^[[:space:]]*\/\//) {
      next
    }
    print $0
  }'
}
