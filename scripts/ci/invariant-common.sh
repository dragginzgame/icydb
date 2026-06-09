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

run_rg() {
  local pattern=$1
  shift
  rg -n --no-heading --color=never "$pattern" "$@" "${COMMON_GLOBS[@]}" || true
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
