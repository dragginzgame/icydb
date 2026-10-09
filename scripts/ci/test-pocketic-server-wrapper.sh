#!/usr/bin/env bash
set -euo pipefail

# Supervision and evidence tests use a harmless substitute, never PocketIC.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-pocketic-wrapper.XXXXXX")"
control_pid=""
finish() {
  local status=$?
  if [[ -n "$control_pid" ]]; then
    kill "$control_pid" 2>/dev/null || true
    wait "$control_pid" 2>/dev/null || true
  fi
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "PocketIC wrapper fixture failure retained: $fixture" >&2; fi
}
trap finish EXIT
cat > "$fixture/server" <<'SERVER'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" == 1 && "$1" == --version ]]; then
  printf 'pocket-ic-server 16.1.0\n'
  exit 0
fi
port_file=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ttl|--hard-ttl) shift 2 ;;
    --port-file) port_file="$2"; shift 2 ;;
    *) exit 99 ;;
  esac
done
[[ -n "$port_file" ]]
printf '%s\n' "$$" > "$TEST_STATE/server.pid"
for ((line = 1; line <= 4096; line++)); do
  printf 'stdout-%s\n' "$line"
  printf 'stderr-%s\n' "$line" >&2
done
[[ "$TEST_MODE" != startup ]] || exit 9
printf '12345\n' > "$port_file"
# One owned process with no children or network sockets.
exec sleep 60
SERVER
cat > "$fixture/child-command" <<'CHILD'
#!/usr/bin/env bash
set -euo pipefail
[[ "$IC_TESTKIT_POCKET_IC_URL" == http://127.0.0.1:12345/ ]]
printf 'called\n' > "$TEST_STATE/child-called"
printf '%s\n' "$$" > "$TEST_STATE/child.pid"
case "$TEST_MODE" in
  child) exit 7 ;;
  term) kill -TERM "$(cat "$TEST_STATE/wrapper.pid")"; exec sleep 60 ;;
  int) kill -INT "$(cat "$TEST_STATE/wrapper.pid")"; exec sleep 60 ;;
  success) exit 0 ;;
  *) exit 99 ;;
esac
CHILD
chmod +x "$fixture/server" "$fixture/child-command"
for ((line = 1; line <= 4096; line++)); do
  printf 'stdout-%s\n' "$line" >> "$fixture/expected-stdout"
  printf 'stderr-%s\n' "$line" >> "$fixture/expected-stderr"
done
printf 'unrelated evidence\n' > "$fixture/unrelated"
cp "$fixture/unrelated" "$fixture/unrelated-before"
sleep 60 &
control_pid="$!"
count=0
shopt -s nullglob
for mode in startup child term int success; do
  state="$fixture/$mode"
  mkdir -p "$state/scratch"
  invocation=(bash "$ROOT/scripts/ci/run-with-pocketic-server.sh" "$fixture/child-command")
  if [[ "$mode" == success ]]; then
    # Qualify Make's managed-lane handoff without executing the Tier B suites.
    invocation=(make --silent --no-print-directory -C "$ROOT" ci-sql-tier-b
      "POCKET_IC_BIN=$fixture/server" "VALIDATION_RUNNER=$fixture/child-command")
  fi
  status=0
  TEST_STATE="$state" TEST_MODE="$mode" TMPDIR="$state/scratch" \
    POCKET_IC_BIN="$fixture/server" bash -c \
    'printf "%s\n" "$$" > "$TEST_STATE/wrapper.pid"; exec "$@"' \
    wrapper "${invocation[@]}" \
    > "$state/output" 2>&1 || status=$?
  case "$mode" in
    startup) [[ "$status" == 1 && ! -e "$state/child-called" ]] ;;
    child) [[ "$status" == 7 && -f "$state/child-called" ]] ;;
    term) [[ "$status" == 143 && -f "$state/child-called" ]] ;;
    int) [[ "$status" == 130 && -f "$state/child-called" ]] ;;
    success) [[ "$status" == 0 && -f "$state/child-called" ]] ;;
  esac
  server_pid="$(cat "$state/server.pid")"
  if kill -0 "$server_pid" 2>/dev/null; then
    echo 'wrapper did not terminate its owned server' >&2; exit 1
  fi
  if [[ -f "$state/child.pid" ]] && kill -0 "$(cat "$state/child.pid")" 2>/dev/null; then
    echo 'wrapper did not terminate its owned command' >&2; exit 1
  fi
  kill -0 "$control_pid"
  cmp "$fixture/unrelated-before" "$fixture/unrelated"
  retained_count=0
  for retained in "$state/scratch/"icydb-pocketic-server.*; do
    retained_count=$((retained_count + 1))
    cmp "$fixture/expected-stdout" "$retained/stdout"
    cmp "$fixture/expected-stderr" "$retained/stderr"
    # Assert the usable location is reported, without freezing diagnostic prose.
    rg -F -- "$retained" "$state/output" >/dev/null
  done
  if [[ "$mode" == success ]]; then [[ "$retained_count" == 0 ]]
  else [[ "$retained_count" == 1 ]]; fi
  count=$((count + 1))
done
printf '[OK] PocketIC wrapper fixtures passed: %s cases (no network)\n' "$count"
