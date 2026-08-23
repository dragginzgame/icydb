#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVER_TTL_SECONDS=900
SERVER_PID=""

if [[ "$#" -eq 0 ]]; then
  echo "usage: run-with-pocketic-server.sh <command> [args...]" >&2
  exit 2
fi
if [[ -z "${POCKET_IC_BIN:-}" || ! -x "$POCKET_IC_BIN" ]]; then
  echo "POCKET_IC_BIN must name the executable pinned PocketIC server" >&2
  exit 1
fi

scratch_root="${TMPDIR:-$ROOT/.cache}"
mkdir -p "$scratch_root"
scratch="$(mktemp -d "$scratch_root/icydb-pocketic-server.XXXXXX")"
port_file="$scratch/port"
stdout_file="$scratch/stdout"
stderr_file="$scratch/stderr"

# Invoked from the EXIT-trap cleanup path.
# shellcheck disable=SC2317,SC2329
report_server_output() {
  echo "==> shared PocketIC stderr (last 40 lines)" >&2
  tail -40 "$stderr_file" >&2 || true
  echo "==> shared PocketIC stdout (last 40 lines)" >&2
  tail -40 "$stdout_file" >&2 || true
}

report_server_resources() {
  if [[ -z "$SERVER_PID" || ! -r "/proc/$SERVER_PID/status" ]]; then
    echo "==> shared PocketIC resources: unavailable"
    return
  fi

  local key value rss="unknown" high_water="unknown" threads="unknown"
  while IFS=: read -r key value; do
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    case "$key" in
      Threads) threads="$value" ;;
      VmHWM) high_water="$value" ;;
      VmRSS) rss="$value" ;;
    esac
  done <"/proc/$SERVER_PID/status"
  echo "==> shared PocketIC resources: rss=$rss high_water=$high_water threads=$threads"
}

# Registered as the process EXIT trap below.
# shellcheck disable=SC2317,SC2329
cleanup() {
  local status="$?"
  trap - EXIT INT TERM
  if [[ -n "$SERVER_PID" ]]; then
    if kill -0 "$SERVER_PID" 2>/dev/null; then
      kill -KILL "$SERVER_PID" 2>/dev/null || true
    fi
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "$status" -ne 0 ]]; then
    report_server_output
  fi
  rm -f "$port_file" "$stdout_file" "$stderr_file"
  rmdir "$scratch" 2>/dev/null || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$POCKET_IC_BIN" \
  --ttl "$SERVER_TTL_SECONDS" \
  --hard-ttl "$SERVER_TTL_SECONDS" \
  --port-file "$port_file" \
  >"$stdout_file" 2>"$stderr_file" &
SERVER_PID="$!"

server_port=""
for ((attempt = 0; attempt < 150; attempt++)); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" || true
    echo "PocketIC server exited before publishing its port" >&2
    exit 1
  fi
  if [[ -f "$port_file" ]]; then
    IFS= read -r server_port <"$port_file" || true
  fi
  if [[ "$server_port" =~ ^[0-9]+$ ]] &&
     ((server_port >= 1 && server_port <= 65535)); then
    break
  fi
  sleep 0.2
done

if [[ ! "$server_port" =~ ^[0-9]+$ ]] ||
   ((server_port < 1 || server_port > 65535)); then
  echo "PocketIC server did not publish a valid port within 30 seconds" >&2
  exit 1
fi

export ICYDB_POCKET_IC_SERVER_URL="http://127.0.0.1:$server_port/"
echo "==> shared PocketIC server ready: $ICYDB_POCKET_IC_SERVER_URL"

started_at="$SECONDS"
status=0
"$@" || status="$?"
echo "==> shared PocketIC command elapsed=$((SECONDS - started_at))s status=$status"
report_server_resources
exit "$status"
