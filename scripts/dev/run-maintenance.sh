#!/usr/bin/env bash
# Shared companions: tasks/README.md tasks/maintenance-prompt.md
set -euo pipefail
umask 077

# A serial scheduler owns concurrency; this helper never installs a schedule.
usage() { echo 'usage: run-maintenance.sh <--check|--run|--if-due> <projects-root> <state-root>' >&2; }
[[ $# == 3 ]] || { usage; exit 2; }
mode="$1"
case "$mode" in --check|--run|--if-due) ;; *) usage; exit 2 ;; esac
projects_root="$2"
state_root="$3"
[[ "$projects_root" == /* && -d "$projects_root" && "$state_root" == /* ]] || {
    echo 'projects-root must be an existing absolute directory; state-root must be absolute' >&2; exit 2;
}
ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
[[ -f "$ROOT/tasks/README.md" && -f "$ROOT/tasks/maintenance-prompt.md" ]] || {
    echo 'maintenance catalog or prompt is missing' >&2; exit 1;
}
codex_bin="${CODEX_BIN:-codex}"

if [[ "$mode" == --check ]]; then
    "$codex_bin" --version
    cli_help="$("$codex_bin" exec --help)"
    [[ "$cli_help" == *--approve-for-me* ]] || {
        echo 'this runner requires Codex exec automatic approval review' >&2; exit 1;
    }
    "$codex_bin" login status
    echo 'Maintenance prerequisites checked; no agent or schedule started.'
    exit 0
fi

mkdir -p "$state_root/runs"
now="$(date -u +%s)"
stamp="$state_root/last-attempt"
if [[ -e "$stamp" || -L "$stamp" ]]; then
    [[ -f "$stamp" && ! -L "$stamp" ]] || { echo 'last-attempt must be a regular file' >&2; exit 1; }
    last="$(cat "$stamp")"
    [[ "$last" =~ ^[0-9]{1,10}$ ]] || { echo 'invalid last-attempt timestamp' >&2; exit 1; }
    (( 10#$last <= now )) || { echo 'last-attempt is in the future; check the host clock' >&2; exit 1; }
    # Compare calendar days so timer jitter or daylight-saving shifts cannot
    # accidentally defer the third morning to the fourth.
    if [[ "$mode" == --if-due ]] && (( now / 86400 - 10#$last / 86400 < 3 )); then
        echo 'Maintenance is not due (fewer than three UTC dates since the last attempt).'
        exit 0
    fi
fi

run_dir="$(mktemp -d "$state_root/runs/$(date -u +%Y%m%dT%H%M%SZ).XXXXXX")"
printf 'Maintenance evidence: %s\n' "$run_dir"
trap 'printf "%s\n" "$?" > "$run_dir/exit-status"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$now" > "$run_dir/started-at"
{
    printf 'Shared Tooling checkout: %s\nProjects directory: %s\nRun directory: %s\n' "$ROOT" "$projects_root" "$run_dir"
    printf 'Prior reports: %s/runs (read only for comparison).\n\n' "$state_root"
    cat "$ROOT/tasks/maintenance-prompt.md"
} > "$run_dir/prompt.txt"
# Record dispatch before starting an agent: a failed/uncertain attempt must not
# cause an unattended retry burst. A manual --run remains available to the owner.
cp "$run_dir/started-at" "$run_dir/next-attempt"
mv -f "$run_dir/next-attempt" "$stamp"
{
    git -C "$ROOT" rev-parse HEAD
    git -C "$ROOT" status --short
    "$codex_bin" --version
} > "$run_dir/inputs.txt" 2>&1
"$codex_bin" exec --approve-for-me --cd "$ROOT" --json \
    --output-last-message "$run_dir/report.md" - \
    < "$run_dir/prompt.txt" > "$run_dir/events.jsonl" 2> "$run_dir/stderr.log"
[[ -s "$run_dir/report.md" ]] || { echo 'Codex returned no maintenance report' >&2; exit 1; }
echo "Maintenance report: $run_dir/report.md"
