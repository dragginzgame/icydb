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
# Check literal inputs before normalization, and resolve existing ancestors
# without creating a not-yet-initialized state root. Aliases must not conceal
# line breaks that would split prompt fields or evidence locations.
perl -MCwd=abs_path -MFile::Basename=dirname -e '
    for my $path (@ARGV) {
        $path !~ /[\r\n]/ or die "maintenance path contains CR/LF\n";
        while (!-e $path && !-l $path) {
            my $parent=dirname($path);
            $parent ne $path or die "cannot resolve maintenance path\n";
            $path=$parent;
        }
        my $resolved=abs_path($path);
        defined $resolved or die "cannot resolve maintenance path\n";
        $resolved !~ /[\r\n]/ or die "resolved maintenance path contains CR/LF\n";
    }
' "$projects_root" "$state_root" "$0" "$PWD"
ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
projects_root="$(cd -P "$projects_root" && pwd)"
[[ "$projects_root" != / && "$projects_root" != "$HOME" ]] || {
    echo 'projects-root must select source checkouts, not a home or filesystem root' >&2; exit 2;
}
[[ -f "$ROOT/tasks/README.md" && -f "$ROOT/tasks/maintenance-prompt.md" ]] || {
    echo 'maintenance catalog or prompt is missing' >&2; exit 1;
}
codex_bin="${CODEX_BIN:-codex}"

# Do not inherit publish tokens, SSH agents, credential helpers, shell startup
# hooks or the user's connector configuration into unattended execution.
# The harness retains its own login; model-generated commands get only the
# explicitly selected read surface below, never the harness credential store.
codex_clean() {
    env -i HOME="$HOME" PATH="$PATH" CODEX_HOME="${CODEX_HOME:-$HOME/.codex}" \
        LANG=C.UTF-8 "$codex_bin" "$@"
}

now="$(date -u +%s)"
stamp="$state_root/last-attempt"
if [[ "$mode" != --check && ( -e "$stamp" || -L "$stamp" ) ]]; then
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

if [[ "$mode" == --check ]]; then
    run_dir="$(mktemp -d "${TMPDIR:-/tmp}/maintenance-check.XXXXXX")"
else
    mkdir -p "$state_root/runs"
    run_dir="$(mktemp -d "$state_root/runs/$(date -u +%Y%m%dT%H%M%SZ).XXXXXX")"
fi
printf 'Maintenance evidence: %s\n' "$run_dir"
trap 'printf "%s\n" "$?" > "$run_dir/exit-status"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if git -C "$run_dir" rev-parse --git-dir >/dev/null 2>&1; then
    echo 'maintenance state must be outside Git repositories to exclude project configuration' >&2; exit 1;
fi
printf '%s\n' "$now" > "$run_dir/started-at"
{
    git -C "$ROOT" rev-parse HEAD
    git -C "$ROOT" status --short
    codex_clean --version
} > "$run_dir/inputs.txt" 2>&1

# Work from the evidence directory, outside project config discovery. A named
# profile allows only runtime prerequisites, selected source reads and evidence
# writes. Never combine it with --sandbox (which overrides named profiles).
configuration=(
    -c 'approval_policy="never"'
    -c 'default_permissions="maintenance"'
    -c 'permissions.maintenance.network.enabled=false'
    -c 'allow_login_shell=false'
    -c 'shell_environment_policy.inherit="core"'
    -c 'shell_environment_policy.filters={"PATH"="include","HOME"="include","LANG"="include"}'
    -c 'apps._default.enabled=false'
    -c 'features.apps=false'
    -c 'web_search="disabled"'
)
filesystem_entries='":minimal"="read"'
permission_path() {
    local path="$1" access="$2" quoted
    quoted="$(perl -MJSON::PP -e 'print encode_json($ARGV[0])' "$path")"
    # CLI -c splits dotted keys without TOML quoted-key semantics. Keep paths
    # (including dots and slashes) as literal keys in one inline TOML table.
    filesystem_entries+=",$quoted=\"$access\""
}
run_dir="$(cd -P "$run_dir" && pwd)"
permission_path "$ROOT" read
permission_path "$projects_root" read
permission_path "$run_dir" write
# Non-system Codex installations may need their bundled sandbox executable.
# Grant that installation only, never its containing home or node_modules tree.
if [[ -n "${MAINTENANCE_CODEX_RUNTIME_ROOT:-}" ]]; then
    runtime_root="$(cd -P "$MAINTENANCE_CODEX_RUNTIME_ROOT" && pwd)"
    [[ "$runtime_root" != / && "$runtime_root" != "$HOME" ]] || {
        echo 'select only the Codex installation directory as runtime root' >&2; exit 1;
    }
    permission_path "$runtime_root" read
fi
# Explicit denials also cover a mistakenly broad projects selection.
for private in "$HOME/.ssh" "$HOME/.aws" "$HOME/.config/gh" "$HOME/.git-credentials" \
    "${CARGO_HOME:-$HOME/.cargo}/credentials" "${CARGO_HOME:-$HOME/.cargo}/credentials.toml" \
    "${CODEX_HOME:-$HOME/.codex}"; do
    permission_path "$private" deny
done
permission_path "$run_dir/private-probe" deny
configuration+=(-c "permissions.maintenance.filesystem={$filesystem_entries}")
printf '%s\n' "${configuration[@]}" > "$run_dir/permissions.txt"

# Qualify the same permission table without a model call. A clean CLI home
# excludes user configuration, matching exec's --ignore-user-config; managed
# configuration still participates. A failed admission never consumes cadence.
mkdir "$run_dir/sandbox-config"
printf 'dummy private marker\n' > "$run_dir/private-probe"
# shellcheck disable=SC2016 # The sandboxed child expands its positional arguments.
if ! (
    codex_clean login status
    env -i HOME="$HOME" PATH="$PATH" CODEX_HOME="$run_dir/sandbox-config" LANG=C.UTF-8 \
        "$codex_bin" sandbox --permission-profile maintenance --include-managed-config \
        --cd "$run_dir" "${configuration[@]}" -- /bin/bash --noprofile --norc -c '
        set -eu
        cat "$1/tasks/README.md" > /dev/null
        printf "evidence write\n" > "$2/preflight-write"
        if (: >> "$1/tasks/README.md") 2>/dev/null; then exit 31; fi
        if cat "$2/private-probe" >/dev/null 2>&1; then exit 32; fi
        command -v curl >/dev/null
        if curl --disable --noproxy "*" --connect-timeout 2 --max-time 3 http://1.1.1.1 >/dev/null 2>&1; then exit 33; fi
        ' -- "$ROOT" "$run_dir"
) > "$run_dir/preflight.log" 2>&1; then
    echo "Maintenance preflight failed; inspect $run_dir/preflight.log" >&2
    exit 1
fi
if [[ "$mode" == --check ]]; then
    echo "Maintenance permissions and prerequisites checked; no agent started. Evidence: $run_dir"
    exit 0
fi

# Carry one completed report into the admitted evidence directory. Do not grant
# the agent general access to the state root or failed/unfinished run logs.
prior_report=''
for prior in "$state_root"/runs/*; do
    [[ -d "$prior" && ! -L "$prior" && -f "$prior/exit-status" && ! -L "$prior/exit-status" &&
       -s "$prior/report.md" && ! -L "$prior/report.md" ]] || continue
    [[ "$(cat "$prior/exit-status")" == 0 ]] || continue
    prior_report="$prior/report.md"
done
if [[ -n "$prior_report" ]]; then
    cp "$prior_report" "$run_dir/prior-report.md"
    printf '%s\n' "$prior_report" > "$run_dir/prior-report-source.txt"
fi
{
    printf 'Shared Tooling checkout: %s\nProjects directory: %s\nRun directory: %s\n' "$ROOT" "$projects_root" "$run_dir"
    if [[ -n "$prior_report" ]]; then printf 'Prior completed report: %s/prior-report.md\n' "$run_dir";
    else printf 'No prior completed report is available.\n'; fi
    cat "$ROOT/tasks/maintenance-prompt.md"
} > "$run_dir/prompt.txt"
# Dispatch failures retain the original no-burst policy; setup failures above
# are safe to retry because no agent has been started.
cp "$run_dir/started-at" "$run_dir/next-attempt"
mv -f "$run_dir/next-attempt" "$stamp"
codex_clean exec --ignore-user-config --ignore-rules --strict-config --skip-git-repo-check \
    "${configuration[@]}" --cd "$run_dir" --json \
    --output-last-message "$run_dir/report.md" - \
    < "$run_dir/prompt.txt" > "$run_dir/events.jsonl" 2> "$run_dir/stderr.log"
[[ -s "$run_dir/report.md" ]] || { echo 'Codex returned no maintenance report' >&2; exit 1; }
echo "Maintenance report: $run_dir/report.md"
