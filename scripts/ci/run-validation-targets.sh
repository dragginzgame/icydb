#!/usr/bin/env bash
# Shared companions: scripts/ci/check-make-execution.sh scripts/ci/verify-file-checksum.sh
set -euo pipefail

usage() {
    echo "usage: run-validation-targets.sh [--fail-fast] [--input FILE]... <make-target>..." >&2
}

RUNNER_SOURCE="${BASH_SOURCE[0]}"
runner_complete=false
bash "$(dirname "$RUNNER_SOURCE")/check-make-execution.sh"
if [[ -n "${VALIDATION_REPOSITORY_ROOT:-}" ]]; then
    REPOSITORY_ROOT="$VALIDATION_REPOSITORY_ROOT"
else
    REPOSITORY_ROOT="$RUNNER_SOURCE"
    [[ "$REPOSITORY_ROOT" == /* ]] || REPOSITORY_ROOT="$PWD/$REPOSITORY_ROOT"
    REPOSITORY_ROOT="$(cd -P "${REPOSITORY_ROOT%/*}/../.." && printf '%s/.' "$PWD")"
    REPOSITORY_ROOT="${REPOSITORY_ROOT%/.}"
fi
REPOSITORY_ROOT="$(cd -P "$REPOSITORY_ROOT" && printf '%s/.' "$PWD")"
REPOSITORY_ROOT="${REPOSITORY_ROOT%/.}"
if [[ "${VALIDATION_RUNNER_SNAPSHOT_PATH:-}" != "$RUNNER_SOURCE" ]]; then
    RUNNER_SNAPSHOT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/validation-runner.XXXXXX")"
    RUNNER_SNAPSHOT="$RUNNER_SNAPSHOT_DIR/run-validation-targets.sh"
    # shellcheck disable=SC2329 # Invoked by EXIT in this wrapper branch only.
    cleanup_snapshot() {
        local status=$? cleanup_status=0
        # Bash 3.2 can enter EXIT with zero after a nounset error.
        [[ "$runner_complete" == true || "$status" != 0 ]] || status=1
        if [[ "$runner_complete" == true ]]; then
            rm -rf "$RUNNER_SNAPSHOT_DIR" || cleanup_status=$?
            if [[ "$cleanup_status" != 0 ]]; then
                [[ "$status" != 0 ]] || status=$cleanup_status
                printf 'Runner source cleanup failed; inspect: %s\n' "$RUNNER_SNAPSHOT_DIR" >&2 || :
            fi
        else printf 'Runner source retained at: %s\n' "$RUNNER_SNAPSHOT_DIR" >&2; fi
        exit "$status"
    }
    trap cleanup_snapshot EXIT
    cp "$RUNNER_SOURCE" "$RUNNER_SNAPSHOT"
    cp "$(dirname "$RUNNER_SOURCE")/check-make-execution.sh" "$RUNNER_SNAPSHOT_DIR/"
    cp "$(dirname "$RUNNER_SOURCE")/verify-file-checksum.sh" "$RUNNER_SNAPSHOT_DIR/"
    bash -n "$RUNNER_SNAPSHOT"
    snapshot_status=0
    VALIDATION_REPOSITORY_ROOT="$REPOSITORY_ROOT" \
        VALIDATION_RUNNER_SNAPSHOT_PATH="$RUNNER_SNAPSHOT" \
        bash "$RUNNER_SNAPSHOT" "$@" || snapshot_status=$?
    runner_complete=true
    exit "$snapshot_status"
fi

# These values identify only this runner's temporary source snapshot. Targets
# may invoke a logger in another checkout; let that invocation choose its root.
# Keep release selections, failure-log policy and nesting depth inherited.
unset VALIDATION_REPOSITORY_ROOT VALIDATION_RUNNER_SNAPSHOT_PATH

FAIL_FAST=false
input_paths=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --fail-fast) FAIL_FAST=true; shift ;;
        --input)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            input_paths+=("$2"); shift 2 ;;
        *) break ;;
    esac
done

if [[ $# -eq 0 ]]; then
    usage
    exit 2
fi

# Admit the complete goal list before creating logs or running any target.
# Make still interprets assignments after --; neither assignments nor options
# are validation goals. The caller's normal exported variables remain intact.
for target in "$@"; do
    case "$target" in
        ''|-*|*=*|*$'\t'*|*$'\n'*|*$'\r'*)
            echo 'validation requires named Make targets, not options, assignments or control characters' >&2
            exit 2
            ;;
    esac
done

# Depth is metadata, never a shell arithmetic expression. Match the bounded
# decimal convention used by the portable helpers so increment cannot overflow.
RUNNER_DEPTH="${VALIDATION_RUNNER_DEPTH:-0}"
[[ "$RUNNER_DEPTH" =~ ^(0|[1-9][0-9]{0,17})$ ]] || {
    echo 'VALIDATION_RUNNER_DEPTH must be a canonical non-negative decimal integer of at most 18 digits' >&2
    exit 2
}
export VALIDATION_RUNNER_DEPTH="$((RUNNER_DEPTH + 1))"

retain_all_logs=false
if [[ -n "${VALIDATION_LOG_DIR:-}" ]]; then
    # One directory per invocation, including nested invocations. Never replace a
    # previous run or delete consumer-selected evidence on success.
    LOG_ROOT="$VALIDATION_LOG_DIR"
    [[ "$LOG_ROOT" == /* ]] || LOG_ROOT="$PWD/$LOG_ROOT"
    mkdir -p "$LOG_ROOT"
    LOG_ROOT="$(cd -P "$LOG_ROOT" && printf '%s/.' "$PWD")"
    LOG_ROOT="${LOG_ROOT%/.}"
    LOG_DIR="$(mktemp -d "$LOG_ROOT/validation.XXXXXX")"
    retain_all_logs=true
    printf 'target\tresult\tseconds\tlog\n' > "$LOG_DIR/timings.tsv"
    printf 'Validation logs and timings: %s\n' "$LOG_DIR"
else
    LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/validation.XXXXXX")"
fi
preserve_temporary_logs=true
retention_failed=false
cleanup_logs() {
    local status=$? cleanup_status=0
    [[ "$runner_complete" == true || "$status" != 0 ]] || status=1
    if [[ "$runner_complete" != true || "$preserve_temporary_logs" == true ]]; then
        printf 'Validation logs retained at: %s\n' "$LOG_DIR" >&2
    else
        rm -rf "$LOG_DIR" || cleanup_status=$?
        if [[ "$cleanup_status" != 0 ]]; then
            [[ "$status" != 0 ]] || status=$cleanup_status
            printf 'Validation logs cleanup failed; inspect: %s\n' "$LOG_DIR" >&2 || :
        fi
    fi
    exit "$status"
}
trap cleanup_logs EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
FAILURE_LOG_ROOT="${VALIDATION_FAILURE_LOG_DIR:-$REPOSITORY_ROOT/target/validation-failures}"
FAILURE_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"

# A checkpoint belongs to one physical checkout. Nested gates there keep the
# original hashes; unrelated disposable checkouts must establish their own.
input_baseline=''
if [[ "${VALIDATION_INPUT_ROOT:-}" == "$REPOSITORY_ROOT" ]]; then
    input_baseline="${VALIDATION_INPUT_BASELINE:-}"
    [[ -f "$input_baseline" && ! -L "$input_baseline" ]] || {
        echo 'error: missing inherited validation input checkpoint' >&2; exit 1;
    }
fi
unset VALIDATION_INPUT_ROOT VALIDATION_INPUT_BASELINE

input_checkpoint() {
    perl -MJSON::PP - "$REPOSITORY_ROOT" "$input_baseline" "$1" "${RUNNER_SOURCE%/*}/verify-file-checksum.sh" \
        ${input_paths[@]+"${input_paths[@]}"} <<'PERL'
use strict;
use warnings;
my ($root, $baseline, $output, $checksum, @paths) = @ARGV;
my $json = JSON::PP->new->canonical;
my $expected = {};
if (length $baseline) {
    open my $in, '<', $baseline or die "error: read checkpoint $baseline: $!\n";
    local $/;
    $expected = $json->decode(<$in>);
    close $in or die "error: close checkpoint: $!\n";
}
my %observed;
my $failed = 0;
for my $path (sort keys %{{map { $_ => 1 } (keys %$expected, @paths)}}) {
    die "error: checkpoint inputs must be relative file paths: $path\n"
        if $path =~ m{^/|[\x00\r\n]|(?:^|/)\.{1,2}(?:/|$)|//|/$};
    my $hash;
    eval {
        my $route = $root;
        for my $part (split m{/}, $path) {
            $route .= "/$part";
            die "symlink in input route" if -l $route;
        }
        die "not a regular readable file" unless -f $route && -r $route;
        open my $in, '-|', 'bash', $checksum, '--print', 'sha256', $route or die "checksum: $!";
        local $/;
        $hash = <$in>;
        close $in or die "checksum observation failed";
        $hash =~ s/\n\z//;
        die "invalid checksum" unless $hash =~ /\A[0-9a-f]{64}\z/;
    };
    if ($@) {
        $observed{$path} = {error => "$@"};
        print STDERR "error: cannot observe validation input $path: $@\n";
        $failed = 1;
    } else {
        $observed{$path} = $hash;
        if (exists $expected->{$path} && $expected->{$path} ne $hash) {
            print STDERR "error: validation input changed: $path ($expected->{$path} -> $hash)\n";
            $failed = 1;
        }
    }
}
open my $out, '>', $output or die "error: write input evidence $output: $!\n";
print {$out} $json->encode(\%observed), "\n" or die "error: write input evidence: $!\n";
close $out or die "error: close input evidence: $!\n";
if ($failed) {
    print STDERR "Restart preparation and the full gate against the current inputs; prior validation cannot be reused.\n";
    exit 1;
}
PERL
}

if [[ -n "$input_baseline" || ${#input_paths[@]} -gt 0 ]]; then
    input_checkpoint "$LOG_DIR/inputs.json" 2> "$LOG_DIR/inputs-error.log" || {
        cat "$LOG_DIR/inputs-error.log" >&2; exit 1;
    }
    input_baseline="$LOG_DIR/inputs.json"
    export VALIDATION_INPUT_ROOT="$REPOSITORY_ROOT" VALIDATION_INPUT_BASELINE="$input_baseline"
fi
input_drift=false
check_inputs() {
    [[ -n "$input_baseline" ]] || return 0
    input_checkpoint "$LOG_DIR/$1-inputs.json"
}

MAX_FAILURE_DETAIL_LINES=160
# One diagnostic predicate drives live highlighting and retained selection.
# Rust paths such as error::tests are names, not diagnostics.
INHERITED_DIAGNOSTIC_PATTERN='^\[ERR:[^]]+\]'
DIAGNOSTIC_PATTERN="$INHERITED_DIAGNOSTIC_PATTERN|"'error:([^:]|$)|error\[|rustc-LLVM ERROR|test result: FAILED|^test .* \.\.\. FAILED$|fatal:|FAILED:|Target failed:|No such file or directory|❌|🚨|make(\[[^]]*\])?:.*\*\*\*'
FAILURE_EVENT_PREFIX="${VALIDATION_FAILURE_EVENT_PREFIX:-}"
if [[ -n "$FAILURE_EVENT_PREFIX" ]]; then
    if [[ "$FAILURE_EVENT_PREFIX" == *$'\n'* || "$FAILURE_EVENT_PREFIX" == *$'\r'* ]]; then
        echo 'failure event prefix must be a single line' >&2
        exit 2
    fi
    # The consumer supplies a literal line prefix, never a regular expression.
    escaped_prefix="$(printf '%s' "$FAILURE_EVENT_PREFIX" | sed 's/[][\\.^$*+?{}()|]/\\&/g')"
    DIAGNOSTIC_PATTERN="^$escaped_prefix|$DIAGNOSTIC_PATTERN"
fi
# These headings add context without classifying their lines as errors.
FAILURE_PATTERN="$DIAGNOSTIC_PATTERN|---- .* stdout ----|failures:|target failed"

failed_targets=()
failure_status=0
summary_status=0
targets=()
results=()
elapsed_seconds=()
logs=()
retained_logs=()
highlighted_failure_log=""

print_error_line() {
    local target="$1"
    local line="$2"

    if [[ -t 1 && -z "${NO_COLOR:-}" && "${TERM:-dumb}" != "dumb" ]]; then
        printf '\033[1;31m[ERR:%s] %s\033[0m\n' "$target" "$line"
    else
        printf '[ERR:%s] %s\n' "$target" "$line"
    fi
}

print_retained_error_line() {
    local line="$1"

    if [[ -t 1 && -z "${NO_COLOR:-}" && "${TERM:-dumb}" != "dumb" ]]; then
        printf '\033[1;31m%s\033[0m\n' "$line"
    else
        printf '%s\n' "$line"
    fi
}

is_live_failure_line() {
    [[ "$1" =~ $DIAGNOSTIC_PATTERN ]]
}

annotate_live_output() {
    local target="$1"
    local line

    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ "$line" =~ $INHERITED_DIAGNOSTIC_PATTERN ]]; then
            print_retained_error_line "$line"
        elif is_live_failure_line "$line"; then
            print_error_line "$target" "$line"
        else
            printf '%s\n' "$line"
        fi
    done
}

# Publish an exact directory entry, never follow an alias or move into a late
# directory. Failed candidates remain beside their complete unique source.
publish_latest_log() {
    local log="$1" destination="$2" candidate
    candidate="$(mktemp "$log.latest.XXXXXX")" || return $?
    cp -p "$log" "$candidate" || return $?
    perl -e 'rename($ARGV[0], $ARGV[1]) or die "publish latest log: $!\n"' "$candidate" "$destination"
}

persist_failure_log() {
    local log="$1"
    local target="$2"
    local index="$3"
    local safe_target="${target//[^[:alnum:]._-]/_}"
    local retained_log="$FAILURE_LOG_ROOT/$FAILURE_RUN_ID-$index-$safe_target.log"

    if ! mkdir -p "$FAILURE_LOG_ROOT" || ! cp "$log" "$retained_log"; then
        return 0
    fi
    if ! publish_latest_log "$retained_log" "$FAILURE_LOG_ROOT/latest.log"; then
        printf 'Unable to publish latest failure log; complete log retained at: %s\n' "$retained_log" >&2
    fi
    printf '%s\n' "$retained_log"
}

select_failure_detail() {
    local log="$1"
    local target="$2"
    local clean_log="${log}.clean"
    local details

    # Cargo forces ANSI color in CI. Normalize only the diagnostic copy so
    # matching remains deterministic while the live output stays colored.
    LC_ALL=C sed $'s/\033\[[0-9;]*[[:alpha:]]//g' "$log" >"$clean_log"

    print_error_line "$target" "Target failed"
    # Bound the selected excerpt before Bash captures it. A large failed build
    # can otherwise copy its entire matching log into shell memory repeatedly.
    details="$(
        if command -v rg >/dev/null 2>&1; then
            rg --color never --no-heading -C 4 -- "$FAILURE_PATTERN" "$clean_log" || true
        else
            grep -E -C 4 -- "$FAILURE_PATTERN" "$clean_log" || true
        fi |
            awk '!/^\[ERR:[^]]+\]/ || !seen[$0]++' |
            tail -n "$((MAX_FAILURE_DETAIL_LINES - 1))"
    )"
    if [[ -z "$details" ]]; then
        details="$(tail -n 80 "$clean_log")"
    fi

    while IFS= read -r line; do
        if [[ "$line" =~ $INHERITED_DIAGNOSTIC_PATTERN ]]; then
            print_retained_error_line "$line"
        elif is_live_failure_line "$line"; then
            print_error_line "$target" "$line"
        else
            # Retain useful context without labelling it as an error.
            printf '[%s] %s\n' "$target" "$line"
        fi
    done <<< "$details"
}

print_failure_detail() {
    local log="$1" target="$2" excerpt="$1.excerpt" line
    # Select once per immutable failed-target log. Console, retained highlights
    # and GitHub summaries project the same excerpt without rescanning raw bytes.
    if [[ ! -f "$excerpt" ]]; then
        select_failure_detail "$log" "$target" > "$excerpt"
    fi
    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ "$line" =~ $INHERITED_DIAGNOSTIC_PATTERN ]]; then
            print_retained_error_line "$line"
        else
            printf '%s\n' "$line"
        fi
    done < "$excerpt"
}

persist_highlighted_failure_log() {
    local highlighted_log="$FAILURE_LOG_ROOT/$FAILURE_RUN_ID-errors.log"

    mkdir -p "$FAILURE_LOG_ROOT" || return 0
    : >"$highlighted_log" || return 0
    for index in "${!targets[@]}"; do
        if [[ "${results[$index]}" == "FAIL" ]]; then
            print_failure_detail "${logs[$index]}" "${targets[$index]}" >>"$highlighted_log"
        fi
    done
    if ! publish_latest_log "$highlighted_log" "$FAILURE_LOG_ROOT/latest-errors.log"; then
        printf 'Unable to publish latest highlighted errors; complete log retained at: %s\n' "$highlighted_log" >&2
    fi
    printf '%s\n' "$highlighted_log"
}

persist_combined_failure_log() {
    local combined="$FAILURE_LOG_ROOT/$FAILURE_RUN_ID-combined.log"
    local index
    mkdir -p "$FAILURE_LOG_ROOT" || return 1
    # Concatenate only this invocation's raw failed-target logs in dispatch
    # order. Nested logger output is already part of its parent's raw target.
    # Never glob another invocation's files or aggregate our decorated summary.
    if ! (
        for index in "${!targets[@]}"; do
            if [[ "${results[$index]}" == FAIL ]]; then
                cat "${logs[$index]}" || exit 1
            fi
        done
    ) > "$combined"; then
        printf 'Incomplete combined failure log retained at: %s\n' "$combined" >&2
        return 1
    fi
    printf '%s\n' "$combined"
}

write_github_summary() {
    if [[ "$RUNNER_DEPTH" != "0" || -z "${GITHUB_STEP_SUMMARY:-}" ]]; then
        return
    fi
    local summary="$LOG_DIR/github-summary.md"

    {
        echo "### Validation summary"
        echo
        echo "| Result | Seconds | Target |"
        echo "| --- | ---: | --- |"
        for index in "${!targets[@]}"; do
            printf "| %s | %s | \`%s\` |\n" \
                "${results[$index]}" \
                "${elapsed_seconds[$index]}" \
                "${targets[$index]}"
        done

        if [[ ${#failed_targets[@]} -ne 0 ]]; then
            echo
            echo "#### Failure details"
            echo
            echo '```text'
            for index in "${!targets[@]}"; do
                if [[ "${results[$index]}" == "FAIL" ]]; then
                    echo
                    print_failure_detail "${logs[$index]}" "${targets[$index]}"
                fi
            done
            echo '```'
        fi
    } >"$summary"
    # Keep a complete local copy before appending to the external summary path.
    # Its failure must not interrupt raw-log retention or mask a target failure.
    if cat "$summary" >>"$GITHUB_STEP_SUMMARY"; then
        :
    else
        summary_status=$?
        preserve_temporary_logs=true
        printf 'Unable to append GitHub summary; complete summary retained at: %s\n' "$summary" >&2 || :
    fi
}

for target in "$@"; do
    log="$LOG_DIR/${#targets[@]}.log"
    start="$SECONDS"
    if [[ "${GITHUB_ACTIONS:-}" == "true" && "$RUNNER_DEPTH" == "0" ]]; then
        printf '::group::%s\n' "$target"
    else
        printf '\n==> %s\n' "$target"
    fi

    # Run no subsequent target once prepared inputs drift, even in diagnostic
    # (non-fail-fast) mode. Preserve a target's original failure if both fail.
    target_status=0
    if check_inputs "${#targets[@]}-before" > "$log" 2>&1; then
        if make --no-print-directory -C "$REPOSITORY_ROOT" -- "$target" 2>&1 |
        tee -a "$log" |
        annotate_live_output "$target"; then
            :
        else
            pipeline_status=("${PIPESTATUS[@]}")
            for component_status in "${pipeline_status[@]}"; do
                if [[ "$component_status" != 0 ]]; then
                    target_status="$component_status"
                    break
                fi
            done
        fi
        if ! check_inputs "${#targets[@]}-after" >> "$log" 2>&1; then
            input_drift=true
            [[ "$target_status" != 0 ]] || target_status=1
        fi
    else
        input_drift=true
        target_status=1
    fi
    if [[ "$target_status" == 0 ]]; then
        result="PASS"
        retained_log=""
    else
        [[ "$failure_status" != 0 ]] || failure_status="$target_status"
        failed_targets+=("$target")
        result="FAIL"
        retained_log="$(persist_failure_log "$log" "$target" "${#targets[@]}")"
        echo
        print_error_line "$target" "Target failed"
        if [[ -n "$retained_log" ]]; then
            print_error_line "$target" "Full failure log retained at: $retained_log"
        else
            retention_failed=true
            retained_log="$log"
            print_error_line "$target" \
                "Unable to retain the complete failure log under: $FAILURE_LOG_ROOT"
            print_error_line "$target" "Full failure log retained at: $retained_log"
        fi
    fi

    elapsed="$((SECONDS - start))"
    targets+=("$target")
    results+=("$result")
    elapsed_seconds+=("$elapsed")
    logs+=("$log")
    retained_logs+=("$retained_log")
    if [[ "$retain_all_logs" == true ]]; then
        printf '%s\t%s\t%s\t%s\n' "$target" "$result" "$elapsed" "$log" >> "$LOG_DIR/timings.tsv"
    fi
    if [[ "${GITHUB_ACTIONS:-}" == "true" && "$RUNNER_DEPTH" == "0" ]]; then
        printf '::endgroup::\n'
    fi

    if [[ "$input_drift" == true || ( "$result" == "FAIL" && "$FAIL_FAST" == "true" ) ]]; then
        break
    fi
done

if [[ "$retention_failed" == false && "$retain_all_logs" == false ]]; then preserve_temporary_logs=false; fi
if [[ -n "$input_baseline" && ${#failed_targets[@]} -gt 0 ]]; then preserve_temporary_logs=true; fi

printf '\nValidation summary:\n'
for index in "${!targets[@]}"; do
    if [[ "${results[$index]}" == "FAIL" ]]; then
        print_error_line "${targets[$index]}" \
            "FAIL ${elapsed_seconds[$index]}s"
    else
        printf '  %-4s %5ss  %s\n' \
            "${results[$index]}" \
            "${elapsed_seconds[$index]}" \
            "${targets[$index]}"
    fi
done

write_github_summary

if [[ ${#failed_targets[@]} -ne 0 ]]; then
    if combined_failure_log="$(persist_combined_failure_log)"; then
        printf 'Combined failure log retained at: %s\n' "$combined_failure_log"
        # Publish the latest completed batch atomically; keep the unique log
        # and any failed candidate. latest.log retains its last-target contract.
        if publish_latest_log "$combined_failure_log" "$FAILURE_LOG_ROOT/latest-combined.log"; then
            printf 'Latest combined failure log: %s/latest-combined.log\n' "$FAILURE_LOG_ROOT"
        else
            printf 'Unable to publish latest combined failure log\n' >&2
            preserve_temporary_logs=true
        fi
    else
        printf 'Unable to retain complete combined failure log; original logs retained\n' >&2
        preserve_temporary_logs=true
    fi
    highlighted_failure_log="$(persist_highlighted_failure_log)"
    if [[ "$RUNNER_DEPTH" == 0 ]]; then printf '\nFailure details (selected from the full logs):\n'; fi
    for index in "${!targets[@]}"; do
        if [[ "${results[$index]}" == "FAIL" ]]; then
            echo
            if [[ -n "${retained_logs[$index]}" ]]; then
                last_retained_log="${retained_logs[$index]}"
                print_error_line "${targets[$index]}" \
                    "Full failure log retained at: ${retained_logs[$index]}"
            fi
            if [[ "$RUNNER_DEPTH" == 0 ]]; then
                print_failure_detail "${logs[$index]}" "${targets[$index]}"
            fi
        fi
    done

    if [[ -n "${last_retained_log:-}" ]]; then
        echo
        print_error_line summary \
            "Latest complete failure log: $last_retained_log"
    fi
    if [[ -n "$highlighted_failure_log" ]]; then
        print_error_line summary \
            "Latest highlighted errors: $highlighted_failure_log"
    fi

    if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
        printf '::error title=Validation targets failed::%s\n' "${failed_targets[*]}"
    fi
    echo >&2
    print_error_line summary "VALIDATION FAILED: ${failed_targets[*]}" >&2
    runner_complete=true
    exit "$failure_status"
fi

if [[ "$summary_status" != 0 ]]; then
    print_error_line summary 'VALIDATION FAILED: targets passed, but GitHub summary could not be written' >&2
    runner_complete=true
    exit "$summary_status"
fi

echo "VALIDATION PASSED: all requested targets succeeded."
runner_complete=true
