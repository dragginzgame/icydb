#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: run-validation-targets.sh [--fail-fast] <make-target>..." >&2
}

RUNNER_SOURCE="${BASH_SOURCE[0]}"
bash "$(dirname "$RUNNER_SOURCE")/check-make-execution.sh"
REPOSITORY_ROOT="${VALIDATION_REPOSITORY_ROOT:-$(cd "$(dirname "$RUNNER_SOURCE")/../.." && pwd)}"
if [[ "${VALIDATION_RUNNER_SNAPSHOT_PATH:-}" != "$RUNNER_SOURCE" ]]; then
    RUNNER_SNAPSHOT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/validation-runner.XXXXXX")"
    RUNNER_SNAPSHOT="$RUNNER_SNAPSHOT_DIR/run-validation-targets.sh"
    trap 'rm -rf "$RUNNER_SNAPSHOT_DIR"' EXIT
    cp "$RUNNER_SOURCE" "$RUNNER_SNAPSHOT"
    cp "$(dirname "$RUNNER_SOURCE")/check-make-execution.sh" "$RUNNER_SNAPSHOT_DIR/"
    bash -n "$RUNNER_SNAPSHOT"
    snapshot_status=0
    VALIDATION_REPOSITORY_ROOT="$REPOSITORY_ROOT" \
        VALIDATION_RUNNER_SNAPSHOT_PATH="$RUNNER_SNAPSHOT" \
        bash "$RUNNER_SNAPSHOT" "$@" || snapshot_status=$?
    exit "$snapshot_status"
fi

# These values identify only this runner's temporary source snapshot. Targets
# may invoke a logger in another checkout; let that invocation choose its root.
# Keep release selections, failure-log policy and nesting depth inherited.
unset VALIDATION_REPOSITORY_ROOT VALIDATION_RUNNER_SNAPSHOT_PATH

FAIL_FAST=false
if [[ "${1:-}" == "--fail-fast" ]]; then
    FAIL_FAST=true
    shift
fi

if [[ $# -eq 0 ]]; then
    usage
    exit 2
fi

LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/validation.XXXXXX")"
preserve_temporary_logs=true
retention_failed=false
cleanup_logs() {
    if [[ "$preserve_temporary_logs" == true ]]; then
        printf 'Validation logs retained at: %s\n' "$LOG_DIR" >&2
    else
        rm -rf "$LOG_DIR"
    fi
}
trap cleanup_logs EXIT
FAILURE_LOG_ROOT="${VALIDATION_FAILURE_LOG_DIR:-$REPOSITORY_ROOT/target/validation-failures}"
FAILURE_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"

RUNNER_DEPTH="${VALIDATION_RUNNER_DEPTH:-0}"
export VALIDATION_RUNNER_DEPTH="$((RUNNER_DEPTH + 1))"
MAX_FAILURE_DETAIL_LINES=160
FAILURE_PATTERN='---- .* stdout ----|^test .* \.\.\. FAILED$|failures:|test result: FAILED|error(\[[A-Z0-9]+\])?:([^:]|$)|target failed|make(\[[0-9]+\])?: \*\*\*'

failed_targets=()
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
    local line="$1"

    case "$line" in
        # Rust paths such as error::tests are names, not diagnostics.
        *"error:"[!:]* | *"error:" | *"error["* | *"rustc-LLVM ERROR"* | \
            *"test result: FAILED"* | test\ *" ... FAILED" | *"fatal:"* | \
            *"FAILED:"* | *"Target failed:"* | *"No such file or directory"* | \
            *"❌"* | *"🚨"* | \
            *make:*"***"* | *make\[*"***"*) return 0 ;;
        *) return 1 ;;
    esac
}

annotate_live_output() {
    local target="$1"
    local line

    while IFS= read -r line || [[ -n "$line" ]]; do
        if is_live_failure_line "$line"; then
            print_error_line "$target" "$line"
        else
            printf '%s\n' "$line"
        fi
    done
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
    cp "$log" "$FAILURE_LOG_ROOT/latest.log" || true
    printf '%s\n' "$retained_log"
}

print_failure_detail() {
    local log="$1"
    local target="$2"
    local clean_log="${log}.clean"
    local details
    local inherited

    # Cargo forces ANSI color in CI. Normalize only the diagnostic copy so
    # matching remains deterministic while the live output stays colored.
    LC_ALL=C sed $'s/\033\[[0-9;]*[[:alpha:]]//g' "$log" >"$clean_log"

    if command -v rg >/dev/null 2>&1; then
        inherited="$(rg '^\[ERR:[^]]+\]' "$clean_log" || true)"
    else
        inherited="$(grep -E '^\[ERR:[^]]+\]' "$clean_log" || true)"
    fi
    if [[ -n "$inherited" ]]; then
        while IFS= read -r line; do
            print_retained_error_line "$line"
        done < <(
            printf '%s\n' "$inherited" |
                awk '!seen[$0]++' |
                tail -n "$MAX_FAILURE_DETAIL_LINES"
        )
        return
    fi

    print_error_line "$target" "Target failed"
    if command -v rg >/dev/null 2>&1; then
        details="$(rg --color never --no-heading -C 4 -- "$FAILURE_PATTERN" "$clean_log" || true)"
    else
        details="$(grep -E -C 4 -- "$FAILURE_PATTERN" "$clean_log" || true)"
    fi
    if [[ -z "$details" ]]; then
        details="$(tail -n 80 "$clean_log")"
    fi

    while IFS= read -r line; do
        if is_live_failure_line "$line"; then
            print_error_line "$target" "$line"
        else
            # Retain useful context without labelling it as an error.
            printf '[%s] %s\n' "$target" "$line"
        fi
    done < <(printf '%s\n' "$details" | tail -n "$((MAX_FAILURE_DETAIL_LINES - 1))")
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
    cp "$highlighted_log" "$FAILURE_LOG_ROOT/latest-errors.log" || true
    printf '%s\n' "$highlighted_log"
}

write_github_summary() {
    if [[ "$RUNNER_DEPTH" != "0" || -z "${GITHUB_STEP_SUMMARY:-}" ]]; then
        return
    fi

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
    } >>"$GITHUB_STEP_SUMMARY"
}

for target in "$@"; do
    log="$LOG_DIR/${#targets[@]}.log"
    start="$SECONDS"
    if [[ "${GITHUB_ACTIONS:-}" == "true" && "$RUNNER_DEPTH" == "0" ]]; then
        printf '::group::%s\n' "$target"
    else
        printf '\n==> %s\n' "$target"
    fi

    if make --no-print-directory -C "$REPOSITORY_ROOT" "$target" 2>&1 |
        tee "$log" |
        annotate_live_output "$target"; then
        result="PASS"
        retained_log=""
    else
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
        print_failure_detail "$log" "$target"
    fi

    elapsed="$((SECONDS - start))"
    targets+=("$target")
    results+=("$result")
    elapsed_seconds+=("$elapsed")
    logs+=("$log")
    retained_logs+=("$retained_log")
    if [[ "${GITHUB_ACTIONS:-}" == "true" && "$RUNNER_DEPTH" == "0" ]]; then
        printf '::endgroup::\n'
    fi

    if [[ "$result" == "FAIL" && "$FAIL_FAST" == "true" ]]; then
        break
    fi
done

if [[ "$retention_failed" == false ]]; then preserve_temporary_logs=false; fi

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
    highlighted_failure_log="$(persist_highlighted_failure_log)"
    printf '\nFailure details (repeated from the full logs):\n'
    for index in "${!targets[@]}"; do
        if [[ "${results[$index]}" == "FAIL" ]]; then
            echo
            if [[ -n "${retained_logs[$index]}" ]]; then
                print_error_line "${targets[$index]}" \
                    "Full failure log retained at: ${retained_logs[$index]}"
            fi
            print_failure_detail "${logs[$index]}" "${targets[$index]}"
        fi
    done

    if [[ -f "$FAILURE_LOG_ROOT/latest.log" ]]; then
        echo
        print_error_line summary \
            "Latest complete failure log: $FAILURE_LOG_ROOT/latest.log"
    fi
    if [[ -n "$highlighted_failure_log" ]]; then
        print_error_line summary \
            "Latest highlighted errors: $FAILURE_LOG_ROOT/latest-errors.log"
    fi

    if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
        printf '::error title=Validation targets failed::%s\n' "${failed_targets[*]}"
    fi
    echo >&2
    print_error_line summary "VALIDATION FAILED: ${failed_targets[*]}" >&2
    exit 1
fi

echo "VALIDATION PASSED: all requested targets succeeded."
