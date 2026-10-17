#!/usr/bin/env bash
# Shared companions: scripts/ci/fixture-lifecycle.sh
# Sourced qualification helpers. Callers own fixture rosters and round trips.

# Render only the actual initialization/EXIT boundary. Explicit variable names
# avoid guessing a consumer's directory or repository ownership. The child must
# receive RETENTION_SOURCE_ROOT and RETENTION_EXIT_PATH from its caller.
fixture_probe_prefix() {
    local source="$1" output="$2" root_variable="$3" directory_variable="$4" injection="$5"
    [[ "$root_variable" =~ ^[A-Za-z_][A-Za-z0-9_]*$ &&
       "$directory_variable" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || return 2
    RETENTION_ROOT_VARIABLE="$root_variable" RETENTION_DIRECTORY_VARIABLE="$directory_variable" \
        RETENTION_INJECTION="$injection" awk '
        $0 ~ "^" ENVIRON["RETENTION_ROOT_VARIABLE"] "=" {
            print ENVIRON["RETENTION_ROOT_VARIABLE"] "=\"$RETENTION_SOURCE_ROOT\""
            next
        }
        { print }
        /^trap .* EXIT$/ {
            variable=ENVIRON["RETENTION_DIRECTORY_VARIABLE"]
            print "printf \"%s\\n\" \"$" variable "\" > \"$RETENTION_EXIT_PATH\""
            print "printf \"retained evidence\\n\" > \"$" variable "/exit-evidence\""
            print ENVIRON["RETENTION_INJECTION"]
            print "exit 99"
            injected=1
            exit
        }
        END { if (!injected) exit 1 }
    ' "$source" > "$output"
}

# Optional caller selections: FIXTURE_PROBE_ROOT_VARIABLE, DIRECTORY_VARIABLE,
# COMPLETION_VARIABLE and TEMPORARY_ROOT use the existing FIXTURE_PROBE_ prefix.
# Defaults retain the standard fixture contract. A selected scratch root may be
# relative or aliased; returned fixture_probe_retained is checked against its
# physical directory. Callers own cwd, RUNNER_TEMP and product diagnostics.
fixture_probe_exit() {
    local source="$1" probe_root="$2" source_root="$3" name="$4" failure="$5"
    shift 5
    local injection expected status=0 retained
    local completion="${FIXTURE_PROBE_COMPLETION_VARIABLE:-fixture_complete}"
    local temporary_root="${FIXTURE_PROBE_TEMPORARY_ROOT:-$probe_root}" physical_root
    fixture_probe_retained=''
    [[ "$completion" =~ ^[A-Za-z_][A-Za-z0-9_]*$ && -d "$temporary_root" ]] || return 2
    physical_root="$(CDPATH='' cd -P -- "$temporary_root" && printf '%s/.' "$PWD")" || return
    physical_root="${physical_root%/.}"
    # Expand probe variables only when the generated child script executes.
    # shellcheck disable=SC2016
    case "$failure" in
        nounset) injection='unset RETENTION_UNBOUND; printf "%s\n" "$RETENTION_UNBOUND"'; expected=1 ;;
        nounset-function) injection='unset RETENTION_UNBOUND; probe_unbound() { echo "$RETENTION_UNBOUND"; }; probe_unbound'; expected=1 ;;
        command) injection='false'; expected=1 ;;
        nonzero) injection='exit 23'; expected=23 ;;
        premature) injection='exit 0'; expected=1 ;;
        completed) injection="$completion=true; exit 0"; expected=0 ;;
        retained-completed) injection="$completion=true; exit 0"; expected=0 ;;
        failed-completion) injection="$completion=true; exit 23"; expected=23 ;;
        cleanup-failure) injection="rm() { return 37; }; $completion=true; exit 0"; expected=37 ;;
        diagnostic) injection='printf "fixture output\n" > "$'"${FIXTURE_PROBE_DIRECTORY_VARIABLE:-fixture}"'/output"; cat() { return 7; }; exit 23'; expected=23 ;;
        *) printf 'Unknown retention probe: %s\n' "$failure" >&2; return 2 ;;
    esac
    local directory="$probe_root/exit-source/scripts/ci"
    local exit_path="$probe_root/$name-$failure.exit"
    mkdir -p "$directory" || return
    cp "$source_root/scripts/ci/fixture-lifecycle.sh" "$directory/" || return
    cp "$source_root/scripts/ci/fixture-retention-probes.sh" "$directory/" || return
    fixture_probe_prefix "$source" "$directory/probe.sh" \
        "${FIXTURE_PROBE_ROOT_VARIABLE:-ROOT}" "${FIXTURE_PROBE_DIRECTORY_VARIABLE:-fixture}" \
        "$injection" || return
    # A repeated name must prove this child reached its EXIT boundary. Keep
    # earlier retained payloads, but never accept their receipt for a new run.
    rm -f -- "$exit_path" || return
    env -u fixture -u TEST_ROOT -u FIXTURE -u FIXTURE_ROOT -u scratch \
        RETENTION_SOURCE_ROOT="$source_root" RETENTION_EXIT_PATH="$exit_path" \
        TMPDIR="$temporary_root" "$BASH" "$directory/probe.sh" "$@" \
        > "$probe_root/$name-$failure.log" 2>&1 || status=$?
    [[ "$status" == "$expected" && -f "$exit_path" ]] || return 1
    retained="$(cat "$exit_path")" || return
    [[ -n "$retained" ]] || return 1
    [[ "$retained" == "$physical_root/"* ]] || return 1
    if [[ "$expected" == 0 && "$failure" != retained-completed ]]; then
        [[ ! -e "$retained" ]] || return 1
    else
        [[ -d "$retained" && "$(cat "$retained/exit-evidence")" == 'retained evidence' ]] || return 1
    fi
    # shellcheck disable=SC2034 # Caller-owned diagnostics may name the exact path.
    fixture_probe_retained="$retained"
}

fixture_probe_exits() {
    local failure
    for failure in nounset nounset-function command nonzero premature completed failed-completion cleanup-failure; do
        fixture_probe_exit "$1" "$2" "$3" "$4" "$failure" "${@:5}" || return
    done
}

# Execute an effectful fixture with caller-prepared PATH stubs and assertions.
# Publish the retained directory without a command substitution/errexit boundary.
fixture_probe_effect() {
    [[ $# == 6 ]] || return 2
    local script="$1" run="$2" prefix="$3" expected="$4" interpreter="$5" marker="$6"
    local status=0 path
    fixture_effect_retained=''
    [[ -f "$script" && -d "$run/tmp" && "$prefix" != */* && -n "$prefix" ]] || return 2
    case "$expected" in 0|[1-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5]) ;; *) return 2 ;; esac
    TMPDIR="$run/tmp" "$interpreter" "$script" > "$run/result.log" 2>&1 || status=$?
    printf '%s\n' "$status" > "$run/status.txt" || return
    [[ "$status" == "$expected" ]] || return 1
    if [[ "$expected" == 0 ]]; then
        # Include hidden entries without changing the caller's glob options.
        for path in "$run/tmp"/* "$run/tmp"/.[!.]* "$run/tmp"/..?*; do
            [[ ! -e "$path" && ! -L "$path" ]] || return 1
        done
        return 0
    fi
    grep -Fq -- "$marker" "$run/result.log" || return 1
    local matches=("$run/tmp/$prefix".*)
    [[ ${#matches[@]} == 1 && -d "${matches[0]}" ]] || return 1
    # shellcheck disable=SC2034 # Public result consumed by the caller's payload assertions.
    fixture_effect_retained="${matches[0]}"
}

# Select one literal assertion with --selected SOURCE PREFIX BINDINGS.
# Selected bindings are trusted, caller-owned shell source, never external input.
# Only the initialization/EXIT prefix runs; fixture bodies and
# tools cannot run. Refuse unsupported layouts before starting the child.
fixture_probe_assertion() {
    [[ $# == 8 && "$5" == --selected ]] || return 2
    local source="$1" probe_root="$2" source_root="$3" name="$4"
    shift 4
    local directory="$probe_root/assertion-source" status=0 retained
    local observation="$probe_root/$name-assertion.path"
    mkdir -p "$directory" || return
    local reached="$probe_root/$name-assertion.reached" assertion='' bindings="$4"
    rm -f -- "$observation" "$reached" || return
    assertion="$(RETENTION_ASSERTION_PREFIX="$3" awk '
        { line=$0; sub(/^[[:space:]]*/, "", line) }
        index(line, ENVIRON["RETENTION_ASSERTION_PREFIX"]) == 1 { print; found++ }
        END { if (found != 1 || ENVIRON["RETENTION_ASSERTION_PREFIX"] == "") exit 1 }
    ' "$2")" || return
    [[ "$assertion" == *'[[ '* ]] || return 2
    RETENTION_ASSERTION="$assertion" RETENTION_BINDINGS="$bindings" awk '
        { line=$0 }
        /^root="\$\{root%\/\.\}"$/ {
            line="root=\"$RETENTION_SOURCE_ROOT\""; anchored=1
        }
        /^trap .* EXIT$/ && !trapped {
            print line
            print "printf \"%s\\n\" \"$fixture\" > \"$RETENTION_ASSERTION_PATH\""
            print "printf \"retained assertion evidence\\n\" > \"$fixture/assertion-evidence\""
            trapped=1
            print ENVIRON["RETENTION_BINDINGS"]
            print "printf reached > \"$RETENTION_ASSERTION_REACHED\""
            # Preserve return-based function guards as well as exit guards.
            print "_retention_selected_assertion() {"
            print ENVIRON["RETENTION_ASSERTION"]
            print "}"
            print "_retention_selected_assertion"
            print "fixture_complete=true; exit 0"
            injected=1; exit
        }
        { print line }
        END { if (!anchored || !trapped || !injected) exit 1 }
    ' "$source" > "$directory/$name.sh" || return
    RETENTION_SOURCE_ROOT="$source_root" RETENTION_ASSERTION_PATH="$observation" \
        RETENTION_ASSERTION_REACHED="$reached" \
        TMPDIR="$probe_root" "$BASH" "$directory/$name.sh" \
        > "$probe_root/$name-assertion.log" 2>&1 || status=$?
    [[ "$status" == 1 && -f "$reached" && "$(cat "$reached")" == reached ]] || return 1
    retained="$(cat "$observation")" || return
    [[ -n "$retained" && -d "$retained" &&
        "$(cat "$retained/assertion-evidence")" == 'retained assertion evidence' ]] || return 1
}
