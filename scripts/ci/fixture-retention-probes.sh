#!/usr/bin/env bash
# Shared companions: scripts/ci/fixture-lifecycle.sh
# Sourced qualification helpers. Callers own fixture rosters and round trips.

fixture_probe_exit() {
    local source="$1" probe_root="$2" source_root="$3" name="$4" failure="$5"
    shift 5
    local injection expected status=0 retained
    # Expand probe variables only when the generated child script executes.
    # shellcheck disable=SC2016
    case "$failure" in
        nounset) injection='unset RETENTION_UNBOUND; printf "%s\n" "$RETENTION_UNBOUND"'; expected=1 ;;
        nounset-function) injection='unset RETENTION_UNBOUND; probe_unbound() { echo "$RETENTION_UNBOUND"; }; probe_unbound'; expected=1 ;;
        command) injection='false'; expected=1 ;;
        nonzero) injection='exit 23'; expected=23 ;;
        premature) injection='exit 0'; expected=1 ;;
        completed) injection='fixture_complete=true; exit 0'; expected=0 ;;
        failed-completion) injection='fixture_complete=true; exit 23'; expected=23 ;;
        cleanup-failure) injection='rm() { return 37; }; fixture_complete=true; exit 0'; expected=37 ;;
        diagnostic) injection='printf "fixture output\n" > "${fixture:-${TEST_ROOT:-${FIXTURE:-${FIXTURE_ROOT:-${scratch:-}}}}}/output"; cat() { return 7; }; exit 23'; expected=23 ;;
        *) printf 'Unknown retention probe: %s\n' "$failure" >&2; return 2 ;;
    esac
    local directory="$probe_root/exit-source/scripts/ci"
    local exit_path="$probe_root/$name-$failure.exit"
    mkdir -p "$directory" || return
    cp "$source_root/scripts/ci/fixture-lifecycle.sh" "$directory/" || return
    cp "$source_root/scripts/ci/fixture-retention-probes.sh" "$directory/" || return
    RETENTION_INJECTION="$injection" awk '
        { print }
        /^ROOT=".*%\/\.\}"$/ {
            print "ROOT=\"$RETENTION_SOURCE_ROOT\""
        }
        /^trap .* EXIT$/ && !injected {
            print "printf \"%s\\n\" \"${fixture:-${TEST_ROOT:-${FIXTURE:-${FIXTURE_ROOT:-${scratch:-}}}}}\" > \"$RETENTION_EXIT_PATH\""
            print "printf \"retained evidence\\n\" > \"${fixture:-${TEST_ROOT:-${FIXTURE:-${FIXTURE_ROOT:-${scratch:-}}}}}/exit-evidence\""
            print ENVIRON["RETENTION_INJECTION"]
            print "exit 99"
            injected=1
        }
        END { if (!injected) exit 1 }
    ' "$source" > "$directory/probe.sh" || return
    # A disposable source owns its root; inherited aliases must not select it.
    env -u fixture -u TEST_ROOT -u FIXTURE -u FIXTURE_ROOT -u scratch \
        RETENTION_SOURCE_ROOT="$source_root" RETENTION_EXIT_PATH="$exit_path" \
        TMPDIR="$probe_root" "$BASH" "$directory/probe.sh" "$@" \
        > "$probe_root/$name-$failure.log" 2>&1 || status=$?
    [[ "$status" == "$expected" ]] || return 1
    retained="$(cat "$exit_path")" || return
    [[ -n "$retained" ]] || return 1
    if [[ "$expected" == 0 ]]; then
        [[ ! -e "$retained" ]] || return 1
    else
        [[ -d "$retained" && "$(cat "$retained/exit-evidence")" == 'retained evidence' ]] || return 1
    fi
}

fixture_probe_exits() {
    local failure
    for failure in nounset nounset-function command nonzero premature completed failed-completion; do
        fixture_probe_exit "$1" "$2" "$3" "$4" "$failure" "${@:5}" || return
    done
}
