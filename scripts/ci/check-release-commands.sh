#!/usr/bin/env bash
set -euo pipefail

# Execute a reviewed Makefile copy in an isolated fixture with a substitute runner.
# This is an adoption check, not a sandbox for arbitrary Makefile code.
if [[ $# -lt 1 ]]; then
    echo 'usage: check-release-commands.sh ROOT [RELATIVE_MAKE_INPUT ...]' >&2
    exit 2
fi
root="$(cd "$1" && pwd -P)"
shift
[[ -f "$root/Makefile" ]] || { echo "missing Makefile: $root" >&2; exit 2; }
for input in "$@"; do
    case "$input" in
        ''|/*|..|../*|*/../*|*/..|./*|*/./*|*/.|Makefile|scripts/ci/run-release.sh)
            echo "invalid fixture input: $input" >&2; exit 2 ;;
    esac
    [[ -f "$root/$input" ]] || { echo "missing fixture input: $input" >&2; exit 2; }
done
# Independent test checkout: never inherit parent release selections or identity.
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES
unset VALIDATION_REPOSITORY_ROOT VALIDATION_RUNNER_SNAPSHOT_PATH
fixture="$(mktemp -d "${TMPDIR:-/tmp}/release-commands.XXXXXX")"
finish() {
    local status=$?
    if [[ "$status" == 0 ]]; then
        rm -rf "$fixture"
    else
        echo "Release command check failed; fixture and logs retained: $fixture" >&2
    fi
}
trap finish EXIT
cp "$root/Makefile" "$fixture/Makefile"
for input in "$@"; do
    mkdir -p "$fixture/$(dirname "$input")"
    cp "$root/$input" "$fixture/$input"
done
mkdir -p "$fixture/scripts/ci"
cat > "$fixture/scripts/ci/run-release.sh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\0' "$@" >> "$RELEASE_COMMAND_EVENTS"
exit "$RELEASE_COMMAND_RESULT"
STUB
chmod +x "$fixture/scripts/ci/run-release.sh"
export RELEASE_COMMAND_EVENTS="$fixture/events"
cd "$fixture"
for kind in patch minor major resume; do
    for result in 0 17; do
        export RELEASE_COMMAND_RESULT="$result"
        : > "$RELEASE_COMMAND_EVENTS"
        status=0
        make --no-print-directory -f Makefile "release-$kind" VERSION=0.1.1 \
            RELEASE_REMOTE=review RELEASE_BRANCH=release-review \
            > "$kind-$result.log" 2>&1 || status=$?
        if [[ "$result" == 0 ]]; then
            [[ "$status" == 0 ]] || exit 1
        else
            [[ "$status" != 0 ]] || exit 1
        fi
        if [[ "$kind" == resume ]]; then
            printf '%s\0' resume 0.1.1 review release-review > expected
        else
            printf '%s\0' "$kind" review release-review > expected
        fi
        cmp expected "$RELEASE_COMMAND_EVENTS"
    done
done
export RELEASE_COMMAND_RESULT=0
for first in patch minor major resume; do
    for second in patch minor major resume; do
        [[ "$first" != "$second" ]] || continue
        : > "$RELEASE_COMMAND_EVENTS"
        if make --no-print-directory -f Makefile "release-$first" "release-$second" \
            VERSION=0.1.1 RELEASE_REMOTE=review RELEASE_BRANCH=release-review \
            > "conflict-$first-$second.log" 2>&1; then
            echo 'Makefile accepted conflicting release commands' >&2
            exit 1
        fi
        [[ ! -s "$RELEASE_COMMAND_EVENTS" ]] || exit 1
    done
done
echo 'Standard release Make adapters passed (substitute runner; no release effects)'
