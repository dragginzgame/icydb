#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/standard-release-entry.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/bin"
real_bash="$(command -v bash)"
printf '#!%s\n' "$real_bash" > "$fixture/bin/bash"
cat >> "$fixture/bin/bash" <<'STUB'
set -euo pipefail
printf '%s\n' "$*" >> "$EVENTS"
[[ "${FAIL_RUNNER:-0}" == 0 ]]
STUB
chmod +x "$fixture/bin/bash"
if [[ -f "$root/tool-versions.env" ]]; then cp "$root/tool-versions.env" "$fixture/"; fi
export EVENTS="$fixture/events"
cd "$fixture"
real_make="$(command -v make)"
# A bare Make invocation shows the maintained release commands without effects.
: > "$EVENTS"
PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" > "$fixture/help"
for command in release-patch release-minor release-major release-resume; do
    grep -Fq "$command" "$fixture/help"
done
[[ ! -s "$EVENTS" ]]
for kind in patch minor major; do
    for fail in 0 1; do
        : > "$EVENTS"
        status=0
        PATH="$fixture/bin:$PATH" FAIL_RUNNER="$fail" "$real_make" --no-print-directory \
            -f "$root/Makefile" "release-$kind" RELEASE_REMOTE=review RELEASE_BRANCH=release-review \
            > "$fixture/output" 2>&1 || status=$?
        if [[ "$fail" == 0 ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
        printf '%s\n' "scripts/ci/run-release.sh $kind review release-review" > "$fixture/expected"
        cmp "$fixture/expected" "$EVENTS"
    done
done
: > "$EVENTS"
PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" \
    release-resume VERSION=0.1.1 RELEASE_REMOTE=review RELEASE_BRANCH=release-review > "$fixture/output" 2>&1
printf '%s\n' 'scripts/ci/run-release.sh resume 0.1.1 review release-review' > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
: > "$EVENTS"
if PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" \
    release-patch release-minor > "$fixture/output" 2>&1; then exit 1; fi
[[ ! -s "$EVENTS" ]]
echo 'standard release Make adapters passed (command stubs; no Git effects)'
