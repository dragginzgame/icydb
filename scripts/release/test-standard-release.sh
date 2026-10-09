#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
bash "$root/scripts/ci/check-release-commands.sh" "$root" scripts/ci/actionlint-checksums.tsv make/tools.mk make/release.mk make/rust-format.mk ci/tool-versions.env
fixture="$(mktemp -d "${TMPDIR:-/tmp}/standard-release-entry.XXXXXX")"
# Surface inner Make diagnostics before removing disposable fixture inputs.
trap '
    status=$?
    if [[ "$status" != 0 && -f "$fixture/output" ]]; then
        cat "$fixture/output" >&2 || true
    fi
    rm -rf "$fixture"
    exit "$status"
' EXIT
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
# The common owner qualifies explicit remote/branch overrides. Exercise the
# consumer's default routing and runner failure through the actual includes too.
for kind in patch minor major resume; do
    for failure in 0 1; do
        : > "$EVENTS"
        status=0
        (unset RELEASE_REMOTE RELEASE_BRANCH
         PATH="$fixture/bin:$PATH" FAIL_RUNNER="$failure" "$real_make" --no-print-directory \
             -f "$root/Makefile" "release-$kind" VERSION=0.269.1) \
             > "$fixture/default-$kind-$failure.log" 2>&1 || status=$?
        if [[ "$failure" == 0 ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
        if [[ "$kind" == resume ]]; then
            printf '%s resume 0.269.1 origin main\n' "$root/scripts/ci/run-release.sh" > "$fixture/expected"
        else
            printf '%s %s origin main\n' "$root/scripts/ci/run-release.sh" "$kind" > "$fixture/expected"
        fi
        cmp "$fixture/expected" "$EVENTS"
    done
done
# The consumer parse boundary rejects unsafe outer Make modes even when an
# invocation overrides MAKEFLAGS. The substitute runner must never be called.
for flags in -i -n -q -t --ignore-errors --just-print --question --touch -in; do
    for override in normal override; do
        : > "$EVENTS"
        status=0
        if [[ "$override" == override ]]; then
            "$real_make" --no-print-directory -f "$root/Makefile" "$flags" \
                release-patch MAKEFLAGS= > "$fixture/refused.log" 2>&1 || status=$?
        else
            "$real_make" --no-print-directory -f "$root/Makefile" "$flags" \
                release-patch > "$fixture/refused.log" 2>&1 || status=$?
        fi
        [[ "$status" -ne 0 && ! -s "$EVENTS" ]]
    done
done
for flags in i n q t; do
    : > "$EVENTS"
    status=0
    MAKEFLAGS="$flags" "$real_make" --no-print-directory -f "$root/Makefile" \
        release-patch > "$fixture/refused.log" 2>&1 || status=$?
    [[ "$status" -ne 0 && ! -s "$EVENTS" ]]
done
# Direct runner admission also precedes all Git observation/mutation, including
# version-only modes which never read a consumer Makefile. Git is a strict
# substitute, so an admission regression cannot perform a release effect.
mkdir "$fixture/refusal-bin"
printf '#!%s\nprintf "git called\\n" >> "$EVENTS"\nexit 97\n' "$real_bash" \
    > "$fixture/refusal-bin/git"
chmod +x "$fixture/refusal-bin/git"
for kind in patch minor major resume; do
    args=("$kind")
    if [[ "$kind" == resume ]]; then args+=(0.266.1); fi
    for flags in i n q t v --ignore-errors --just-print --question --touch --version; do
        : > "$EVENTS"
        status=0
        MAKEFLAGS="$flags" PATH="$fixture/refusal-bin:$PATH" \
            "$real_bash" "$root/scripts/ci/run-release.sh" "${args[@]}" origin main \
            > "$fixture/refused.log" 2>&1 || status=$?
        [[ "$status" -ne 0 && ! -s "$EVENTS" ]]
    done
done
# Real preflight must fill its selected cache before the offline validation gate.
# Simulate a cold cache without registry requests or dependency changes.
mkdir -p scripts/ci
cp "$root/scripts/ci/finalize-release-changelog.awk" scripts/ci/
printf '# Changelog\n\n## [0.1.1]\n\nFixture notes.\n' > CHANGELOG.md
export CACHE_READY="$fixture/cache-ready"
export EXPECTED_CARGO_HOME="$root/.cache/cargo/icydb"
printf '#!%s\n' "$real_bash" > "$fixture/bin/cargo"
cat >> "$fixture/bin/cargo" <<'CARGO'
set -euo pipefail
[[ "$CARGO_HOME" == "$EXPECTED_CARGO_HOME" ]]
printf 'cargo %s\n' "$*" >> "$EVENTS"
case "$*" in
    'fetch --locked')
        [[ "${CARGO_NET_OFFLINE:-false}" != true ]]
        [[ "${FAIL_FETCH:-0}" == 0 ]] || exit 1
        touch "$CACHE_READY"
        ;;
    'metadata --locked --offline --no-deps --format-version 1')
        [[ "${CARGO_NET_OFFLINE:-false}" == true && -f "$CACHE_READY" ]]
        ;;
    *) exit 99 ;;
esac
CARGO
printf '#!%s\n' "$real_bash" > "$fixture/bin/make"
cat >> "$fixture/bin/make" <<'MAKE'
set -euo pipefail
if [[ "$*" == '--no-print-directory validate' ]]; then
    CARGO_HOME="$EXPECTED_CARGO_HOME" cargo metadata --locked --offline --no-deps --format-version 1
else
    exec "$REAL_MAKE" -f "$ROOT_MAKEFILE" "$@"
fi
MAKE
chmod +x "$fixture/bin/cargo" "$fixture/bin/make"
export REAL_MAKE="$real_make"
export ROOT_MAKEFILE="$root/Makefile"
for fail in 0 1; do
    : > "$EVENTS"
    rm -f "$CACHE_READY"
    status=0
    # Only command stubs run here; the simulated preflight precedes offline validation.
    CARGO_NET_OFFLINE=false PATH="$fixture/bin:$PATH" FAIL_FETCH="$fail" "$real_make" --no-print-directory -f "$root/Makefile" \
        release-preflight release-verify "MAKE=$fixture/bin/make" \
        RELEASE_SOURCE=1111111111111111111111111111111111111111 \
        RELEASE_VERSION=0.1.1 RELEASE_DATE=2026-10-05 \
        "RELEASE_TMP_DIR=$fixture/release-tmp" > "$fixture/output" 2>&1 || status=$?
    printf '%s\n' 'scripts/ci/release-candidate-receipt.sh verify-tested-tree 1111111111111111111111111111111111111111' \
        'cargo fetch --locked' > "$fixture/expected"
    if [[ "$fail" == 0 ]]; then
        [[ "$status" == 0 ]]
        printf '%s\n' 'cargo metadata --locked --offline --no-deps --format-version 1' >> "$fixture/expected"
    else
        [[ "$status" != 0 && ! -e "$CACHE_READY" ]]
    fi
    cmp "$fixture/expected" "$EVENTS"
done
echo 'standard release Make adapters passed (command stubs; no Git effects)'
