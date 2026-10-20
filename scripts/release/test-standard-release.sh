#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/standard-release-entry.XXXXXX")"
# Surface inner Make diagnostics before removing disposable fixture inputs.
# Preserve incomplete evidence even when Bash 3.2 reports a zero exit status.
fixture_complete=false
finish() {
    local status=$?
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" != 0 ]]; then
        if [[ -f "$fixture/output" ]]; then cat "$fixture/output" >&2 || true; fi
        printf "Standard release fixture retained: %s\n" "$fixture" >&2
    else
        rm -rf "$fixture"
    fi
    exit "$status"
}
trap finish EXIT
[[ "$fixture" == /* ]] || fixture="$PWD/$fixture"
fixture="$(cd -P "$fixture" && printf '%s/.' "$PWD")"
fixture="${fixture%/.}"
export TMPDIR="${fixture%/*}"
release_inputs=(scripts/ci/actionlint-checksums.tsv make/consumer.mk make/tools.mk make/release.mk make/rust-format.mk make/execution.mk scripts/ci/check-make-execution.sh scripts/ci/run-formatting.sh ci/tool-versions.env)
bash "$root/scripts/ci/check-release-commands.sh" "$root" "${release_inputs[@]}"
# Exercise this consumer's actual Make adapters after the checker changes cwd.
mkdir "$fixture/relative-temp"
ln -s "$fixture/relative-temp" "$fixture/temp-alias"
for selected_tmp in relative-temp temp-alias; do
    (cd "$fixture"; TMPDIR="$selected_tmp" bash "$root/scripts/ci/check-release-commands.sh" \
        "$root" "${release_inputs[@]}") > "$fixture/$selected_tmp.log" 2>&1
    [[ -z "$(ls -A "$fixture/relative-temp")" ]] || exit 1
done
mkdir -p "$fixture/bin"
real_bash="$(command -v bash)"
export RELEASE_FIXTURE_BASH="$real_bash"
printf '#!%s\n' "$real_bash" > "$fixture/bin/bash"
cat >> "$fixture/bin/bash" <<'STUB'
set -euo pipefail
if [[ "$1" == */scripts/ci/check-make-execution.sh ]]; then
    exec "$RELEASE_FIXTURE_BASH" "$@"
fi
if [[ "$1" == scripts/ci/release-candidate-receipt.sh ]]; then
    [[ "${MAKEFLAGS:-}" =~ --jobserver-(auth|fds)=([0-9]+),([0-9]+) ]]
    reader="${BASH_REMATCH[2]}"; writer="${BASH_REMATCH[3]}"
    : <&"$reader"
    : >&"$writer"
    [[ "$CARGO_HOME" == "$EXPECTED_CARGO_HOME" ]]
fi
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
    for override in normal cleared replaced hidden; do
        : > "$EVENTS"
        status=0
        overrides=()
        case "$override" in
            cleared) overrides=(MAKEFLAGS=) ;;
            replaced) overrides=(MAKEFLAGS=--no-print-directory) ;;
            hidden) overrides=(MAKEFLAGS= MFLAGS=) ;;
        esac
        "$real_make" --no-print-directory -f "$root/Makefile" "$flags" \
            release-patch ${overrides[@]+"${overrides[@]}"} > "$fixture/refused.log" 2>&1 || status=$?
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
printf '#!%s\n' "$real_bash" > "$fixture/refusal-bin/git"
cat >> "$fixture/refusal-bin/git" <<'GIT'
printf 'git called\n' >> "$EVENTS"
exit 97
GIT
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
export SELECTED_READY="$fixture/selected-ready"
export EXPECTED_CARGO_HOME="$root/.cache/cargo/icydb"
printf '#!%s\n' "$real_bash" > "$fixture/bin/cargo"
cat >> "$fixture/bin/cargo" <<'CARGO'
set -euo pipefail
[[ "${MAKEFLAGS:-}" =~ --jobserver-(auth|fds)=([0-9]+),([0-9]+) ]]
reader="${BASH_REMATCH[2]}"; writer="${BASH_REMATCH[3]}"
: <&"$reader"
: >&"$writer"
[[ "$CARGO_HOME" == "$EXPECTED_CARGO_HOME" ]]
printf 'cargo %s\n' "$*" >> "$EVENTS"
case "$*" in
    'fetch --locked')
        if [[ "${CARGO_NET_OFFLINE:-false}" == true ]]; then
            [[ -f "$CACHE_READY" ]]
        fi
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
case "$*" in
    '--no-print-directory install-tools')
        printf 'selected tools setup\n' >> "$EVENTS"
        [[ "${FAIL_SETUP:-0}" == 0 ]] || exit 23
        touch "$SELECTED_READY"
        ;;
    '--no-print-directory tools-check')
        [[ "${CARGO_NET_OFFLINE:-false}" == true ]]
        printf 'selected tools check\n' >> "$EVENTS"
        [[ "${FAIL_CHECK:-0}" == 0 && -f "$SELECTED_READY" ]] || exit 29
        ;;
    '--no-print-directory validate')
        [[ -f "$SELECTED_READY" ]]
        [[ "$CARGO_RELEASE_METADATA_EDITOR" == "$EDITOR_PATH" ]]
        [[ "$CARGO_RELEASE_METADATA_MAX_BYTES" == 8388608 ]]
        CARGO_HOME="$EXPECTED_CARGO_HOME" cargo metadata --locked --offline --no-deps --format-version 1
        ;;
    *) exec "$REAL_MAKE" -f "$ROOT_MAKEFILE" "$@" ;;
esac
MAKE
chmod +x "$fixture/bin/cargo" "$fixture/bin/make"
export REAL_MAKE="$real_make"
export ROOT_MAKEFILE="$root/Makefile"
# A failed editor admission cannot dispatch a release consumer even when the
# producer emits a complete executable path. Parent-shell selections cannot
# override the Make-owned editor and input allowance.
printf '#!%s\n' "$real_bash" > "$fixture/bin/editor-selector"
cat >> "$fixture/bin/editor-selector" <<'EDITOR'
set -euo pipefail
[[ "$#" == 0 ]]
printf '%s\n' "$EDITOR_PATH"
[[ "${FAIL_EDITOR:-0}" == 0 && -f "$SELECTED_READY" ]]
EDITOR
chmod +x "$fixture/bin/editor-selector"
export EDITOR_PATH="$fixture/bin/bash"
export CARGO_RELEASE_METADATA_EDITOR=stale-relative-editor
export CARGO_RELEASE_METADATA_MAX_BYTES=0
: > "$EVENTS"
if FAIL_EDITOR=1 PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" \
    release-prepare-version "RELEASE_EDITOR_CHECK=$fixture/bin/editor-selector" \
    > "$fixture/editor-refused.log" 2>&1; then
    echo "release consumer admitted a failed editor observation" >&2; exit 1
fi
[[ ! -s "$EVENTS" ]]
for stage in success fetch setup check; do
    : > "$EVENTS"
    rm -f "$CACHE_READY" "$SELECTED_READY"
    status=0
    fail_fetch=0 fail_setup=0 fail_check=0
    case "$stage" in fetch) fail_fetch=1 ;; setup) fail_setup=1 ;; check) fail_check=1 ;; esac
    CARGO_NET_OFFLINE=false PATH="$fixture/bin:$PATH" FAIL_FETCH="$fail_fetch" FAIL_SETUP="$fail_setup" FAIL_CHECK="$fail_check" "$real_make" --no-print-directory -j4 -f "$root/Makefile" \
        release-preflight "MAKE=$fixture/bin/make" \
        RELEASE_SOURCE=1111111111111111111111111111111111111111 \
        RELEASE_VERSION=0.1.1 RELEASE_DATE=2026-10-05 \
        "RELEASE_TMP_DIR=$fixture/release-tmp" > "$fixture/output" 2>&1 || status=$?
    if [[ "$status" == 0 ]]; then
        CARGO_NET_OFFLINE=true PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -j4 -f "$root/Makefile" \
            release-verify "MAKE=$fixture/bin/make" "RELEASE_TMP_DIR=$fixture/release-tmp" \
            "RELEASE_EDITOR_CHECK=$fixture/bin/editor-selector" \
            >> "$fixture/output" 2>&1 || status=$?
    fi
    printf '%s\n' 'scripts/ci/release-candidate-receipt.sh verify-tested-tree 1111111111111111111111111111111111111111' \
        'cargo fetch --locked' > "$fixture/expected"
    if [[ "$stage" != fetch ]]; then printf 'selected tools setup\n' >> "$fixture/expected"; fi
    if [[ "$stage" == success || "$stage" == check ]]; then printf 'selected tools check\n' >> "$fixture/expected"; fi
    if [[ "$stage" == success ]]; then
        [[ "$status" == 0 ]]
        printf '%s\n' 'cargo metadata --locked --offline --no-deps --format-version 1' >> "$fixture/expected"
    else
        [[ "$status" != 0 ]]
    fi
    cmp "$fixture/expected" "$EVENTS"
done
# Explicit offline preflight admits an existing selection without provisioning.
# Missing selected tools refuse before validation even under parallel Make.
for ready in prepared missing; do
    : > "$EVENTS"
    touch "$CACHE_READY"
    rm -f "$SELECTED_READY"
    if [[ "$ready" == prepared ]]; then touch "$SELECTED_READY"; fi
    status=0
    CARGO_NET_OFFLINE=true PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -j4 -f "$root/Makefile" \
        release-preflight "MAKE=$fixture/bin/make" \
        RELEASE_SOURCE=1111111111111111111111111111111111111111 \
        RELEASE_VERSION=0.1.1 RELEASE_DATE=2026-10-05 \
        "RELEASE_TMP_DIR=$fixture/release-tmp" > "$fixture/output" 2>&1 || status=$?
    printf '%s\n' 'scripts/ci/release-candidate-receipt.sh verify-tested-tree 1111111111111111111111111111111111111111' \
        'cargo fetch --locked' 'selected tools check' > "$fixture/expected"
    cmp "$fixture/expected" "$EVENTS"
    if [[ "$ready" == prepared ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
done
# Saved-source admission selects the archived lock even when working metadata
# selects a newer editor. Failed Git/tool producers cannot supply a path.
mkdir -p "$fixture/selection/scripts/release" "$fixture/selection/scripts/dev" \
    "$fixture/selection/bin" "$fixture/selection-tmp"
cp "$root/scripts/release/select-editor.sh" "$fixture/selection/scripts/release/"
printf '[[package]]\nname = "ic-build"\nversion = "0.19.0"\n' > "$fixture/saved-lock"
printf '[[package]]\nname = "ic-build"\nversion = "0.21.0"\n' > "$fixture/selection/Cargo.lock"
printf '#!%s\n' "$real_bash" > "$fixture/selection/bin/git"
cat >> "$fixture/selection/bin/git" <<'GIT'
set -euo pipefail
[[ "$1" == -C && "$3" == show && "$4" == 1111111111111111111111111111111111111111:Cargo.lock ]]
cat "$SAVED_LOCK"
[[ "${FAIL_SOURCE:-0}" == 0 ]]
GIT
printf '#!%s\n' "$real_bash" > "$fixture/selection/scripts/dev/install-rust-tools.sh"
cat >> "$fixture/selection/scripts/dev/install-rust-tools.sh" <<'INSTALL'
set -euo pipefail
[[ "$#" == 11 && "$1" == --consumer && "$3 $4" == '--package ic-build' && "$5" == --lockfile ]]
[[ "$7 $8 $9 ${10} ${11}" == '--bin ic-build-release-metadata --profile debug --check' ]]
cmp "$SAVED_LOCK" "$6"
printf 'editor check\n' >> "$EVENTS"
printf '%s\n' "$EDITOR_PATH"
[[ "${FAIL_ADMIT:-0}" == 0 ]]
INSTALL
chmod +x "$fixture/selection/bin/git"
export SAVED_LOCK="$fixture/saved-lock"
for producer in success source editor; do
    : > "$EVENTS"
    status=0
    fail_source=0 fail_admit=0
    case "$producer" in source) fail_source=1 ;; editor) fail_admit=1 ;; esac
    TMPDIR="$fixture/selection-tmp" PATH="$fixture/selection/bin:$PATH" \
        FAIL_SOURCE="$fail_source" FAIL_ADMIT="$fail_admit" \
        "$real_bash" "$fixture/selection/scripts/release/select-editor.sh" \
        1111111111111111111111111111111111111111 \
        > "$fixture/editor-selection" 2> "$fixture/editor-selection-errors" || status=$?
    if [[ "$producer" == success ]]; then
        [[ "$status" == 0 && "$(cat "$fixture/editor-selection")" == "$EDITOR_PATH" ]]
        [[ -z "$(ls -A "$fixture/selection-tmp")" ]]
    else
        [[ "$status" != 0 && ! -s "$fixture/editor-selection" ]]
        [[ -n "$(ls -A "$fixture/selection-tmp")" ]]
    fi
    if [[ "$producer" == source ]]; then [[ ! -s "$EVENTS" ]]
    else [[ "$(cat "$EVENTS")" == 'editor check' ]]; fi
done
printf '[OK] Shared release routing, admission and selected-tool preparation ordering passed\n'
fixture_complete=true
