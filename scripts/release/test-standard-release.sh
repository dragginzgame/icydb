#!/usr/bin/env bash
set -euo pipefail
# Disposable Make invocations own their flags and cache selections. In a real
# release gate, inherited command-line assignments otherwise override fixture
# environment values (and can select the caller's group instead of its ledger).
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS
unset RELEASE_GROUPS RELEASE_SETUP_MODE
export RELEASE_BATCH_ACTIVE=1 RELEASE_PUBLISH_TARGET=''
export RELEASE_GROUP=database
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$root/scripts/ci/fixture-lifecycle.sh"
fixture="$(fixture_create "${TMPDIR:-/tmp}/standard-release-entry.XXXXXX")"
# Surface inner Make diagnostics before removing disposable fixture inputs.
# Preserve incomplete evidence even when Bash 3.2 reports a zero exit status.
fixture_complete=false
finish() {
  local status=$?
  fixture_completion_status "$status" "$fixture_complete" || status=$?
  if [[ "$status" != 0 ]]; then fixture_replay_logs "$fixture/output"; fi
  fixture_finish "$status" "$fixture" "Standard release fixture retained"
}
trap finish EXIT
export TMPDIR="${fixture%/*}"
# The actual release prerequisites admit the current Git root before cleanup.
# Own that root here so dispatch controls cannot resolve the parent checkout.
git -C "$fixture" init --quiet
release_inputs=(scripts/release/group.sh scripts/release/verify.sh scripts/ci/release-receipts.sh ci/cargo-release-groups.json scripts/ci/cargo-release-groups.jq scripts/ci/actionlint-checksums.tsv make/consumer.mk make/tools.mk make/release.mk make/rust-format.mk make/execution.mk scripts/ci/check-make-execution.sh scripts/ci/run-formatting.sh ci/tool-versions.env)
# Dispatch qualification substitutes both native admission owners; their real
# metadata/snapshot admission is qualified separately against this checkout.
routing_source="$fixture/routing-source"
mkdir -p "$routing_source"
bash "$root/scripts/ci/prepare-script-fixture.sh" "$root" "$routing_source" Makefile "${release_inputs[@]}"
mkdir -p "$routing_source/scripts/ci"
printf '#!/usr/bin/env bash\nexit 0\n' > "$routing_source/scripts/ci/check-qualification-tooling.sh"
release_inputs+=(scripts/ci/check-qualification-tooling.sh)
bash "$root/scripts/ci/check-release-commands.sh" "$routing_source" "${release_inputs[@]}"
# Exercise this consumer's actual Make adapters after the checker changes cwd.
mkdir "$fixture/relative-temp"
ln -s "$fixture/relative-temp" "$fixture/temp-alias"
for selected_tmp in relative-temp temp-alias; do
    (cd "$fixture"; TMPDIR="$selected_tmp" bash "$root/scripts/ci/check-release-commands.sh" \
        "$routing_source" "${release_inputs[@]}") > "$fixture/$selected_tmp.log" 2>&1
    [[ -z "$(ls -A "$fixture/relative-temp")" ]] || exit 1
done
mkdir -p "$fixture/bin"
real_bash="$(command -v bash)"
export RELEASE_FIXTURE_BASH="$real_bash"
printf '#!%s\n' "$real_bash" > "$fixture/bin/bash"
cat >> "$fixture/bin/bash" <<'STUB'
set -euo pipefail
if [[ "$1" == */scripts/ci/check-make-execution.sh || "$1" == */scripts/release/group.sh || "$1" == scripts/release/verify.sh ]]; then
    exec "$RELEASE_FIXTURE_BASH" "$@"
fi
if [[ "$1" == */scripts/ci/check-qualification-tooling.sh || "$1" == scripts/ci/check-qualification-tooling.sh ]]; then
    exit 0
fi
if [[ "$1" == */scripts/dev/toko-tooling.sh ]]; then
    [[ "$#" == 6 && "$2 $3" == 'check --consumer' && "$4" == "$RELEASE_FIXTURE_ROOT" && "$5 $6" == '--manifest .toko-tooling.snapshot' ]]
    printf 'tooling admission\n' >> "$EVENTS"
    exit "${FAIL_TOOLING:-0}"
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
export RELEASE_FIXTURE_ROOT="$fixture"
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
        printf 'tooling admission\n' > "$fixture/expected"
        if [[ "$kind" == resume ]]; then
            printf '%s resume 0.269.1 origin main\n' "$root/scripts/ci/run-release.sh" >> "$fixture/expected"
        else
            printf '%s %s origin main\n' "$root/scripts/ci/run-release.sh" "$kind" >> "$fixture/expected"
        fi
        cmp "$fixture/expected" "$EVENTS"
    done
done
# Failed admission must stop the actual Make caller before runner dispatch.
: > "$EVENTS"
if PATH="$fixture/bin:$PATH" FAIL_TOOLING=31 "$real_make" --no-print-directory \
    -f "$root/Makefile" release-patch > "$fixture/admission-refused.log" 2>&1; then exit 1; fi
printf 'tooling admission\n' > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
# The adopted frozen entrypoint must keep IcyDB's real Make routing, selected
# roster and publication boundary. Substitute only the effectful batch runner;
# this proves dispatch, not frozen product builds or publication qualification.
export RELEASE_FIXTURE_PERL
RELEASE_FIXTURE_PERL="$(command -v perl)"
printf '#!%s\n' "$real_bash" > "$fixture/bin/perl"
cat >> "$fixture/bin/perl" <<'FROZEN'
set -euo pipefail
if [[ "$1" == */scripts/ci/run-release-batch.pl ]]; then
    printf '%s\n' "$*" >> "$EVENTS"
else
    exec "$RELEASE_FIXTURE_PERL" "$@"
fi
FROZEN
chmod +x "$fixture/bin/perl"
: > "$EVENTS"
env -u RELEASE_GROUPS -u RELEASE_BATCH_ACTIVE PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory \
    -f "$root/Makefile" release-patch RELEASE_GROUP=database RELEASE_REMOTE=reviewed \
    RELEASE_BRANCH=main RELEASE_DELIVERY=direct > "$fixture/frozen-default.log" 2>&1
printf 'tooling admission\n%s --root %s --groups database --kind patch --remote reviewed --branch main --delivery direct --setup-target release-candidate-setup\n' \
    "$root/scripts/ci/run-release-batch.pl" "$fixture" > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
for groups in database cli database,cli; do
    for kind in patch minor major; do
        : > "$EVENTS"
        PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" \
            "release-$kind" RELEASE_GROUP="${groups%%,*}" RELEASE_GROUPS="$groups" RELEASE_BATCH_ACTIVE=0 \
            RELEASE_PUBLISH_TARGET=publish RELEASE_REMOTE=reviewed RELEASE_BRANCH=main \
            RELEASE_DELIVERY=direct > "$fixture/frozen-$groups-$kind.log" 2>&1
        printf 'tooling admission\n%s --root %s --groups %s --kind %s --remote reviewed --branch main --delivery direct --publish-target publish --setup-target release-candidate-setup\n' \
            "$root/scripts/ci/run-release-batch.pl" "$fixture" "$groups" "$kind" > "$fixture/expected"
        cmp "$fixture/expected" "$EVENTS"
    done
done
: > "$EVENTS"
PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" release-patch \
    RELEASE_GROUP=cli RELEASE_GROUPS=cli RELEASE_BATCH_ACTIVE=0 RELEASE_PUBLISH_TARGET= \
    RELEASE_REMOTE=reviewed RELEASE_BRANCH=main RELEASE_DELIVERY=direct > "$fixture/frozen-git-only.log" 2>&1
printf 'tooling admission\n%s --root %s --groups cli --kind patch --remote reviewed --branch main --delivery direct --setup-target release-candidate-setup\n' \
    "$root/scripts/ci/run-release-batch.pl" "$fixture" > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
: > "$EVENTS"
PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" release-patch \
    RELEASE_GROUP=database RELEASE_GROUPS=database,cli RELEASE_BATCH_ACTIVE=1 \
    RELEASE_REMOTE=reviewed RELEASE_BRANCH=main > "$fixture/frozen-child.log" 2>&1
printf 'tooling admission\n%s patch reviewed main\n' "$root/scripts/ci/run-release.sh" > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
# Shared owns saved-batch operations; they need no new group selection.
for operation in status resume reconcile retire; do
    : > "$EVENTS"
    env -u RELEASE_GROUP PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory \
        -f "$root/Makefile" "release-batch-$operation" BATCH=batch_reviewed \
        RELEASE_DELIVERY=direct > "$fixture/batch-$operation.log" 2>&1
    printf '%s --root %s --%s batch_reviewed --delivery direct\n' \
        "$root/scripts/ci/run-release-batch.pl" "$fixture" "$operation" > "$fixture/expected"
    cmp "$fixture/expected" "$EVENTS"
done
# A selected group cannot opt out of capture; retired settings refuse before
# the effectful runner is called. These are explicit Shared boundary diagnostics.
: > "$EVENTS"
if PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" \
    release-patch RELEASE_GROUPS= RELEASE_BATCH_ACTIVE=0 > "$fixture/empty-roster.log" 2>&1; then exit 1; fi
[[ ! -s "$EVENTS" ]]
for retired in RELEASE_FROZEN_GROUPS RELEASE_FROZEN_PUBLISH_TARGET RELEASE_FROZEN_SETUP_TARGET RELEASE_FROZEN_SETUP_MODE FROZEN_RELEASE_ACTIVE; do
    if PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -f "$root/Makefile" \
        release-patch "$retired=" > "$fixture/retired-setting.log" 2>&1; then exit 1; fi
    [[ ! -s "$EVENTS" ]]
done
# Exercise routine qualification through the actual Make caller with script
# substitutes. Keep snapshot admission and consumer release proofs selected;
# a failed proof must stop dispatch before later fixtures. No gate runs here.
mkdir "$fixture/roster-bin"
printf '#!%s\n' "$real_bash" > "$fixture/roster-bin/bash"
cat >> "$fixture/roster-bin/bash" <<'ROSTER'
set -euo pipefail
if [[ "$1" == */scripts/ci/check-make-execution.sh || "$1" == */scripts/release/group.sh ]]; then
    exec "$RELEASE_FIXTURE_BASH" "$@"
fi
script="${1#"$ROSTER_ROOT/"}"
[[ -f "$ROSTER_ROOT/$script" ]]
printf '%s\n' "$script" >> "$EVENTS"
[[ "$script" != "$ROSTER_FAILURE" ]]
ROSTER
cp "$fixture/roster-bin/bash" "$fixture/roster-bin/perl"
chmod +x "$fixture/roster-bin/"{bash,perl}
for failure in none scripts/release/test-release-groups.sh; do
    : > "$EVENTS"
    status=0
    PATH="$fixture/roster-bin:$PATH" ROSTER_ROOT="$root" ROSTER_FAILURE="$failure" \
        "$real_make" --no-print-directory -C "$root" -f "$root/Makefile" check-portable-automation \
        RELEASE_EDITOR_ENV= > "$fixture/roster-${failure##*/}.log" 2>&1 || status=$?
    [[ "$(head -n 1 "$EVENTS")" == scripts/ci/check-qualification-tooling.sh ]]
    [[ "$(sed -n '2p' "$EVENTS")" == scripts/dev/toko-tooling.sh ]]
    for proof in scripts/ci/test-toko-tooling-adapters.sh scripts/release/test-standard-release.sh \
        scripts/release/test-release-groups.sh; do
        [[ "$(grep -Fxc "$proof" "$EVENTS")" == 1 ]]
    done
    if [[ "$failure" == none ]]; then
        [[ "$status" == 0 ]]
        for proof in scripts/ci/test-tooling-fixture-completion.sh \
            scripts/ci/test-publish-workspace.sh; do
            [[ "$(grep -Fxc "$proof" "$EVENTS")" == 1 ]]
        done
    else
        [[ "$status" != 0 && "$(tail -n 1 "$EVENTS")" == "$failure" ]]
    fi
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
mkdir -p scripts/release ci
cp "$root/scripts/release/verify.sh" "$root/scripts/release/group.sh" scripts/release/
cp "$root/scripts/ci/release-receipts.sh" "$root/scripts/ci/cargo-release-groups.jq" scripts/ci/
cp "$root/ci/cargo-release-groups.json" ci/
cp "$root/scripts/ci/finalize-release-changelog.awk" scripts/ci/
mkdir -p releases/database
printf '# Changelog\n\n## [0.1.1]\n\nFixture notes.\n' > releases/database/CHANGELOG.md
export CACHE_READY="$fixture/cache-ready"
export FETCH_EVENTS="$fixture/fetch-events"
export SELECTED_READY="$fixture/selected-ready"
export EXPECTED_CARGO_HOME="$root/.cache/cargo/icydb"
export EXPECTED_QUALIFICATION_MANIFEST="$root/qualification/Cargo.toml"
printf '#!%s\n' "$real_bash" > "$fixture/bin/cargo"
cat >> "$fixture/bin/cargo" <<'CARGO'
set -euo pipefail
[[ "${MAKEFLAGS:-}" =~ --jobserver-(auth|fds)=([0-9]+),([0-9]+) ]]
reader="${BASH_REMATCH[2]}"; writer="${BASH_REMATCH[3]}"
: <&"$reader"
: >&"$writer"
[[ "$CARGO_HOME" == "$EXPECTED_CARGO_HOME" ]]
case "$*" in
    'fetch --locked'|"fetch --manifest-path $EXPECTED_QUALIFICATION_MANIFEST --locked")
        printf '%s\n' "$*" >> "$FETCH_EVENTS"
        if [[ "${CARGO_NET_OFFLINE:-false}" == true ]]; then
            [[ -f "$CACHE_READY" ]]
        fi
        [[ "${FAIL_FETCH:-0}" == 0 ]] || exit 1
        touch "$CACHE_READY"
        ;;
    'metadata --locked --offline --no-deps --format-version 1')
        printf 'cargo %s\n' "$*" >> "$EVENTS"
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
        [[ -f "$CACHE_READY" ]]
        printf 'selected tools setup\n' >> "$EVENTS"
        [[ "${FAIL_SETUP:-0}" == 0 ]] || exit 23
        touch "$SELECTED_READY"
        ;;
    '--no-print-directory tools-check')
        [[ "${CARGO_NET_OFFLINE:-false}" == true && -f "$CACHE_READY" ]]
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
# Product preparation selects both native locks. The substitute rejects the
# independent Canic graph and every other Cargo transport.
assert_fetches() {
    printf '%s\n' 'fetch --locked' "fetch --manifest-path $EXPECTED_QUALIFICATION_MANIFEST --locked" \
        | LC_ALL=C sort > "$fixture/expected-fetches"
    LC_ALL=C sort "$FETCH_EVENTS" > "$fixture/actual-fetches"
    cmp "$fixture/expected-fetches" "$fixture/actual-fetches"
}
for stage in success fetch setup check; do
    : > "$EVENTS"
    : > "$FETCH_EVENTS"
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
            release-verify RELEASE_SOURCE=1111111111111111111111111111111111111111 \
            "RELEASE_RECEIPT_DIR=$fixture/receipts" \
            "MAKE=$fixture/bin/make" "RELEASE_TMP_DIR=$fixture/release-tmp" \
            "RELEASE_EDITOR_CHECK=$fixture/bin/editor-selector" \
            >> "$fixture/output" 2>&1 || status=$?
    fi
    printf '%s\n' 'tooling admission' 'scripts/ci/release-candidate-receipt.sh verify-tested-tree 1111111111111111111111111111111111111111' \
        > "$fixture/expected"
    if [[ "$stage" == fetch ]]; then
        [[ -s "$FETCH_EVENTS" && ! -e "$CACHE_READY" ]]
        while IFS= read -r fetch; do
            [[ "$fetch" == 'fetch --locked' ]]
        done < "$FETCH_EVENTS"
    else
        assert_fetches
    fi
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
    : > "$FETCH_EVENTS"
    touch "$CACHE_READY"
    rm -f "$SELECTED_READY"
    if [[ "$ready" == prepared ]]; then touch "$SELECTED_READY"; fi
    status=0
    CARGO_NET_OFFLINE=true PATH="$fixture/bin:$PATH" "$real_make" --no-print-directory -j4 -f "$root/Makefile" \
        release-preflight "MAKE=$fixture/bin/make" \
        RELEASE_SOURCE=1111111111111111111111111111111111111111 \
        RELEASE_VERSION=0.1.1 RELEASE_DATE=2026-10-05 \
        "RELEASE_TMP_DIR=$fixture/release-tmp" > "$fixture/output" 2>&1 || status=$?
    printf '%s\n' 'tooling admission' 'scripts/ci/release-candidate-receipt.sh verify-tested-tree 1111111111111111111111111111111111111111' \
        'selected tools check' > "$fixture/expected"
    assert_fetches
    cmp "$fixture/expected" "$EVENTS"
    if [[ "$ready" == prepared ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
done
# Saved-source admission selects the archived lock even when working metadata
# selects a newer editor. Failed Git/tool producers cannot supply a path.
mkdir -p "$fixture/selection/scripts/release" "$fixture/selection/scripts/dev" "$fixture/selection/scripts/ci" \
    "$fixture/selection/bin" "$fixture/selection-tmp"
cp "$root/scripts/release/select-editor.sh" "$fixture/selection/scripts/release/"
cp "$root/scripts/ci/compare-stable-versions.pl" "$root/scripts/ci/StableVersion.pm" "$fixture/selection/scripts/ci/"
printf '[[package]]\nname = "ic-build"\nversion = "0.21.0"\n' > "$fixture/selection/Cargo.lock"
printf '#!%s\n' "$real_bash" > "$fixture/selection/bin/git"
cat >> "$fixture/selection/bin/git" <<'GIT'
set -euo pipefail
[[ "$1" == -C && "$3" == show ]]
case "$4" in
    1111111111111111111111111111111111111111:ci/cargo-release-groups.json)
        if [[ "${SAVED_QUALIFICATION:-0}" == 1 ]]; then printf '{"qualification_workspace":"qualification"}\n'
        else printf '{}\n'; fi
        exit 0 ;;
    1111111111111111111111111111111111111111:qualification/Cargo.lock) [[ "${SAVED_QUALIFICATION:-0}" == 1 ]] ;;
    1111111111111111111111111111111111111111:Cargo.lock) [[ "${SAVED_QUALIFICATION:-0}" == 0 ]] ;;
    *) exit 99 ;;
esac
cat "$SAVED_LOCK"
[[ "${FAIL_SOURCE:-0}" == 0 ]]
GIT
printf '#!%s\n' "$real_bash" > "$fixture/selection/scripts/dev/install-rust-tools.sh"
cat >> "$fixture/selection/scripts/dev/install-rust-tools.sh" <<'INSTALL'
set -euo pipefail
[[ "$1" == --consumer && "$3 $4" == '--package ic-build' && "$5" == --lockfile ]]
if [[ "$EXPECT_EDITOR_CLI" == 1 ]]; then
    [[ "$#" == 13 && "$7 $8 $9 ${10} ${11} ${12} ${13}" == '--bin ic-build-release-metadata --profile debug --features cli --check' ]]
else
    [[ "$#" == 11 && "$7 $8 $9 ${10} ${11}" == '--bin ic-build-release-metadata --profile debug --check' ]]
fi
cmp "$SAVED_LOCK" "$6"
printf 'editor check\n' >> "$EVENTS"
printf '%s\n' "$EDITOR_PATH"
[[ "${FAIL_ADMIT:-0}" == 0 ]]
INSTALL
chmod +x "$fixture/selection/bin/git"
export SAVED_LOCK="$fixture/saved-lock"
for saved_version in 0.19.0 0.63.0 0.81.1; do
    export SAVED_QUALIFICATION=0
    if [[ "$saved_version" == 0.81.1 ]]; then export SAVED_QUALIFICATION=1; fi
    attempts="$fixture/selection-tmp/$saved_version"
    mkdir "$attempts"
    printf '[[package]]\nname = "ic-build"\nversion = "%s"\n' "$saved_version" > "$SAVED_LOCK"
    export EXPECT_EDITOR_CLI=0
    if [[ "$saved_version" != 0.19.0 ]]; then export EXPECT_EDITOR_CLI=1; fi
    for producer in success source editor; do
        : > "$EVENTS"
        status=0
        fail_source=0 fail_admit=0
        case "$producer" in source) fail_source=1 ;; editor) fail_admit=1 ;; esac
        TMPDIR="$attempts" PATH="$fixture/selection/bin:$PATH" \
            FAIL_SOURCE="$fail_source" FAIL_ADMIT="$fail_admit" \
            "$real_bash" "$fixture/selection/scripts/release/select-editor.sh" \
            1111111111111111111111111111111111111111 \
            > "$fixture/editor-selection" 2> "$fixture/editor-selection-errors" || status=$?
        if [[ "$producer" == success ]]; then
            [[ "$status" == 0 && "$(cat "$fixture/editor-selection")" == "$EDITOR_PATH" ]]
            [[ -z "$(ls -A "$attempts")" ]]
        else
            [[ "$status" != 0 && ! -s "$fixture/editor-selection" ]]
            [[ -n "$(ls -A "$attempts")" ]]
        fi
        if [[ "$producer" == source ]]; then [[ ! -s "$EVENTS" ]]
        else [[ "$(cat "$EVENTS")" == 'editor check' ]]; fi
    done
done
printf '[OK] Shared release routing, admission and selected-tool preparation ordering passed\n'
fixture_complete=true
