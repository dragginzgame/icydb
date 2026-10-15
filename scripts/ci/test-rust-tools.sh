#!/usr/bin/env bash
# Shared companions: scripts/dev/install-rust-tools.sh scripts/dev/cargo-tool-build.sh scripts/dev/tool-store.sh scripts/ci/cargo-tool-cache-key.sh scripts/ci/select-tool-evidence.sh scripts/ci/archive-evidence.sh .githooks/pre-commit
set -euo pipefail
path_context=false
if [[ "${1:-}" == --path-context && $# == 1 ]]; then path_context=true;
elif [[ $# != 0 ]]; then echo 'usage: fixture [--path-context]' >&2; exit 2; fi
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"; ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/rust-tools-test.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
fixture_complete=false
finish() {
    local status=$?
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
    if [[ -s "$fixture/forbidden-git" ]]; then
        cat "$fixture/forbidden-git" >&2
        status=1
    fi
    if [[ "$status" == 0 ]]; then rm -rf "$fixture"
    else printf 'Failed Rust tool fixture retained: %s\n' "$fixture" >&2; fi
    exit "$status"
}
trap finish EXIT
export SHARED_TOOLING_STORE="$fixture/store"
mkdir "$fixture/templates"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
mkdir -p "$fixture/compiler/bin" "$fixture/assets" "$fixture/a" "$fixture/b"
# Always exercise BSD-style padded counts, including on Linux.
mkdir "$fixture/commands"
printf '#!%s\nfixture=%q\nreal_git=%q\n' "$BASH" "$fixture" "$(command -v git)" > "$fixture/commands/git"
cat >> "$fixture/commands/git" <<'STUB'
args=("$@")
while [[ $# -gt 0 ]]; do
    case "$1" in
        -C|-c|--git-dir|--work-tree|--namespace) shift 2 ;;
        -*) shift ;;
        *) break ;;
    esac
done
case "${1:-}" in
    commit|commit-tree|tag|push)
        printf 'forbidden fixture Git command: %s\n' "$1" >> "$fixture/forbidden-git"
        exit 79 ;;
esac
exec "$real_git" "${args[@]}"
STUB
printf '#!%s\nreal_wc=%q\n' "$BASH" "$(command -v wc)" > "$fixture/commands/wc"
cat >> "$fixture/commands/wc" <<'STUB'
if [[ "$*" == -l ]]; then
    count="$("$real_wc" "$@")" || exit $?
    printf '%8d\n' "$count"
else exec "$real_wc" "$@"; fi
STUB
for tool in ln perl; do
    printf '#!%s\nfixture=%q\nreal_tool=%q\n' "$BASH" "$fixture" "$(command -v "$tool")" > "$fixture/commands/$tool"
done
cat >> "$fixture/commands/ln" <<'STUB'
destination="${!#}"
# A regressed installer must never get as far as attempting a root write.
[[ "$destination" != /selected ]] || { echo 'blocked root activation' >&2; exit 79; }
if [[ -f "$fixture/fail-reference" && "$destination" == */rust-set.* ]]; then
    echo 'injected reference failure' >&2; exit 77
fi
exec "$real_tool" "$@"
STUB
cat >> "$fixture/commands/perl" <<'STUB'
if [[ "$1" == -e && "$2" == *'rename('* ]]; then
    destination="${!#}"
    for boundary in store local common; do
        [[ -f "$fixture/fail-$boundary-activation" ]] || continue
        case "$boundary:$destination" in
            store:*/current|local:*/rust/selected-*|common:*/rust/common)
                echo "injected $boundary activation failure" >&2; exit 78 ;;
        esac
    done
fi
exec "$real_tool" "$@"
STUB
printf '#!%s\nfixture=%q\nreal_tool=%q\n' "$BASH" "$fixture" "$(command -v uname)" > "$fixture/commands/uname"
cat >> "$fixture/commands/uname" <<'STUB'
"$real_tool" "$@" || exit $?
if [[ "$*" == -srm && -f "$fixture/changed-native" ]]; then cat "$fixture/changed-native"; fi
STUB
chmod +x "$fixture/commands/"*
export PATH="$fixture/commands:$PATH"
printf 'fixture rustc\n' > "$fixture/compiler-version"
printf '#!%s\n' "$BASH" > "$fixture/compiler/bin/rustc"
printf 'fixture=%q\n' "$fixture" >> "$fixture/compiler/bin/rustc"
cat >> "$fixture/compiler/bin/rustc" <<'STUB'
case "$*" in
    '--print sysroot')
        [[ ! -f rust-toolchain.toml ]] || { echo 'unprepared staged compiler' >&2; exit 77; }
        echo "$fixture/compiler" ;;
    *) cat "$fixture/compiler-version"; echo 'host: fixture-host' ;;
esac
STUB
printf '#!%s\nset -euo pipefail\n' "$BASH" > "$fixture/compiler/bin/cargo"
printf 'fixture=%q\njq_bin=%q\n' "$fixture" "$(command -v jq)" >> "$fixture/compiler/bin/cargo"
cat >> "$fixture/compiler/bin/cargo" <<'STUB'
case "$1" in -vV|--version) echo 'cargo fixture'; exit 0 ;; esac
[[ -z "${RUSTFLAGS+x}${RUSTC_WRAPPER+x}${CARGO_ENCODED_RUSTFLAGS+x}${UNMODELLED_SECRET+x}" ]] || exit 71
[[ "$CARGO_HOME" == "$fixture/store/cargo-cache" && "$RUSTC" == "$fixture/compiler/bin/rustc" ]] || exit 72
if [[ "$1" == info ]]; then
    if [[ -f "$fixture/no-info" ]]; then
        printf 'refused\n' >> "$fixture/info-refusals"
        echo 'injected Cargo source acquisition failure' >&2
        exit 91
    fi
    identity="${2/@/-}"
    mkdir -p "$CARGO_HOME/registry/cache/index.crates.io-fixture"
    cp "$fixture/assets/$identity.crate" "$CARGO_HOME/registry/cache/index.crates.io-fixture/"
    exit 0
fi
[[ "$1" == install ]] || exit 73
package="$2"; shift 2
version='' kind='' target='' destination='' profile=release locked=false registry='' jobs=''
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version) version="${2#=}"; shift 2 ;;
        --root) destination="$2"; shift 2 ;;
        --target-dir) build="$2"; shift 2 ;;
        --bin|--example) kind="${1#--}"; target="$2"; shift 2 ;;
        --registry) registry="$2"; shift 2 ;;
        --jobs) jobs="$2"; shift 2 ;;
        --locked) locked=true; shift ;;
        --debug) profile=debug; shift ;;
        --offline) shift ;;
        *) exit 74 ;;
    esac
done
[[ "$locked:$registry:$jobs" == true:crates-io:2 && "$build" == "$destination/build" && "$PWD" == "$destination/home" && "$HOME" == "$PWD" ]] || exit 75
printf '%s %s %s %s %s\n' "$package" "$version" "$kind" "$target" "$profile" >> "$fixture/builds"
mkdir -p "$destination/bin" "$build"
printf 'retained compilation\n' > "$build/evidence"
[[ ! -f "$fixture/fail-$package" ]] || exit 23
if [[ -f "$fixture/locked-file" ]]; then
    printf '\n' >> "$(cat "$fixture/locked-file")"
    sed 's/2.0.0/2.0.1/' "$(cat "$fixture/locked-file")" > "$fixture/changed-lock"
    cp "$fixture/changed-lock" "$(cat "$fixture/locked-file")"
fi
printf '#!%s\n' "$BASH" > "$destination/bin/$target"
printf 'printf "probe\\n" >> %q\n' "$fixture/probes" >> "$destination/bin/$target"
case "$package" in
    cargo-sort|cargo-sort-derives|candid-extractor)
        printf 'printf "%%s\\n" %q\n' "$package $version" >> "$destination/bin/$target" ;;
    *) echo 'exit 97' >> "$destination/bin/$target" ;;
esac
chmod +x "$destination/bin/$target"
compiler="$("$RUSTC" -vV)"
"$jq_bin" -n --arg identity "$package $version (registry+https://github.com/rust-lang/crates.io-index)" \
    --arg version "=$version" --arg target "$target" --arg profile "$profile" --arg compiler "$compiler" \
    '{installs:{($identity):{version_req:$version,bins:[$target],profile:$profile,target:"fixture-host",rustc:$compiler}}}' > "$destination/.crates2.json"
printf 'Cargo receipt\n' > "$destination/.crates.toml"
if [[ -f "$fixture/redirect-observation" ]]; then
    ln -s "$fixture/outside-observation" "$destination/$(cat "$fixture/redirect-observation")"
fi
if [[ -f "$fixture/fail-receipt" ]]; then printf '{"installs":{}}\n' > "$destination/.crates2.json"; fi
if [[ -f "$fixture/change-build" ]]; then
    case "$(cat "$fixture/change-build")" in
        compiler) printf 'changed fixture compiler\n' > "$fixture/compiler-version" ;;
        native) printf 'changed fixture native inventory\n' > "$fixture/changed-native" ;;
        policy) printf '\n# Changed fixture build policy\n' >> "$fixture/tool-source/scripts/dev/cargo-tool-build.sh" ;;
    esac
fi
STUB
chmod +x "$fixture/compiler/bin/"*
export PATH="$fixture/compiler/bin:$PATH"
installer="$ROOT/scripts/dev/install-rust-tools.sh"
cat > "$fixture/versions" <<'PINS'
SHARED_TOOLING_CARGO_SORT_VERSION=99.1.0
SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION=99.2.0
SHARED_TOOLING_CANDID_EXTRACTOR_VERSION=99.3.0
PINS
source_package() {
    local name="$1" version="$2"
    mkdir -p "$fixture/source/$name-$version"
    printf 'version = 3\n[[package]]\nname = "%s"\nversion = "%s"\n' "$name" "$version" > "$fixture/source/$name-$version/Cargo.lock"
    tar -czf "$fixture/assets/$name-$version.crate" -C "$fixture/source" "$name-$version"
}
for pair in 'cargo-sort 99.1.0' 'cargo-sort-derives 99.2.0' 'candid-extractor 99.3.0' 'sample 1.2.3' 'sample 2.0.0'; do
    read -r name version <<< "$pair"; source_package "$name" "$version"
done
refuse() {
    if "$@" > "$fixture/refused.log" 2>&1; then echo 'unexpected Cargo tool admission' >&2; exit 1; fi
}
common() { bash "$installer" --consumer "$consumer" --versions "$fixture/versions" "$@"; }
hook() (cd "$consumer" && bash .githooks/pre-commit)
consumer="$fixture/a"
# Direct compiler identity checks must use the fixture's prepared context too.
# The invoking consumer may select an entirely different Rust toolchain.
cd "$consumer"
common --preflight
[[ ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" ]] || exit 1
refuse common --check
[[ ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" ]] || exit 1
if [[ "$path_context" == true ]]; then
    common
    common --check
    original="$(readlink "$consumer/.tools/rust/common")"
    before="$(wc -l < "$fixture/builds")"
    common
    [[ "$(wc -l < "$fixture/builds")" == "$before" ]] || exit 1
    source_package cargo-sort 99.1.1
    sed 's/99.1.0/99.1.1/' "$fixture/versions" > "$fixture/new-versions"
    cp "$fixture/new-versions" "$fixture/versions"
    touch "$fixture/fail-cargo-sort"
    refuse common
    grep -F 'Cargo tool attempt retained:' "$fixture/refused.log"
    [[ "$(readlink "$consumer/.tools/rust/common")" == "$original" ]] || exit 1
    [[ ! -e "$consumer/.tools/rust/common.lock" && -z "$(find "$SHARED_TOOLING_STORE" -name install.lock)" ]] || exit 1
    [[ -n "$(find "$SHARED_TOOLING_STORE" -path '*/build/evidence' -type f)" ]] || exit 1
    fixture_complete=true
    echo 'Rust path-context install, reuse, check and retained failure passed'
    exit 0
fi
# The CI cache key shares the installer identity and performs no setup. Exercise
# changes in a private copy so no production source is modified by qualification.
mkdir -p "$fixture/cache-source/scripts/dev" "$fixture/cache-source/scripts/ci" "$fixture/cache-source/ci"
for file in scripts/ci/cargo-tool-cache-key.sh scripts/ci/tool-catalog-records.pl scripts/ci/verify-file-checksum.sh \
    scripts/dev/cargo-tool-build.sh scripts/dev/tool-store.sh scripts/dev/install-rust-tools.sh; do
    cp "$ROOT/$file" "$fixture/cache-source/$file"
done
cache_owner="$fixture/cache-source/scripts/ci/cargo-tool-cache-key.sh"
cache_pins="$fixture/cache-source/ci/tool-versions.env"
cp "$fixture/versions" "$cache_pins"
cache_before="$(bash "$cache_owner" "$cache_pins")"
[[ "$cache_before" == key=*$'\n'root="$SHARED_TOOLING_STORE" ]] || exit 1
[[ "$(bash "$cache_owner" "$cache_pins")" == "$cache_before" && ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" ]] || exit 1
printf '\n# Changed pins\n' >> "$cache_pins"
printf 'export SHARED_TOOLING_JQ_VERSION=99.9.9\nexport SHARED_TOOLING_JQ_SHA256_LINUX_AMD64=abc123\n' >> "$cache_pins"
[[ "$(bash "$cache_owner" "$cache_pins")" == "$cache_before" ]] || exit 1
awk '{ rows[NR]=$0 } END { for (i=NR; i>0; i--) print rows[i] }' "$cache_pins" > "$fixture/reordered-pins"
[[ "$(bash "$cache_owner" "$fixture/reordered-pins")" == "$cache_before" ]] || exit 1
# Shell comments need whitespace after an assignment; a glued # is value data.
sed 's/$/ # comment/' "$fixture/versions" > "$cache_pins"
[[ "$(bash "$cache_owner" "$cache_pins")" == "$cache_before" ]] || exit 1
sed 's/$/#suffix/' "$fixture/versions" > "$cache_pins"
[[ "$(bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
# CR in a shell assignment is part of its value, not harmless whitespace.
awk '{ printf "%s\r\n", $0 }' "$fixture/versions" > "$cache_pins"
[[ "$(bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
for pin in CARGO_SORT CARGO_SORT_DERIVES CANDID_EXTRACTOR; do
    sed "s/SHARED_TOOLING_${pin}_VERSION=.*/SHARED_TOOLING_${pin}_VERSION=98.0.0/" "$fixture/versions" > "$cache_pins"
    [[ "$(bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
done
# Executable configuration is opaque: retain all bytes without running it.
# shellcheck disable=SC2016 # Literal shell text must never be evaluated by the cache reader.
for opaque in 'touch "$CACHE_KEY_SIDE_EFFECT"' 'export RUSTUP_TOOLCHAIN=nightly' \
    'export SHARED_TOOLING_CARGO_SORT_VERSION=99.1.0'; do
    cp "$fixture/versions" "$cache_pins"
    printf '%s\n' "$opaque" >> "$cache_pins"
    opaque_key="$(CACHE_KEY_SIDE_EFFECT="$fixture/unexpected-cache-effect" bash "$cache_owner" "$cache_pins")"
    [[ "$opaque_key" != "$cache_before" && ! -e "$fixture/unexpected-cache-effect" ]] || exit 1
    printf '# Opaque catalog changed\n' >> "$cache_pins"
    [[ "$(bash "$cache_owner" "$cache_pins")" != "$opaque_key" ]] || exit 1
done
cp "$fixture/versions" "$cache_pins"
for file in scripts/dev/cargo-tool-build.sh scripts/dev/install-rust-tools.sh; do
    printf '\n# Changed cache input\n' >> "$fixture/cache-source/$file"
    [[ "$(bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
    cp "$ROOT/$file" "$fixture/cache-source/$file"
done
printf 'changed compiler\n' > "$fixture/compiler-version"
[[ "$(bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
printf 'fixture rustc\n' > "$fixture/compiler-version"
printf 'changed native environment\n' > "$fixture/changed-native"
[[ "$(bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
rm "$fixture/changed-native"
[[ "$(ImageVersion=fixture-changed-image bash "$cache_owner" "$cache_pins")" != "$cache_before" ]] || exit 1
[[ "$(bash "$cache_owner" "$cache_pins")" == "$cache_before" ]] || exit 1
refuse bash "$cache_owner"
rm "$cache_pins"
refuse bash "$cache_owner" "$cache_pins"
[[ ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" && ! -e "$fixture/builds" ]] || exit 1
# User TOML policy retains its boolean type before any source/install dispatch.
# The environment override remains an independent, explicitly selected contract.
(
    # shellcheck disable=SC2030 # This policy override is deliberately subshell-local.
    export CARGO_HOME="$fixture/user-cargo"
    unset CARGO_NET_OFFLINE
    mkdir "$CARGO_HOME"
    for value in '"true"' '"false\n"' 0 1 '[]' '{}' ; do
        printf '[net]\noffline = %s\n' "$value" > "$CARGO_HOME/config.toml"
        refuse common
        [[ ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" && ! -e "$fixture/builds" ]] || exit 1
    done
    # Exercise accepted flags through the same owner, without building tools.
    # shellcheck source=scripts/dev/tool-store.sh
    source "$ROOT/scripts/dev/tool-store.sh"
    # shellcheck source=scripts/dev/cargo-tool-build.sh
    source "$ROOT/scripts/dev/cargo-tool-build.sh"
    for value in missing false true; do
        printf '[net]\n' > "$CARGO_HOME/config.toml"
        [[ "$value" == missing ]] || printf 'offline = %s\n' "$value" >> "$CARGO_HOME/config.toml"
        cargo_build_init
        expected=''; [[ "$value" != true ]] || expected=--offline
        [[ "${offline_args[*]-}" == "$expected" ]] || exit 1
    done
    printf '' > "$CARGO_HOME/config.toml"
    cargo_build_init
    [[ "${#offline_args[@]}" == 0 ]] || exit 1
    # Cargo gives config precedence over config.toml when both exist.
    printf '[net]\noffline = true\n' > "$CARGO_HOME/config.toml"
    printf '[net]\noffline = false\n' > "$CARGO_HOME/config"
    cargo_build_init
    [[ "${#offline_args[@]}" == 0 ]] || exit 1
    printf '[net]\noffline = "invalid"\n' > "$CARGO_HOME/config"
    for CARGO_NET_OFFLINE in true 1 false 0; do
        export CARGO_NET_OFFLINE
        cargo_build_init
        expected=''
        case "$CARGO_NET_OFFLINE" in true|1) expected=--offline ;; esac
        [[ "${offline_args[*]-}" == "$expected" ]] || exit 1
    done
)
# Preserve previous physical installations; adopt new shared references explicitly.
mkdir -p "$consumer/.tools/rust/previous/installed/build"
printf 'old local evidence\n' > "$consumer/.tools/rust/previous/installed/build/payload"
RUSTFLAGS=invalid RUSTC_WRAPPER=/missing UNMODELLED_SECRET=must-not-propagate common
[[ "$(wc -l < "$fixture/builds")" -eq 3 ]] || exit 1
common --check
first_binary="$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$consumer/.tools/rust/common/bin/cargo-sort")"
(
    # shellcheck disable=SC2031 # Reapply the override independently for this subshell.
    export CARGO_HOME="$fixture/user-cargo"
    unset CARGO_NET_OFFLINE
    before="$(readlink "$consumer/.tools/rust/common")"
    for mode in setup check; do
        if [[ "$mode" == setup ]]; then refuse common; else refuse common --check; fi
        [[ "$(readlink "$consumer/.tools/rust/common")" == "$before" && ! -e "$consumer/.tools/rust/common.lock" ]] || exit 1
        [[ "$(wc -l < "$fixture/builds")" -eq 3 ]] || exit 1
    done
)
# A real staged hook uses these receipts without installing into its export.
# Cargo/compiler binaries above are substitutes; receipt admission and Git are real.
mkdir -p "$consumer/scripts/ci" "$consumer/scripts/dev" "$consumer/.githooks"
cp "$ROOT/.githooks/pre-commit" "$consumer/.githooks/"
for file in check-make-execution verify-file-checksum; do
    cp "$ROOT/scripts/ci/$file.sh" "$consumer/scripts/ci/"
done
for file in install-rust-tools cargo-tool-build tool-store; do
    cp "$ROOT/scripts/dev/$file.sh" "$consumer/scripts/dev/"
done
cp "$fixture/versions" "$consumer/pins"
printf '.tools/\n' > "$consumer/.gitignore"
printf 'formatted\n' > "$consumer/selected.rs"
cat > "$consumer/Makefile" <<'MAKE'
fmt:
	@bash scripts/dev/install-rust-tools.sh --consumer "$(CURDIR)" --versions pins --check > /dev/null
	@test ! -e .tools
	@printf 'formatted\n' > selected.rs
MAKE
git init -q "$consumer"
# Borrow immutable history; only this disposable repository's refs/index change.
source_commit="$(git -C "$ROOT" rev-parse HEAD)"
source_objects="$(git -C "$ROOT" rev-parse --git-path objects)"
case "$source_objects" in /*) ;; *) source_objects="$ROOT/$source_objects" ;; esac
printf '%s\n' "$source_objects" > "$consumer/.git/objects/info/alternates"
git -C "$consumer" update-ref HEAD "$source_commit"
# Keep only the fixture payload and an unchanged tracked README in its index.
# Other source paths are staged deletions, never formatter inputs.
git -C "$consumer" reset -q HEAD -- README.md
git -C "$consumer" checkout-index -- README.md
git -C "$consumer" add .gitignore .githooks scripts pins README.md selected.rs Makefile
printf 'working-only README\n' > "$consumer/README.md"
printf 'retained evidence\n' > "$consumer/untracked-evidence"
printf 'unformatted\n' > "$consumer/selected.rs"
git -C "$consumer" add selected.rs
# Selected catalog edits must reject partial staging without changing any bytes.
hook_tree="$(git -C "$consumer" write-tree)"
printf 'exit 99\n' > "$consumer/pins"
refuse hook
grep -F 'partially staged file: pins' "$fixture/refused.log" >/dev/null
[[ "$(git -C "$consumer" write-tree)" == "$hook_tree" && "$(cat "$consumer/selected.rs")" == unformatted ]] || exit 1
# Independently admit the staged catalog against the real checkout's receipts;
# unrelated working catalog bytes must never select the exported tools.
mkdir "$fixture/staged-inputs"
git -C "$consumer" checkout-index --all --prefix="$fixture/staged-inputs/"
SHARED_TOOLING_FORMAT_WORKTREE="$fixture/staged-inputs" SHARED_TOOLING_FORMAT_TOOLS_FROM="$consumer" \
    bash "$installer" --consumer "$fixture/staged-inputs" --versions "$fixture/staged-inputs/pins" --check
[[ ! -e "$fixture/staged-inputs/.tools" && "$(cat "$consumer/pins")" == 'exit 99' ]] || exit 1
cp "$fixture/versions" "$consumer/pins"
(cd "$consumer" && bash .githooks/pre-commit) > "$fixture/hook.log" 2>&1
[[ "$(git -C "$consumer" show :selected.rs)" == formatted ]] || exit 1
[[ "$(cat "$consumer/README.md")" == 'working-only README' ]] || exit 1
[[ "$(git -C "$consumer" show :README.md)" == "$(git -C "$consumer" show HEAD:README.md)" ]] || exit 1
printf 'unformatted\n' > "$consumer/selected.rs"
git -C "$consumer" add selected.rs
hook_tree="$(git -C "$consumer" write-tree)"
receipt="${first_binary%/bin/*}/selection.json"
cp "$receipt" "$fixture/receipt.saved"
printf '{}\n' > "$receipt"
refuse hook
cp "$fixture/receipt.saved" "$receipt"
[[ "$(git -C "$consumer" write-tree)" == "$hook_tree" && "$(cat "$consumer/selected.rs")" == unformatted ]] || exit 1
# Changed staged pins and compiler context require fresh explicit preparation.
printf 'SHARED_TOOLING_CARGO_SORT_VERSION=99.9.0\n' >> "$consumer/pins"
git -C "$consumer" add pins
refuse hook
cp "$fixture/versions" "$consumer/pins"
git -C "$consumer" add pins
printf 'changed compiler\n' > "$fixture/compiler-version"
refuse hook
printf 'fixture rustc\n' > "$fixture/compiler-version"
[[ "$(git -C "$consumer" write-tree)" == "$hook_tree" && "$(cat "$consumer/untracked-evidence")" == 'retained evidence' ]] || exit 1
printf 'unprepared staged compiler\n' > "$consumer/rust-toolchain.toml"
git -C "$consumer" add rust-toolchain.toml
refuse hook
grep -F 'unprepared staged compiler' "$fixture/refused.log" >/dev/null
git -C "$consumer" rm -qf rust-toolchain.toml
[[ "$(wc -l < "$fixture/builds")" -eq 3 ]]
mkdir "$fixture/export"
SHARED_TOOLING_FORMAT_WORKTREE="$fixture/export" SHARED_TOOLING_FORMAT_TOOLS_FROM="$consumer" \
    refuse bash "$installer" --consumer "$fixture/export" --versions "$fixture/versions"
[[ ! -e "$fixture/export/.tools" ]]
# An unrelated consumer never borrows a parent hook's selection context.
SHARED_TOOLING_FORMAT_WORKTREE="$fixture/export" SHARED_TOOLING_FORMAT_TOOLS_FROM="$consumer" \
    refuse bash "$installer" --consumer "$fixture/b" --versions "$fixture/versions" --check
printf 'old local evidence\n' | cmp - "$consumer/.tools/rust/previous/installed/build/payload"
# Conditional common setup must preserve a foreign lock and every selection,
# with no build/reference/activation attempt and no successful-path output.
held_slot="${first_binary%/set.*/bin/*}"
mkdir "$held_slot/install.lock"
printf 'foreign owner\n' > "$held_slot/install.lock/owner"
cp "$held_slot/install.lock/owner" "$fixture/foreign-owner"
old_common="$(readlink "$consumer/.tools/rust/common")"
old_current="$(readlink "$held_slot/current")"
find "$consumer/.tools/rust" "$held_slot" -print | sort > "$fixture/before-held"
status=0
common > "$fixture/held.out" 2> "$fixture/held.err" || status=$?
[[ "$status" -ne 0 && ! -s "$fixture/held.out" ]] || { echo 'common setup concealed a held shared lock' >&2; exit 1; }
grep -F "tool store installation locked: $held_slot/install.lock" "$fixture/held.err" >/dev/null
if grep -F 'blocked root activation' "$fixture/held.err"; then exit 1; fi
cmp "$held_slot/install.lock/owner" "$fixture/foreign-owner"
[[ "$(readlink "$consumer/.tools/rust/common")" == "$old_common" && "$(readlink "$held_slot/current")" == "$old_current" ]] || exit 1
[[ "$(wc -l < "$fixture/builds")" -eq 3 ]] || exit 1
find "$consumer/.tools/rust" "$held_slot" -print | sort > "$fixture/after-held"
cmp "$fixture/before-held" "$fixture/after-held"
rm "$held_slot/install.lock/owner"
rmdir "$held_slot/install.lock"
# A CI cache restores physical shared generations, never checkout selections.
# Round-trip all builds/receipts/source bytes before the existing cold-checkout
# reuse control; its no-info/build-count assertions prove no download or rebuild.
(cd "$SHARED_TOOLING_STORE" && tar -czf "$fixture/cargo-tools-cache.tar.gz" cargo-cache rust-*)
mv "$SHARED_TOOLING_STORE" "$fixture/store-before-restore"
(umask 077; mkdir "$SHARED_TOOLING_STORE")
tar -xzf "$fixture/cargo-tools-cache.tar.gz" -C "$SHARED_TOOLING_STORE"
consumer="$fixture/b"
touch "$fixture/no-info"
CARGO_NET_OFFLINE=true common
[[ "$(wc -l < "$fixture/builds")" -eq 3 && "$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$consumer/.tools/rust/common/bin/cargo-sort")" == "$first_binary" ]] || exit 1
[[ -z "$(find "$consumer/.tools" -type f)" ]] || exit 1
common --check
# New compiler identity selects new payloads; the first checkout stays pinned.
printf 'fixture rustc changed\n' > "$fixture/compiler-version"
refuse common --check
common
[[ "$(wc -l < "$fixture/builds")" -eq 6 && "$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$fixture/a/.tools/rust/common/bin/cargo-sort")" == "$first_binary" ]] || exit 1
printf 'fixture rustc\n' > "$fixture/compiler-version"
common
[[ "$(wc -l < "$fixture/builds")" -eq 6 ]] || exit 1
rm "$fixture/no-info"
# Selected product executables are verified without executing them.
selected_args=(--consumer "$consumer" --package sample --version 1.2.3 --example prepare --profile debug)
: > "$fixture/probes"
path="$(bash "$installer" "${selected_args[@]}")"
[[ ! -s "$fixture/probes" && -x "$path" ]] || exit 1
CARGO_NET_OFFLINE=true bash "$installer" "${selected_args[@]}" --check > "$fixture/selected-check"
[[ "$(cat "$fixture/selected-check")" == "$path" && ! -s "$fixture/probes" ]] || exit 1
# Explicit extension setup reuses the selection quietly, without info/install
# dispatch or executable probes. Another checkout receives the same bytes.
builds_before="$(wc -l < "$fixture/builds")"
touch "$fixture/no-info"
for retry in first second; do
    CARGO_NET_OFFLINE=true bash "$installer" "${selected_args[@]}" > "$fixture/reuse-$retry.out" 2> "$fixture/reuse-$retry.err"
    [[ "$(cat "$fixture/reuse-$retry.out")" == "$path" && ! -s "$fixture/reuse-$retry.err" ]] || exit 1
done
extension_path="$(CARGO_NET_OFFLINE=true bash "$installer" --consumer "$fixture/a" \
    --package sample --version 1.2.3 --example prepare --profile debug 2> "$fixture/extension-reuse.err")"
[[ "$(wc -l < "$fixture/builds")" == "$builds_before" && ! -s "$fixture/probes" && ! -s "$fixture/extension-reuse.err" ]] || exit 1
[[ "$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$extension_path")" == \
   "$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$path")" ]] || exit 1
rm "$fixture/no-info"
physical="$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$path")"; bundle="${physical%/bin/*}"
# Missing source must preserve Cargo's status through both installer call shapes,
# without a build, retry, activation or loss of previously retained evidence.
cp "$fixture/versions" "$fixture/missing-source-versions"
printf 'SHARED_TOOLING_CARGO_SORT_VERSION=99.9.9\n' >> "$fixture/missing-source-versions"
cp -R "$SHARED_TOOLING_STORE" "$fixture/before-source-store"
cp -R "$consumer/.tools" "$fixture/before-source-tools"
source_common="$(readlink "$consumer/.tools/rust/common")"
: > "$fixture/info-refusals"
touch "$fixture/no-info"
attempts=0
for mode in common selected; do
    status=0
    if [[ "$mode" == common ]]; then
        common --versions "$fixture/missing-source-versions" > "$fixture/source-$mode.out" 2> "$fixture/source-$mode.err" || status=$?
    else
        bash "$installer" "${selected_args[@]}" --version 9.9.9 > "$fixture/source-$mode.out" 2> "$fixture/source-$mode.err" || status=$?
    fi
    attempts=$((attempts + 1))
    [[ "$status" -eq 91 && ! -s "$fixture/source-$mode.out" &&
       "$(wc -l < "$fixture/info-refusals")" -eq "$attempts" &&
       "$(wc -l < "$fixture/builds")" == "$builds_before" &&
       "$(readlink "$consumer/.tools/rust/common")" == "$source_common" &&
       "$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$path")" == "$physical" ]] || exit 1
    grep -Fx 'injected Cargo source acquisition failure' "$fixture/source-$mode.err" >/dev/null
    diff -r "$fixture/before-source-store" "$SHARED_TOOLING_STORE"
    diff -r "$fixture/before-source-tools" "$consumer/.tools"
done
rm "$fixture/no-info"
for field in bin/prepare .crates2.json inputs.json selection.json source.crate Cargo.lock; do
    cp "$bundle/$field" "$fixture/saved"
    printf '\nchanged\n' >> "$bundle/$field"
    refuse bash "$installer" "${selected_args[@]}" --check
    [[ ! -s "$fixture/probes" ]] || exit 1
    cp "$fixture/saved" "$bundle/$field"
done
# A different kind/profile cannot reuse the example selection.
cp "$bundle/.crates2.json" "$fixture/receipt"
cp "$bundle/selection.json" "$fixture/selection"
for field in version_req target profile rustc; do
    jq --arg field "$field" '.installs[][$field]="wrong"' "$fixture/receipt" > "$bundle/.crates2.json"
    digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$bundle/.crates2.json")"
    jq --arg digest "$digest" '.receipt_sha256=$digest' "$fixture/selection" > "$bundle/selection.json"
    refuse bash "$installer" "${selected_args[@]}" --check
done
cp "$fixture/receipt" "$bundle/.crates2.json"
cp "$fixture/selection" "$bundle/selection.json"
for file in inputs.json selection.json .crates2.json; do
    cp "$bundle/$file" "$fixture/single-json"
    cat "$fixture/single-json" >> "$bundle/$file"
    refuse bash "$installer" "${selected_args[@]}" --check
    cp "$fixture/single-json" "$bundle/$file"
done
[[ ! -s "$fixture/probes" ]] || exit 1
other="$(bash "$installer" --consumer "$consumer" --package sample --version 1.2.3 --bin prepare --profile release)"
[[ "$other" != "$path" ]] || exit 1
slot="${bundle%/*}"
mkdir "$slot/install.lock"
refuse bash "$installer" "${selected_args[@]}" --check
grep -F "tool store installation locked: $slot/install.lock" "$fixture/refused.log"
rmdir "$slot/install.lock"
# A failed build keeps the previously selected common set and shared evidence.
source_package cargo-sort 99.1.1
printf 'SHARED_TOOLING_CARGO_SORT_VERSION=99.1.1\n' >> "$fixture/versions"
before="$(readlink "$consumer/.tools/rust/common")"
touch "$fixture/fail-cargo-sort"
refuse common
[[ "$(readlink "$consumer/.tools/rust/common")" == "$before" ]] || exit 1
grep -F 'Cargo tool attempt retained:' "$fixture/refused.log"
rm "$fixture/fail-cargo-sort"
# Admission, reference and each publication boundary must stop common setup.
# Earlier common tools remain selected; failed candidates/evidence are retained.
for failure in receipt reference store-activation local-activation common-activation; do
    touch "$fixture/fail-$failure"
    status=0
    common > "$fixture/$failure.out" 2> "$fixture/$failure.err" || status=$?
    [[ "$status" -ne 0 && ! -s "$fixture/$failure.out" ]] || { echo "concealed $failure failure" >&2; exit 1; }
    case "$failure" in reference) [[ "$status" -eq 77 ]] ;; *activation) [[ "$status" -eq 78 ]] ;; receipt) [[ "$status" -eq 1 ]] ;; esac
    [[ "$(readlink "$consumer/.tools/rust/common")" == "$before" ]] || exit 1
    [[ "$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$fixture/a/.tools/rust/common/bin/cargo-sort")" == "$first_binary" ]] || exit 1
    [[ ! -e "$consumer/.tools/rust/common.lock" ]] || exit 1
    [[ -z "$(find "$SHARED_TOOLING_STORE" -name install.lock)" ]] || exit 1
    rm "$fixture/fail-$failure"
done
common
[[ "$(readlink "$consumer/.tools/rust/common")" != "$before" ]] || exit 1
# The real selector/archiver includes only this consumer's referenced generations.
mkdir "$fixture/diagnostics"
bash "$ROOT/scripts/ci/select-tool-evidence.sh" full "$consumer" "$fixture/diagnostics" > "$fixture/selections"
inputs=()
while IFS= read -r -d '' root && IFS= read -r -d '' relative; do inputs+=("$root" "$relative"); done < "$fixture/selections"
bash "$ROOT/scripts/ci/archive-evidence.sh" "$fixture/evidence.tar.gz" "${inputs[@]}" > /dev/null
mkdir "$fixture/unpacked"
tar -xzf "$fixture/evidence.tar.gz" -C "$fixture/unpacked"
cmp "$bundle/bin/prepare" "$fixture/unpacked/${bundle#"$SHARED_TOOLING_STORE/"}/bin/prepare"
[[ "$(find "$fixture/unpacked" -name evidence -type f | wc -l)" -gt 0 ]] || exit 1
# Refuse injected global Cargo configuration even on an otherwise reusable set.
printf '[build]\nrustflags=["--bad"]\n' > "$SHARED_TOOLING_STORE/cargo-cache/config.toml"
refuse common --check
rm "$SHARED_TOOLING_STORE/cargo-cache/config.toml"
common --check
mv "$SHARED_TOOLING_STORE/cargo-cache/registry/cache" "$fixture/retained-cache"
ln -s "$fixture/retained-cache" "$SHARED_TOOLING_STORE/cargo-cache/registry/cache"
refuse common
rm "$SHARED_TOOLING_STORE/cargo-cache/registry/cache"
mv "$fixture/retained-cache" "$SHARED_TOOLING_STORE/cargo-cache/registry/cache"
# Lock-selected package changes during compilation must prevent activation.
mkdir "$fixture/locked"
cat > "$fixture/locked/Cargo.lock" <<'LOCK'
version = 3
[[package]]
name = "sample"
version = "2.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
LOCK
printf '%s\n' "$fixture/locked/Cargo.lock" > "$fixture/locked-file"
refuse bash "$installer" --consumer "$fixture/locked" --package sample --lockfile Cargo.lock --bin prepare --profile release
grep -F 'Cargo tool lock selection changed' "$fixture/refused.log"
[[ -z "$(find "$fixture/locked/.tools/rust" -name 'selected-*')" ]] || exit 1
rm "$fixture/locked-file"
# Redirected local/store paths never become installation destinations.
mkdir "$fixture/redirected" "$fixture/outside"
ln -s "$fixture/outside" "$fixture/redirected/.tools"
refuse bash "$installer" --consumer "$fixture/redirected" --versions "$fixture/versions"
[[ -z "$(ls -A "$fixture/outside")" ]] || exit 1
ln -s "$SHARED_TOOLING_STORE" "$fixture/store-link"
SHARED_TOOLING_STORE="$fixture/store-link" refuse common --check
# Change each build identity input after the substitute Cargo writes its receipt.
# Mutate only disposable sources; the real admission, retention and activation run.
mkdir -p "$fixture/tool-source/scripts/dev" "$fixture/tool-source/scripts/ci"
cp "$ROOT/scripts/dev/"{install-rust-tools,cargo-tool-build,tool-store}.sh "$fixture/tool-source/scripts/dev/"
cp "$ROOT/scripts/ci/verify-file-checksum.sh" "$fixture/tool-source/scripts/ci/"
cp "$fixture/compiler-version" "$fixture/original-compiler"
before="$(readlink "$consumer/.tools/rust/common")"
source_package cargo-sort 99.1.2
printf 'SHARED_TOOLING_CARGO_SORT_VERSION=99.1.2\n' >> "$fixture/versions"
for change in compiler native policy; do
    printf '%s\n' "$change" > "$fixture/change-build"
    status=0
    UNMODELLED_SECRET=must-not-retain bash "$fixture/tool-source/scripts/dev/install-rust-tools.sh" \
        --consumer "$consumer" --versions "$fixture/versions" > "$fixture/$change.out" 2> "$fixture/$change.err" || status=$?
    [[ "$status" -eq 1 && ! -s "$fixture/$change.out" ]] || exit 1
    field="$change"; [[ "$change" != policy ]] || field=policy_sha256
    grep -Fx "Cargo tool build environment changed: $field" "$fixture/$change.err"
    attempt="$(sed -n 's/^Cargo tool attempt retained: //p' "$fixture/$change.err")"
    [[ -f "$attempt/build/evidence" && -f "$attempt/install.log" && ! -e "$attempt/selection.json" && ! -e "${attempt%/*}/current" ]] || exit 1
    jq -e --arg field "$field" --slurpfile observed "$attempt/build-identity.observed.json" \
        '.build[$field] != $observed[0][$field] and (.build | del(.[$field])) == ($observed[0] | del(.[$field]))' "$attempt/inputs.json" > /dev/null
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$(jq -r .build.native "$attempt/inputs.json")" "$attempt/native-environment.txt"
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$(jq -r .native "$attempt/build-identity.observed.json")" "$attempt/native-environment.observed.txt"
    if grep -F must-not-retain "$attempt/"*.json "$attempt/"*.txt "$fixture/$change.err"; then exit 1; fi
    [[ "$(readlink "$consumer/.tools/rust/common")" == "$before" ]] || exit 1
    [[ ! -e "$consumer/.tools/rust/common.lock" && -z "$(find "$SHARED_TOOLING_STORE" -name install.lock)" ]] || exit 1
    # The standard evidence selector must retain both observations, even though
    # this generation was never activated.
    mkdir "$fixture/$change-diagnostics"
    bash "$ROOT/scripts/ci/select-tool-evidence.sh" full "$consumer" "$fixture/$change-diagnostics" > "$fixture/$change-selections"
    inputs=()
    while IFS= read -r -d '' root && IFS= read -r -d '' relative; do inputs+=("$root" "$relative"); done < "$fixture/$change-selections"
    bash "$ROOT/scripts/ci/archive-evidence.sh" "$fixture/$change-evidence.tar.gz" "${inputs[@]}" > /dev/null
    mkdir "$fixture/$change-unpacked"
    tar -xzf "$fixture/$change-evidence.tar.gz" -C "$fixture/$change-unpacked"
    for evidence in inputs.json native-environment.txt build-identity.observed.json native-environment.observed.txt; do
        cmp "$attempt/$evidence" "$fixture/$change-unpacked/${attempt#"$SHARED_TOOLING_STORE/"}/$evidence"
    done
    rm "$fixture/change-build"
    cp "$fixture/original-compiler" "$fixture/compiler-version"
    rm -f "$fixture/changed-native"
    cp "$ROOT/scripts/dev/cargo-tool-build.sh" "$fixture/tool-source/scripts/dev/"
done
for leaf in build-identity.observed.json native-environment.observed.txt; do
    printf '%s\n' "$leaf" > "$fixture/redirect-observation"
    printf 'outside evidence must remain intact\n' > "$fixture/outside-observation"
    cp "$fixture/outside-observation" "$fixture/expected-observation"
    printf 'compiler\n' > "$fixture/change-build"
    refuse bash "$installer" --consumer "$consumer" --versions "$fixture/versions"
    attempt="$(sed -n 's/^Cargo tool attempt retained: //p' "$fixture/refused.log")"
    [[ -L "$attempt/$leaf" && -f "$attempt/build/evidence" && -f "$attempt/install.log" ]]
    [[ ! -e "$attempt/selection.json" && ! -e "${attempt%/*}/current" ]]
    cmp "$fixture/expected-observation" "$fixture/outside-observation"
    [[ "$(readlink "$consumer/.tools/rust/common")" == "$before" ]]
    [[ ! -e "$consumer/.tools/rust/common.lock" && -z "$(find "$SHARED_TOOLING_STORE" -name install.lock)" ]]
    [[ -z "$(find "$consumer/.tools/rust" -name '.selected-*.lock')" ]]
    mkdir "$fixture/$leaf-diagnostics"
    bash "$ROOT/scripts/ci/select-tool-evidence.sh" full "$consumer" "$fixture/$leaf-diagnostics" > "$fixture/$leaf-selections"
    inputs=()
    while IFS= read -r -d '' root && IFS= read -r -d '' relative; do inputs+=("$root" "$relative"); done < "$fixture/$leaf-selections"
    bash "$ROOT/scripts/ci/archive-evidence.sh" "$fixture/$leaf.tar.gz" "${inputs[@]}" > /dev/null
    # Extract only this retained leaf: it must still be a link, not target bytes.
    mkdir "$fixture/$leaf-unpacked"
    relative="${attempt#"$SHARED_TOOLING_STORE/"}/$leaf"
    tar -xzf "$fixture/$leaf.tar.gz" -C "$fixture/$leaf-unpacked" "./$relative"
    [[ -L "$fixture/$leaf-unpacked/$relative" ]]
    cmp "$fixture/expected-observation" "$fixture/outside-observation"
    rm "$fixture/redirect-observation" "$fixture/change-build"
    cp "$fixture/original-compiler" "$fixture/compiler-version"
done
[[ "$(git -C "$fixture/a" rev-parse HEAD)" == "$source_commit" && ! -s "$fixture/forbidden-git" ]] || exit 1
echo 'Shared Cargo installation, standard environment, identities, retention and offline checks passed'
fixture_complete=true
