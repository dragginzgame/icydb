#!/usr/bin/env bash
# Shared companions: scripts/dev/install-rust-tools.sh scripts/dev/cargo-tool-build.sh scripts/dev/tool-store.sh scripts/ci/select-tool-evidence.sh scripts/ci/archive-evidence.sh .githooks/pre-commit
set -euo pipefail
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"; ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/rust-tools-test.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
fixture_complete=false
finish() {
    local status=$?
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
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
    [[ ! -f "$fixture/no-info" ]] || exit 91
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
common --preflight
[[ ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" ]] || exit 1
refuse common --check
[[ ! -e "$SHARED_TOOLING_STORE" && ! -e "$consumer/.tools" ]] || exit 1
# Preserve previous physical installations; adopt new shared references explicitly.
mkdir -p "$consumer/.tools/rust/previous/installed/build"
printf 'old local evidence\n' > "$consumer/.tools/rust/previous/installed/build/payload"
RUSTFLAGS=invalid RUSTC_WRAPPER=/missing UNMODELLED_SECRET=must-not-propagate common
[[ "$(wc -l < "$fixture/builds")" -eq 3 ]] || exit 1
common --check
first_binary="$(perl -MCwd=abs_path -e 'print abs_path(shift)' "$consumer/.tools/rust/common/bin/cargo-sort")"
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
printf 'original\n' > "$consumer/README.md"
printf 'formatted\n' > "$consumer/selected.rs"
cat > "$consumer/Makefile" <<'MAKE'
fmt:
	@bash scripts/dev/install-rust-tools.sh --consumer "$(CURDIR)" --versions pins --check > /dev/null
	@test ! -e .tools
	@printf 'formatted\n' > selected.rs
MAKE
git init -q "$consumer"
git -C "$consumer" add .gitignore .githooks scripts pins README.md selected.rs Makefile
git -C "$consumer" -c user.name=Fixture -c user.email=fixture@example.invalid \
    -c core.hooksPath=/dev/null commit -qm fixture
printf 'working-only README\n' > "$consumer/README.md"
printf 'retained evidence\n' > "$consumer/untracked-evidence"
printf 'unformatted\n' > "$consumer/selected.rs"
git -C "$consumer" add selected.rs
# Unstaged catalog changes cannot select the tools used on the staged source.
printf 'exit 99\n' > "$consumer/pins"
(cd "$consumer" && bash .githooks/pre-commit) > "$fixture/hook.log" 2>&1
[[ "$(git -C "$consumer" show :selected.rs)" == formatted ]]
[[ "$(cat "$consumer/pins")" == 'exit 99' && "$(cat "$consumer/README.md")" == 'working-only README' ]]
cp "$fixture/versions" "$consumer/pins"
printf 'unformatted\n' > "$consumer/selected.rs"
git -C "$consumer" add selected.rs
hook_tree="$(git -C "$consumer" write-tree)"
receipt="${first_binary%/bin/*}/selection.json"
cp "$receipt" "$fixture/receipt.saved"
printf '{}\n' > "$receipt"
refuse hook
cp "$fixture/receipt.saved" "$receipt"
[[ "$(git -C "$consumer" write-tree)" == "$hook_tree" && "$(cat "$consumer/selected.rs")" == unformatted ]]
# Changed staged pins and compiler context require fresh explicit preparation.
printf 'SHARED_TOOLING_CARGO_SORT_VERSION=99.9.0\n' >> "$consumer/pins"
git -C "$consumer" add pins
refuse hook
cp "$fixture/versions" "$consumer/pins"
git -C "$consumer" add pins
printf 'changed compiler\n' > "$fixture/compiler-version"
refuse hook
printf 'fixture rustc\n' > "$fixture/compiler-version"
[[ "$(git -C "$consumer" write-tree)" == "$hook_tree" && "$(cat "$consumer/untracked-evidence")" == 'retained evidence' ]]
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
echo 'Shared Cargo installation, standard environment, identities, retention and offline checks passed'
fixture_complete=true
