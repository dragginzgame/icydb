#!/usr/bin/env bash
set -euo pipefail

# Consumer-owned Make code is reviewed executable input, not sandboxed code.
if [[ $# -lt 4 ]]; then
    echo 'usage: check-formatting-hooks.sh ROOT RUST_PATH MANIFEST_PATH {UNSORTED_MANIFEST|--no-dependency-tables} [OVERLAY_PATH ...]' >&2
    exit 2
fi
root="$(cd "$1" && pwd -P)"
rust="$2"; manifest="$3"; unsorted="$4"
shift 4
perturb_manifest=true
if [[ "$unsorted" == --no-dependency-tables ]]; then
    perturb_manifest=false
else
    [[ "$unsorted" == /* ]] || unsorted="$PWD/$unsorted"
    [[ -f "$unsorted" && ! -L "$unsorted" ]] || exit 2
fi
for path in "$rust" "$manifest" Makefile README.md .githooks/pre-commit scripts/dev/install-git-hooks.sh scripts/ci/check-make-execution.sh "$@"; do
    case "$path" in
        ''|/*|..|../*|*/../*|*/..|./*|*/./*|*/.|.git|.git/*) echo "invalid relative input: $path" >&2; exit 2 ;;
    esac
    [[ -f "$root/$path" && ! -L "$root/$path" ]] || exit 2
    parent="$(cd "$(dirname "$root/$path")" && pwd -P)"
    case "$parent/" in "$root/"*) ;; *) exit 2 ;; esac
done
[[ "$rust" == *.rs && "${manifest##*/}" == Cargo.toml ]] || exit 2
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES
unset VALIDATION_REPOSITORY_ROOT VALIDATION_RUNNER_SNAPSHOT_PATH
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_NAMESPACE
export CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0
# Disposable adoption checkouts reuse the consumer's explicitly prepared tools.
export PATH="$root/.tools/host/bin:$root/.tools/ic/bin:$root/.tools/rust/bin:$PATH"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/formatting-adoption.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
finish() {
    local status=$?
    if [[ "$status" == 0 ]]; then rm -rf "$fixture";
    else echo "Formatting adoption evidence retained: $fixture" >&2; fi
}
trap finish EXIT
mkdir "$fixture/templates"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
objects="$(git -C "$root" rev-parse --git-path objects)"
case "$objects" in /*) ;; *) objects="$root/$objects" ;; esac
git init --quiet "$fixture/base"
cd "$fixture/base"
printf '%s\n' "$objects" > .git/objects/info/alternates
git update-ref HEAD "$(git -C "$root" rev-parse HEAD)"
git read-tree HEAD
# The shared hook supports only regular tracked files. Reject before export.
git ls-files --stage > "$fixture/entries"
if LC_ALL=C grep -Ev '^100(644|755) ' "$fixture/entries" > "$fixture/unsupported"; then
    echo 'formatting adoption requires regular tracked files' >&2; exit 1
fi
git checkout-index --all
for path in "$rust" "$manifest" Makefile .githooks/pre-commit scripts/dev/install-git-hooks.sh scripts/ci/check-make-execution.sh "$@"; do
    mkdir -p "$(dirname "$path")"
    cp -p "$root/$path" "$path"
    git --literal-pathspecs add -- "$path"
done
make --no-print-directory fmt-check > "$fixture/baseline.log" 2>&1
cp "$manifest" "$fixture/sorted-manifest"
if [[ "$perturb_manifest" == true ]]; then
    if cmp -s "$unsorted" "$manifest"; then echo 'unsorted input must differ from selected manifest' >&2; exit 2; fi
else
    echo 'No dependency tables selected; manifest sorting perturbation omitted'
fi
git ls-files -z '*Cargo.lock' > "$fixture/locks"
new_case() {
    cp -R "$fixture/base" "$fixture/$1"
    cd "$fixture/$1"
}
preserved_locks() {
    local path
    while IFS= read -r -d '' path; do cmp "$fixture/base/$path" "$path"; done < "$fixture/locks"
}
reject_hook() {
    if bash .githooks/pre-commit > .git/rejection.log 2>&1; then
        echo 'hook accepted a failing case' >&2; exit 1
    fi
    [[ "$(git write-tree)" == "$tree" ]] || exit 1
    cmp .git/before "$selected"
    preserved_locks
}
new_case refresh
printf '\npub fn shared_tooling_hook_fixture( ) { }\n' >> "$rust"
if [[ "$perturb_manifest" == true ]]; then cp "$unsorted" "$manifest"; fi
git --literal-pathspecs add -- "$rust" "$manifest"
printf '\nUnrelated working edit.\n' >> README.md
cp README.md .git/readme
printf 'untracked evidence\n' > shared-hook-untracked.txt
tree="$(git write-tree)"
bash .githooks/pre-commit > .git/formatting.log 2>&1
[[ "$(git write-tree)" != "$tree" ]] || exit 1
git --literal-pathspecs diff --exit-code -- "$rust" "$manifest" > .git/working-diff
cmp "$fixture/sorted-manifest" "$manifest"
grep -Fx 'pub fn shared_tooling_hook_fixture() {}' "$rust" >/dev/null
cmp .git/readme README.md
git diff --cached --exit-code HEAD -- README.md > .git/readme-index
[[ "$(cat shared-hook-untracked.txt)" == 'untracked evidence' && -z "$(git ls-files -- shared-hook-untracked.txt)" ]] || exit 1
preserved_locks
make --no-print-directory fmt-check > .git/check.log 2>&1
tree="$(git write-tree)"
bash .githooks/pre-commit >> .git/formatting.log 2>&1
[[ "$(git write-tree)" == "$tree" ]] || exit 1
for selected in "$rust" "$manifest"; do
    new_case "partial-${selected##*/}"
    printf '\n' >> "$selected"
    git --literal-pathspecs add -- "$selected"
    printf '\n' >> "$selected"
    cp "$selected" .git/before
    tree="$(git write-tree)"
    reject_hook
done
new_case formatter-failure
selected="$rust"
printf '\nfn broken(\n' >> "$rust"
git --literal-pathspecs add -- "$rust"
cp "$rust" .git/before
tree="$(git write-tree)"
reject_hook
cmp "$fixture/sorted-manifest" "$manifest"
new_case installer
bash scripts/dev/install-git-hooks.sh > .git/install.log 2>&1
[[ "$(git config --local --get core.hooksPath)" == .githooks ]] || exit 1
ln -s "$PWD" "$fixture/alias"
(cd "$fixture/alias"; bash scripts/dev/install-git-hooks.sh) >> .git/install.log 2>&1
git config --local core.hooksPath existing-hooks
if bash scripts/dev/install-git-hooks.sh > .git/conflict.log 2>&1; then exit 1; fi
[[ "$(git config --local --get core.hooksPath)" == existing-hooks ]] || exit 1
echo 'Consumer formatting, preservation and hook installation checks passed'
if [[ "$perturb_manifest" == true ]]; then echo 'Dependency sorting perturbation passed'; fi
