#!/usr/bin/env bash
# Actual IcyDB hooks and locked builds; only validation breadth and registry
# effects are substituted. The live release retains every product obligation.
set -euo pipefail
[[ $# == 0 ]] || exit 2
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES SHARED_TOOLING_ROOT
unset RELEASE_GROUP RELEASE_FROZEN_GROUPS RELEASE_FROZEN_PUBLISH_TARGET FROZEN_RELEASE_ACTIVE
unset RELEASE_COMMIT RELEASE_SOURCE RELEASE_RECEIPT_DIR ICYDB_RELEASE_ROOT PACKAGE
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES
unset CARGO_HOME CARGO_TARGET_DIR
mkdir -p "$root/target/tmp"
fixture="$(mktemp -d "$root/target/tmp/frozen-icydb.XXXXXXXX")"
complete=false
finish() {
    local status=$?
    [[ "$complete" == true || "$status" != 0 ]] || status=1
    printf 'Frozen IcyDB hook evidence: %s\n' "$fixture" >&2
    exit "$status"
}
trap finish EXIT
normal="$fixture/normal"
mkdir "$normal" "$fixture/bin" "$fixture/templates" "$fixture/archives"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
export FROZEN_TEST_RECORD="$fixture" FROZEN_TEST_NORMAL="$normal" FROZEN_TEST_CARGO FROZEN_TEST_CURL FROZEN_TEST_SOURCE_CACHE
FROZEN_TEST_CARGO="$(command -v cargo)"
FROZEN_TEST_CURL="$(command -v curl)"
FROZEN_TEST_SOURCE_CACHE="$(make --no-print-directory -s -C "$root" print-cargo-home)"
bash "$root/scripts/ci/verify-shared-tooling-snapshot.sh" --consumer "$root"
git -C "$root" ls-files --cached --others --exclude-standard -z > "$fixture/source-paths"
while IFS= read -r -d '' path; do
    [[ -e "$root/$path" ]] || continue
    [[ -f "$root/$path" && ! -L "$root/$path" ]]
    mkdir -p "$normal/$(dirname "$path")"
    cp -p "$root/$path" "$normal/$path"
done < "$fixture/source-paths"
# Fixture release notes must match its patch request independently of the live
# candidate's minor/patch selection. These are disposable fixture inputs only.
for group in database cli; do
    primary="$(jq -r --arg group "$group" '.groups[] | select(.name==$group) | .primary' "$normal/ci/cargo-release-groups.json")"
    current="$("$root/.tools/host/bin/yq" -p toml -o json -r '.package.version' "$normal/crates/$primary/Cargo.toml")"
    next="${current%.*}.$((${current##*.} + 1))"
    detail="releases/$group/${next%.*}.md"
    for notes in "releases/$group/CHANGELOG.md" "$detail"; do
        printf '# Frozen hook fixture %s\n\n## [%s]\n\n### 🧪 Testing\n\n- Exercise captured-source delivery and recovery.\n' "$group" "$next" > "$normal/$notes"
    done
    jq --arg group "$group" --arg detail "$detail" \
        '(.groups[] | select(.name==$group) | .metadata_files) = [$detail]' \
        "$normal/ci/cargo-release-groups.json" > "$fixture/groups.json"
    cp "$fixture/groups.json" "$normal/ci/cargo-release-groups.json"
done
# The updater's acquisition cache must contain this captured graph too. A
# newly deferred root lock may not have been fetched by its author yet.
make --no-print-directory -C "$normal" fetch "CARGO_WORK_HOME=$FROZEN_TEST_SOURCE_CACHE"
cp "$normal/Cargo.lock" "$fixture/captured-Cargo.lock"
export FROZEN_TEST_TESTKIT FROZEN_TEST_ITOA
FROZEN_TEST_TESTKIT="$("$root/.tools/host/bin/yq" -p toml -o json -r '.package[] | select(.name=="ic-testkit") | .version' "$normal/Cargo.lock")"
FROZEN_TEST_ITOA="$("$root/.tools/host/bin/yq" -p toml -o json -r '.package[] | select(.name=="itoa") | .version' "$normal/Cargo.lock")"
cat > "$normal/scripts/release/frozen-qualification-stage.sh" <<'STAGE'
#!/usr/bin/env bash
set -euo pipefail
yq="$PWD/.tools/host/bin/yq"
testkit="$("$yq" -p toml -o json -r '.package[] | select(.name=="ic-testkit") | .version' Cargo.lock)"
itoa="$("$yq" -p toml -o json -r '.package[] | select(.name=="itoa") | .version' Cargo.lock)"
[[ "$testkit" == "$FROZEN_TEST_TESTKIT" && "$itoa" == "$FROZEN_TEST_ITOA" ]]
printf '%s %s %s\n' "$RELEASE_GROUP" "$testkit" "$itoa" >> "$FROZEN_TEST_RECORD/stages"
if [[ "$RELEASE_GROUP" == database && -f "$FROZEN_TEST_RECORD/fail-once" ]]; then
    successor=1.0.17
    [[ "$FROZEN_TEST_ITOA" != "$successor" ]] || successor=1.0.18
    CARGO_HOME="$FROZEN_TEST_SOURCE_CACHE" "$FROZEN_TEST_CARGO" update --offline \
        --manifest-path "$FROZEN_TEST_NORMAL/Cargo.toml" -p itoa --precise "$successor"
    rm "$FROZEN_TEST_RECORD/fail-once"
    exit 143
fi
if [[ "$RELEASE_GROUP" == database ]]; then
    "$FROZEN_TEST_CARGO" check --locked --offline -p icydb --features testing
    # Qualify archive/source custody before release metadata changes. Final
    # versions are still verified by real publication during live delivery.
    CARGO_NET_OFFLINE=false make --no-print-directory package-cli
    "$FROZEN_TEST_CARGO" package --locked --offline -p icydb-diagnostic-code
    for package in icydb-cli icydb-diagnostic-code; do
        version="$("$yq" -p toml -o json -r '.package.version' "crates/$package/Cargo.toml")"
        archive="$CARGO_TARGET_DIR/package/$package-$version.crate"
        cp "$archive" "$FROZEN_TEST_RECORD/archives/"
        tar -xOf "$archive" "$package-$version/.cargo_vcs_info.json" |
            jq -e --arg source "$(git rev-parse HEAD)" '.git.sha1==$source and (.git.dirty // false)==false' >/dev/null
        tar -xOf "$archive" "$package-$version/Cargo.toml.orig" > "$FROZEN_TEST_RECORD/$package.original"
        git show "HEAD:crates/$package/Cargo.toml" > "$FROZEN_TEST_RECORD/$package.source"
        cmp "$FROZEN_TEST_RECORD/$package.original" "$FROZEN_TEST_RECORD/$package.source"
    done
else
    "$FROZEN_TEST_CARGO" check --locked --offline -p icydb-cli --all-targets
fi
STAGE
cat >> "$normal/Makefile" <<'MAKE'

# Qualification-only gate breadth. All metadata, Git, receipt and publication
# adapters above remain the actual IcyDB implementation.
validate:
	+$(CARGO_WORK_ENV) bash scripts/release/frozen-qualification-stage.sh
publish:
	+$(CARGO_PUBLISH_ENV) bash scripts/ci/publish-workspace.sh
	+@printf '%s\n' '$(RELEASE_GROUP)' >> "$$FROZEN_TEST_RECORD/publications"
	+@if [ '$(RELEASE_GROUP)' = database ] && [ -f "$$FROZEN_TEST_RECORD/publication-stop" ]; then rm "$$FROZEN_TEST_RECORD/publication-stop"; exit 143; fi
MAKE
cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" != publish ]]; then exec "$FROZEN_TEST_CARGO" "$@"; fi
[[ "$*" == *' --locked '* && "$*" == *' --registry crates-io'* ]]
package=''
while [[ $# != 0 ]]; do
    if [[ "$1" == -p ]]; then package="$2"; break; fi
    shift
done
[[ -n "$package" ]]
version="$(.tools/host/bin/yq -p toml -o json -r '.package.version' "crates/$package/Cargo.toml")"
printf '%s\t%s\t%s\n' "$package" "$version" "$(git rev-parse HEAD)" >> "$FROZEN_TEST_RECORD/published"
CARGO
cat > "$fixture/bin/curl" <<'REGISTRY'
#!/usr/bin/env bash
set -euo pipefail
url=''
for arg in "$@"; do
    case "$arg" in https://crates.io/api/v1/crates/icydb*/*) url="$arg" ;; esac
done
[[ -n "$url" ]] || exec "$FROZEN_TEST_CURL" "$@"
pair="${url#https://crates.io/api/v1/crates/}"
if [[ -f "$FROZEN_TEST_RECORD/published" ]] &&
   awk -F '\t' -v package="${pair%/*}" -v version="${pair##*/}" '$1==package && $2==version {found=1} END {exit !found}' "$FROZEN_TEST_RECORD/published"; then
    printf 200
else
    printf 404
fi
REGISTRY
chmod +x "$fixture/bin/"{cargo,curl}
export PATH="$fixture/bin:$PATH"
git init -q -b main "$normal"
git -C "$normal" config user.name 'IcyDB frozen qualification'
git -C "$normal" config user.email qualification@example.invalid
git -C "$normal" config core.hooksPath /dev/null
git -C "$normal" add -- .
git -C "$normal" commit -qm 'Reproducible IcyDB frozen-hook qualification'
git init -q --bare "$fixture/remote.git"
git -C "$normal" remote add origin "$fixture/remote.git"
git -C "$normal" push -q origin main
source="$(git -C "$normal" rev-parse HEAD)"
if make -s -C "$normal" release-patch RELEASE_GROUP=cli RELEASE_FROZEN_GROUPS=cli,database > "$fixture/order-refusal.log" 2>&1; then exit 1; fi
: > "$fixture/fail-once"
: > "$fixture/publication-stop"
if make --no-print-directory -C "$normal" release-patch RELEASE_GROUP=database \
    RELEASE_FROZEN_GROUPS=database,cli RELEASE_FROZEN_PUBLISH_TARGET=publish > "$fixture/interrupted.log" 2>&1; then exit 1; fi
intents=("$normal"/.git/release-state/frozen/batch-*/intent.json)
[[ ${#intents[@]} == 1 && -f "${intents[0]}" ]]
jq -e '.phase=="releasing" and .index==0' "${intents[0]}" >/dev/null
[[ ! -f "$fixture/fail-once" && "$(git -C "$normal" rev-parse HEAD)" == "$source" ]]
cp "$normal/Cargo.lock" "$fixture/expected-deferred-Cargo.lock"
if cmp -s "$fixture/captured-Cargo.lock" "$fixture/expected-deferred-Cargo.lock"; then exit 1; fi
batch="$(basename "${intents[0]%/*}")"
if make --no-print-directory -C "$normal" release-batch-resume "BATCH=$batch" > "$fixture/resumed.log" 2>&1; then exit 1; fi
jq -e '.phase=="publishing" and .index==0' "${intents[0]}" >/dev/null
[[ "$(wc -l < "$fixture/published")" == 6 ]]
printf 'database\n' > "$fixture/expected-publications"
cmp "$fixture/expected-publications" "$fixture/publications"
if make --no-print-directory -C "$normal" release-batch-resume "BATCH=$batch" > "$fixture/uncertain.log" 2>&1; then exit 1; fi
[[ "$(wc -l < "$fixture/published")" == 6 ]]
make --no-print-directory -C "$normal" release-batch-resume "BATCH=$batch" PUBLICATION_RESULT=published > "$fixture/reconciled.log" 2>&1
jq -e '.phase=="complete" and .index==2' "${intents[0]}" >/dev/null
[[ "$(git -C "$normal" rev-parse HEAD)" == "$source" ]]
cmp "$normal/Cargo.lock" "$fixture/expected-deferred-Cargo.lock"
printf 'database\ncli\n' > "$fixture/expected-publications"
cmp "$fixture/expected-publications" "$fixture/publications"
[[ "$(wc -l < "$fixture/published")" == 7 ]]
work="$normal/target/release-frozen/$batch"
gitdir="$normal/.git/release-state/frozen/$batch/repository.git"
[[ "$(git --git-dir="$fixture/remote.git" rev-parse main)" == "$(git --git-dir="$gitdir" rev-parse HEAD)" ]]
for group in database cli; do
    version="$(RELEASE_GROUP="$group" CARGO_HOME="$work/.cache/cargo/icydb" bash "$work/scripts/release/version.sh")"
    commit="$(git --git-dir="$gitdir" rev-parse "$group-v$version^{}")"
    RELEASE_GROUP="$group" RELEASE_COMMIT="$commit" CARGO_HOME="$work/.cache/cargo/icydb" \
        bash "$work/scripts/ci/verify-release-gate-receipt.sh" "$commit" --publication
done
make --no-print-directory -C "$normal" release-batch-resume "BATCH=$batch" > "$fixture/completed-resume.log" 2>&1
[[ "$(wc -l < "$fixture/published")" == 7 ]]
cmp "$fixture/expected-publications" "$fixture/publications"
make --no-print-directory -C "$normal" release-batch-status "BATCH=$batch" > "$fixture/status.log" 2>&1
if make --no-print-directory -C "$normal" release-batch-reconcile "BATCH=$batch" > "$fixture/dirty-reconcile.log" 2>&1; then exit 1; fi
[[ "$(git -C "$normal" rev-parse HEAD)" == "$source" ]]
cmp "$normal/Cargo.lock" "$fixture/expected-deferred-Cargo.lock"
# Only this disposable normal checkout is restored for a stale-source probe.
git -C "$normal" restore --source=HEAD -- Cargo.lock
remote_before="$(git --git-dir="$fixture/remote.git" rev-parse main)"
if make --no-print-directory -C "$normal" release-patch RELEASE_GROUP=cli > "$fixture/remote-conflict.log" 2>&1; then exit 1; fi
[[ "$(git --git-dir="$fixture/remote.git" rev-parse main)" == "$remote_before" && "$(wc -l < "$fixture/published")" == 7 ]]
make --no-print-directory -C "$normal" release-batch-reconcile "BATCH=$batch" > "$fixture/reconciled-normal.log" 2>&1
[[ "$(git -C "$normal" rev-parse HEAD)" == "$remote_before" ]]
# IcyDB's repository-local Cargo cache is outside Shared's narrow retirement
# allowance. Keep it and the delivery evidence until separately authorized.
if make --no-print-directory -C "$normal" release-batch-retire "BATCH=$batch" > "$fixture/retire-refusal.log" 2>&1; then exit 1; fi
[[ -d "$work/.cache" && -f "${intents[0]}" && "$(wc -l < "$fixture/published")" == 7 ]]
printf 'Frozen IcyDB hooks passed: shared setup/recovery, supplier order, captured graph, deferred update, archive identities, exact receipts, completed resume, remote conflict, guarded reconciliation and cache-retirement refusal. Registry effects were substitutes.\n'
complete=true
