#!/usr/bin/env bash
# Explicit broad acceptance, never a release or an automatic routine-CI gate.
set -euo pipefail
working_tree=false
group=cli
while [[ $# != 0 ]]; do
    case "$1" in
      --working-tree) working_tree=true; shift ;;
      --group) [[ $# -ge 2 && ( "$2" == cli || "$2" == database ) ]] || exit 2; group="$2"; shift 2 ;;
      *) echo 'usage: qualify-native-validation.sh [--working-tree] [--group cli|database]' >&2; exit 2 ;;
    esac
done
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
bash vendor/toko-tooling/scripts/ci/check-make-execution.sh
[[ "$working_tree" == true || -z "$(git status --porcelain --untracked-files=all)" ]] || {
    echo 'Native mapping qualification requires settled, clean committed input.' >&2; exit 1;
}
[[ ! -L target ]] || { echo 'redirected qualification directory' >&2; exit 1; }
mkdir -p target
attempt="$(mktemp -d "$root/target/release-validation-native.XXXXXX")"
mkdir "$attempt/evidence" "$attempt/templates" "$attempt/scratch"
input_source="$(git rev-parse HEAD)"
printf '%s\n' "$input_source" > "$attempt/evidence/input-source"
complete=false
retain() {
    local status=$?
    [[ "$complete" == true || "$status" != 0 ]] || status=1
    if [[ -d "$attempt/repo/target/release-validation" ]]; then
        cp -R "$attempt/repo/target/release-validation" "$attempt/evidence/validation" || status=1
    fi
    # Only this successful disposable fixture is reclaimable. Failed source,
    # build output and logs remain available for inspection.
    if [[ "$complete" == true && "$status" == 0 ]]; then rm -rf -- "$attempt/repo"; fi
    printf 'Native mapping evidence: %s\n' "$attempt/evidence"
    exit "$status"
}
trap retain EXIT
source_paths=()
if [[ "$working_tree" == true ]]; then
    # Explicit local acceptance snapshots the actual build inputs, never the
    # user's index. Shared checkpoints guard both export and final source custody.
    [[ -z "$(git ls-files --unmerged)" ]] || { echo 'Unmerged candidate inputs.' >&2; exit 1; }
    git ls-files --cached --others --exclude-standard -z > "$attempt/evidence/source-paths"
    git ls-files --stage -z > "$attempt/evidence/source-index"
    git diff --binary HEAD -- > "$attempt/evidence/candidate.patch"
    while IFS= read -r -d '' path; do
        if [[ -e "$root/$path" || -L "$root/$path" ]]; then
            source_paths[${#source_paths[@]}]="$path"
        fi
    done < "$attempt/evidence/source-paths"
    perl vendor/toko-tooling/scripts/ci/validation-input-checkpoint.pl "$root" '' "$attempt/evidence/source-inputs.json" \
        "$root/vendor/toko-tooling/scripts/ci/verify-file-checksum.sh" "${source_paths[@]}"
    printf '%s\n' working-tree-snapshot > "$attempt/evidence/source-kind"
else
    printf '%s\n' committed-source > "$attempt/evidence/source-kind"
fi
# Fixture history must not inherit developer signing, hooks or redirected Git state.
git_context="$(git rev-parse --local-env-vars)"
while IFS= read -r variable; do unset "$variable"; done <<< "$git_context"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$attempt/templates"
git clone --quiet --shared --no-hardlinks --no-checkout "$root" "$attempt/repo"
git -C "$attempt/repo" remote remove origin
git -C "$attempt/repo" checkout --quiet --detach "$input_source"
cd "$attempt/repo"
git config user.name 'IcyDB mapping qualification'
git config user.email qualification@example.invalid
git config core.hooksPath /dev/null
# Identical product input produces the same synthetic source and annotated
# baseline on every native host, independently of runner clocks or identities.
export GIT_AUTHOR_NAME='IcyDB mapping qualification' GIT_COMMITTER_NAME='IcyDB mapping qualification'
export GIT_AUTHOR_EMAIL=qualification@example.invalid GIT_COMMITTER_EMAIL=qualification@example.invalid
export GIT_AUTHOR_DATE GIT_COMMITTER_DATE
GIT_AUTHOR_DATE="$(git show -s --format=%cI "$input_source")"
GIT_COMMITTER_DATE="$GIT_AUTHOR_DATE"
if [[ "$working_tree" == true ]]; then
    if [[ -s "$attempt/evidence/candidate.patch" ]]; then git apply --binary "$attempt/evidence/candidate.patch"; fi
    # Export untracked candidate files and final working bytes as well as tracked
    # edits. Missing tracked paths were already removed by the admitted patch.
    for path in "${source_paths[@]}"; do
        mkdir -p "$(dirname "$path")"
        cp -p "$root/$path" "$path"
    done
    perl vendor/toko-tooling/scripts/ci/validation-input-checkpoint.pl "$PWD" "$attempt/evidence/source-inputs.json" \
        "$attempt/evidence/exported-inputs.json" "$PWD/vendor/toko-tooling/scripts/ci/verify-file-checksum.sh"
    git add --all -- .
    git commit --quiet --allow-empty -m 'Settled IcyDB candidate snapshot for local qualification'
    git rev-parse HEAD > "$attempt/evidence/candidate-source"
fi
baseline="$group-v999.0.0"
git tag -am 'Synthetic qualification baseline; never a release baseline' "$baseline"
# Controlled real-product edits exercise the ownership mapping without changing
# dependencies, features, generated identities, retained bytes or release state.
if [[ "$group" == cli ]]; then
    perl -0777 -pe 's/about = "Developer CLI tools for IcyDB"/about = "Developer CLI tools for IcyDB (mapping qualification)"/g == 1 or die "CLI presentation fixture changed\n"' \
        crates/icydb-cli/src/cli/mod.rs > "$attempt/cli-presentation.rs"
    cp "$attempt/cli-presentation.rs" crates/icydb-cli/src/cli/mod.rs
    git add -- crates/icydb-cli/src/cli/mod.rs
else
    printf '\n// Controlled database mapping qualification.\n' >> crates/icydb-core/src/db/database_format/mod.rs
    git add -- crates/icydb-core/src/db/database_format/mod.rs
fi
git commit --quiet -m "Controlled $group ownership qualification"
export RELEASE_GROUP="$group" RELEASE_SOURCE
RELEASE_SOURCE="$(git rev-parse HEAD)"
git bundle create "$attempt/evidence/source.bundle" HEAD "refs/tags/$baseline" "^$input_source" \
    > "$attempt/evidence/bundle.log" 2>&1
# Reuse acquisition only, not product build output or validation evidence.
# Shared's existing installers own active executable selections in this fixture.
source_cache="$(make --no-print-directory -s -C "$root" print-cargo-home)"
[[ -d "$source_cache" ]] || { echo 'Prepare the locked source cache first.' >&2; exit 1; }
mkdir -p .cache/cargo
ln -s "$source_cache" .cache/cargo/icydb
export TMPDIR="$attempt/scratch"
make --no-print-directory install-tools > "$attempt/evidence/setup.log" 2>&1
make --no-print-directory fetch >> "$attempt/evidence/setup.log" 2>&1
git diff --exit-code --quiet
export PATH="$PWD/.tools/host/bin:$PWD/.tools/ic/bin:$PWD/.tools/rust/common/bin:$PATH"
export CARGO_HOME="$PWD/.cache/cargo/icydb"
cli_version="$(RELEASE_GROUP=cli bash scripts/release/version.sh)"
package_directory="$(make --no-print-directory -s print-cargo-target-dir)/package"
# Admit the selected CLI archive before running the additional engine gates.
for scope in auto workspace; do
    bash scripts/release/qualify-validation-plan.sh "$group" "$baseline" "$scope" \
        > "$attempt/evidence/$scope.log" 2>&1
    archive="$package_directory/icydb-cli-$cli_version.crate"
    # Database selection keeps CLI process/library consumers through workspace
    # tests; only full scope adds its supplemental archive stage. Publication
    # still verifies final database archives against freshly published siblings.
    if [[ "$scope" == workspace || "$group" == cli ]]; then
        [[ -f "$archive" && ! -L "$archive" ]] || exit 1
        cp -p "$archive" "$attempt/evidence/$scope-cli-package.crate"
    fi
done
if [[ "$group" == cli ]]; then
    cmp "$attempt/evidence/auto-cli-package.crate" "$attempt/evidence/workspace-cli-package.crate"
fi
tar -xOf "$attempt/evidence/workspace-cli-package.crate" "icydb-cli-$cli_version/.cargo_vcs_info.json" \
    > "$attempt/evidence/archive-vcs.json"
jq -e --arg source "$RELEASE_SOURCE" '.git.sha1==$source and (.git.dirty // false)==false and
    .path_in_vcs=="crates/icydb-cli"' "$attempt/evidence/archive-vcs.json" > /dev/null
tar -xOf "$attempt/evidence/workspace-cli-package.crate" "icydb-cli-$cli_version/Cargo.toml.orig" \
    > "$attempt/evidence/archive-Cargo.toml.orig"
git show "$RELEASE_SOURCE:crates/icydb-cli/Cargo.toml" > "$attempt/evidence/source-Cargo.toml"
cmp "$attempt/evidence/source-Cargo.toml" "$attempt/evidence/archive-Cargo.toml.orig"
bash vendor/toko-tooling/scripts/ci/verify-file-checksum.sh --print sha256 "$attempt/evidence/workspace-cli-package.crate" \
    > "$attempt/evidence/archive-sha256"
full="$(sed -n '\|^target/release-validation/qualification\.[A-Za-z0-9]*/receipt.json$|p' "$attempt/evidence/workspace.log")"
selected="$(sed -n '\|^target/release-validation/qualification\.[A-Za-z0-9]*/receipt.json$|p' "$attempt/evidence/auto.log")"
[[ -n "$full" && -n "$selected" && "$full" != *$'\n'* && "$selected" != *$'\n'* ]] || exit 1
perl scripts/release/compare-validation-plans.pl "${full%/receipt.json}" "${selected%/receipt.json}" \
    > "$attempt/evidence/comparison.json"
[[ "$(git -C "$root" rev-parse HEAD)" == "$input_source" ]] || {
    echo 'Original source changed during qualification.' >&2; exit 1;
}
if [[ "$working_tree" == true ]]; then
    git -C "$root" ls-files --cached --others --exclude-standard -z > "$attempt/evidence/final-source-paths"
    git -C "$root" ls-files --stage -z > "$attempt/evidence/final-source-index"
    git -C "$root" diff --binary HEAD -- > "$attempt/evidence/final-candidate.patch"
    cmp "$attempt/evidence/source-paths" "$attempt/evidence/final-source-paths"
    cmp "$attempt/evidence/source-index" "$attempt/evidence/final-source-index"
    cmp "$attempt/evidence/candidate.patch" "$attempt/evidence/final-candidate.patch"
    perl "$root/vendor/toko-tooling/scripts/ci/validation-input-checkpoint.pl" "$root" "$attempt/evidence/source-inputs.json" \
        "$attempt/evidence/final-source-inputs.json" "$root/vendor/toko-tooling/scripts/ci/verify-file-checksum.sh"
else
    [[ -z "$(git -C "$root" status --porcelain --untracked-files=all)" ]] || exit 1
fi
complete=true
