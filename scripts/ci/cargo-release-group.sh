#!/usr/bin/env bash
# Shared companions: scripts/ci/cargo-release-groups.jq scripts/ci/release-metadata-transaction.sh scripts/ci/finalize-release-changelog.awk scripts/ci/compare-stable-versions.pl
set -euo pipefail
script_dir="${BASH_SOURCE[0]%/*}"
script_dir="$(cd -P "$script_dir" && pwd -P)"
[[ $# -ge 2 && $# -le 4 ]] || { echo 'usage: cargo-release-group.sh version|files|plan|prepare|check|check-graph GROUP' >&2; exit 2; }
operation="$1" group="$2" expected_primary="${3:-}" ordering="${4:-}"
if [[ -n "$ordering" ]]; then
    [[ "$operation" == check && ( "$ordering" == --manifest-order || "$ordering" == --manifest-order-grouped ) ]] || exit 2
fi
case "$operation" in version|files|plan|prepare|check|check-graph) ;; *) exit 2 ;; esac
[[ "$group" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || exit 2
policy=ci/cargo-release-groups.json
# shellcheck source=scripts/ci/release-metadata-transaction.sh
source "$script_dir/release-metadata-transaction.sh"
temporary="$(mktemp -d "${TMPDIR:-/tmp}/cargo-release-group.XXXXXX")"
complete=false
finish() {
    local status=$?
    if [[ "$complete" == true && "$status" == 0 ]]; then rm -rf -- "$temporary"
    else printf 'Release-group evidence retained: %s\n' "$temporary" >&2; fi
    exit "$status"
}
trap finish EXIT
selected_root="$(pwd -P)"
if [[ -n "${RELEASE_COMMIT:-}" && "$operation" != prepare ]]; then
    [[ "$RELEASE_COMMIT" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || exit 1
    [[ "$(git cat-file -t "$RELEASE_COMMIT")" == commit ]] || exit 1
    mkdir "$temporary/source"
    git archive --format=tar "$RELEASE_COMMIT" | tar -xf - -C "$temporary/source"
    selected_root="$(cd "$temporary/source" && pwd -P)"
fi

admit() {
    local root="$1" evidence="$2" manifest package path canonical
    local statuses=(0 0)
    mkdir -p "$evidence"
    for path in "$policy" Cargo.toml Cargo.lock; do
        release_metadata_regular "$root" "$path" || return 1
        mkdir -p "$evidence/inputs"
        if [[ "$path" == */* ]]; then mkdir -p "$evidence/inputs/${path%/*}"; fi
        cp -p -- "$root/$path" "$evidence/inputs/$path" || return
    done
    if (cd "$root" && cargo metadata --locked --offline --format-version 1) \
        2>&1 > "$evidence/metadata.json" | tee "$evidence/metadata.stderr" >&2; then :
    else statuses=("${PIPESTATUS[@]}"); fi
    printf '%s\n' "${statuses[0]}" > "$evidence/cargo-status"
    printf '%s\n' "${statuses[1]}" > "$evidence/log-status"
    [[ "${statuses[0]}" == 0 ]] || return "${statuses[0]}"
    [[ "${statuses[1]}" == 0 ]] || return "${statuses[1]}"
    "${YQ:-yq}" -p toml -o json '.' "$root/Cargo.toml" > "$evidence/root.json" || return
    "${YQ:-yq}" -p toml -o json '.' "$root/Cargo.lock" > "$evidence/lock.json" || return
    printf '{}\n' > "$evidence/manifests.json"
    jq -er '. as $m | .packages[] | select(.id as $id | $m.workspace_members | index($id)) | [.name,.manifest_path] | @tsv' \
        "$evidence/metadata.json" > "$evidence/members" || return
    while IFS=$'\t' read -r package manifest; do
        case "$manifest" in "$root"/*) path="${manifest#"$root"/}" ;; *) echo 'member outside release root' >&2; return 1 ;; esac
        release_metadata_regular "$root" "$path" || return 1
        canonical="$(cd -P "${manifest%/*}" && pwd -P)/${manifest##*/}" || return
        [[ "$canonical" == "$manifest" ]] || return 1
        "${YQ:-yq}" -p toml -o json '.' "$manifest" > "$evidence/member.json" || return
        jq --arg package "$package" --slurpfile value "$evidence/member.json" '. + {($package):$value[0]}' \
            "$evidence/manifests.json" > "$evidence/next.json" || return
        mv "$evidence/next.json" "$evidence/manifests.json" || return
    done < "$evidence/members"
    jq -L "$script_dir" -e --slurpfile config "$root/$policy" --slurpfile root "$evidence/root.json" \
        --slurpfile manifests "$evidence/manifests.json" --slurpfile lock "$evidence/lock.json" --arg group "$group" '
        include "cargo-release-groups";
        if ($config|length) != 1 then error("expected one release-group policy") else
        admit_groups($config[0];$root[0];$manifests[0];$lock[0];$group) end' \
        "$evidence/metadata.json" > "$evidence/group.json" || return
    jq -L "$script_dir" -n --slurpfile group "$evidence/group.json" --slurpfile root "$evidence/root.json" \
        --slurpfile metadata "$evidence/metadata.json" --arg directory "$root" --arg next "${RELEASE_VERSION:-0.0.0}" '
        include "cargo-release-groups";
        $metadata[0] as $m | group_plan($group[0];$root[0];
          [$m.packages[] | select(.id as $id | $m.workspace_members | index($id))];$directory;$next)' \
        > "$evidence/plan.json" || return
    # Check every catalog path, including unused aliases and unselected groups:
    # Cargo does not resolve unused workspace dependency declarations itself.
    jq -L "$script_dir" -r --slurpfile root "$evidence/root.json" '
        include "cargo-release-groups";
        . as $m | member_catalog($root[0];
          [.packages[] | select(.id as $id | $m.workspace_members | index($id))])[] |
        [.package,.value.path] | @tsv' "$evidence/metadata.json" > "$evidence/dependencies" || return
    while IFS=$'\t' read -r package path; do
        canonical="$(cd "$root/$path" && pwd -P)/Cargo.toml" || return
        manifest="$(jq -er --arg name "$package" '.packages[] | select(.name == $name and .source == null) | .manifest_path' "$evidence/metadata.json")" || return
        [[ "$canonical" == "$manifest" ]] || { echo "root dependency path for $package is not its owned member" >&2; return 1; }
    done < "$evidence/dependencies"
    jq -r '(.shared_metadata_files // [])[], (.groups[] | .changelog, (.metadata_files // [])[])' "$root/$policy" > "$evidence/ledgers" || return
    while IFS= read -r path; do release_metadata_regular "$root" "$path" || return 1; done < "$evidence/ledgers"
    jq -r '.files[]' "$evidence/plan.json" > "$evidence/files" || return
    while IFS= read -r path; do release_metadata_regular "$root" "$path" || return 1; done < "$evidence/files"
}

# shellcheck disable=SC2329 # Transaction callbacks.
render_group() {
    local original="$1" candidate="$2" path index=0
    # Admission is bound to the transaction's originals before any candidate write.
    for path in Cargo.toml Cargo.lock; do cmp -- "$original/$path" "$selected_root/$path" || return; done
    cmp -- "$temporary/admitted/inputs/$policy" "$selected_root/$policy" || return
    while IFS= read -r path; do
        jq --arg path "$path" '.manifests[] | select(.path == $path) | {version,dependencies}' \
            "$temporary/admitted/plan.json" > "$temporary/edit-$index.json" || return
        "$CARGO_RELEASE_METADATA_EDITOR" --max-input-bytes "$CARGO_RELEASE_METADATA_MAX_BYTES" \
            group-manifest "$original/$path" "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "$temporary/edit-$index.json" \
            > "$candidate/$path" || return
        index=$((index+1))
    done < "$temporary/manifests"
    local packages=()
    while IFS= read -r path; do packages+=("$path"); done < "$temporary/packages"
    "$CARGO_RELEASE_METADATA_EDITOR" --max-input-bytes "$CARGO_RELEASE_METADATA_MAX_BYTES" lock \
        "$original/Cargo.lock" "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "${packages[@]}" > "$candidate/Cargo.lock" || return
    awk -v version="$RELEASE_VERSION" -v previous="$RELEASE_PREVIOUS" -v date="$RELEASE_DATE" \
        -f "$script_dir/finalize-release-changelog.awk" "$original/$changelog" > "$candidate/$changelog" || return
    # Consumers render/check their additional metadata. Shared preserves it in
    # this transaction and includes it in the exact staging and receipt roster.
    jq -r '.metadata_files[]' "$temporary/admitted/plan.json" > "$temporary/metadata-files" || return
    while IFS= read -r path; do
        cp -p -- "$original/$path" "$candidate/$path" || return
    done < "$temporary/metadata-files"
}

# shellcheck disable=SC2329 # Transaction callback.
check_group_candidate() {
    admit "$selected_root" "$temporary/prepared" || return
    [[ "$(jq -r .previous "$temporary/prepared/plan.json")" == "$RELEASE_VERSION" ]] || return 1
    # Every nonselected package retains its exact identity; full locked metadata
    # also proves changed cross-group requirements still resolve without fetching.
    jq -n -e --slurpfile before "$temporary/admitted/metadata.json" --slurpfile after "$temporary/prepared/metadata.json" \
        --slurpfile plan "$temporary/admitted/plan.json" '
        def unrelated($m): [$m.packages[] | select(.name as $n | $plan[0].members | index($n) | not) | {id,version}] | sort_by(.id);
        unrelated($before[0]) == unrelated($after[0])' > /dev/null
}

if [[ -n "$ordering" ]]; then
    sort_arguments=(sort --workspace --check)
    [[ "$ordering" != --manifest-order-grouped ]] || sort_arguments+=(--grouped)
    (cd "$selected_root" && cargo "${sort_arguments[@]}")
fi
admit "$selected_root" "$temporary/admitted"
if [[ -n "$expected_primary" ]]; then
    [[ "$(jq -r .primary "$temporary/admitted/plan.json")" == "$expected_primary" ]] || { echo 'primary does not belong to selected release group' >&2; exit 1; }
fi
observed="$(jq -r .previous "$temporary/admitted/plan.json")"
changelog="$(jq -r .changelog "$temporary/admitted/plan.json")"
case "$operation" in
    version) printf '%s\n' "$observed" ;;
    files) jq -jr '.files[] | . + "\u0000"' "$temporary/admitted/plan.json" ;;
    plan) cat "$temporary/admitted/plan.json" ;;
    check-graph) ;;
    check)
        [[ "$observed" == "${RELEASE_VERSION:?}" ]] || exit 1
        awk -v heading="## [$RELEASE_VERSION] - ${RELEASE_DATE:?}" '$0 == heading {count++} END {if(count!=1)exit 1}' "$selected_root/$changelog"
        ;;
    prepare)
        [[ "$selected_root" == "$PWD" && "$observed" == "${RELEASE_PREVIOUS:?}" ]] || exit 1
        [[ "$(perl "$script_dir/compare-stable-versions.pl" "${RELEASE_VERSION:?}" "$observed")" == 1 ]] || exit 1
        [[ "${CARGO_RELEASE_METADATA_EDITOR:?}" == /* && -x "$CARGO_RELEASE_METADATA_EDITOR" && -f "$CARGO_RELEASE_METADATA_EDITOR" &&
           "${CARGO_RELEASE_METADATA_MAX_BYTES:?}" =~ ^[1-9][0-9]*$ ]] || exit 2
        jq -r '.manifests[].path' "$temporary/admitted/plan.json" > "$temporary/manifests"
        jq -r '.members[]' "$temporary/admitted/plan.json" > "$temporary/packages"
        files=()
        while IFS= read -r path; do files+=("$path"); done < "$temporary/admitted/files"
        release_metadata_prepare "${TMPDIR:-/tmp}/cargo-group-backup.XXXXXX" render_group check_group_candidate "${files[@]}"
        ;;
    *) exit 2 ;;
esac
complete=true
