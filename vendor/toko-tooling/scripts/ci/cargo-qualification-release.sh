#!/usr/bin/env bash
# Shared companions: scripts/ci/cargo-qualification-release.jq
# Sourced by cargo-release-group.sh. Qualification is an explicit policy opt-in;
# the product plan remains the sole owner of release versions and membership.
# shellcheck disable=SC2154 # Caller supplies policy, script_dir and temporary.
qualification_admit() {
    local root="$1" evidence="$2" path manifest alias directory canonical
    local statuses=(0 0)
    jq -e 'has("qualification_workspace")' "$root/$policy" > /dev/null || return 0
    directory="$root/qualification"
    mkdir -p "$evidence/qualification/inputs" || return
    for path in Cargo.toml Cargo.lock; do
        release_metadata_regular "$root" "qualification/$path" || return 1
        cp -p -- "$directory/$path" "$evidence/qualification/inputs/$path" || return
    done
    if [[ -n "$ordering" ]]; then
        local sort_arguments=(sort --workspace --check)
        [[ "$ordering" != --manifest-order-grouped ]] || sort_arguments+=(--grouped)
        (cd "$directory" && cargo "${sort_arguments[@]}") || return
    fi
    if (cd "$directory" && cargo metadata --locked --offline --format-version 1) \
        2>&1 > "$evidence/qualification/metadata.json" | tee "$evidence/qualification/metadata.stderr" >&2; then :
    else statuses=("${PIPESTATUS[@]}"); fi
    printf '%s\n' "${statuses[0]}" > "$evidence/qualification/cargo-status"
    printf '%s\n' "${statuses[1]}" > "$evidence/qualification/log-status"
    [[ "${statuses[0]}" == 0 ]] || return "${statuses[0]}"
    [[ "${statuses[1]}" == 0 ]] || return "${statuses[1]}"
    "${YQ:-yq}" -p toml -o json '.' "$directory/Cargo.toml" > "$evidence/qualification/root.json" || return
    jq -L "$script_dir" -e --slurpfile product "$evidence/metadata.json" \
        --slurpfile manifest "$evidence/qualification/root.json" --slurpfile plan "$evidence/plan.json" \
        --arg directory "$directory" --arg next "${RELEASE_VERSION:-0.0.0}" '
        include "cargo-qualification-release";
        qualification_plan($product[0]; $manifest[0]; $plan[0]; $directory; $next)' \
        "$evidence/qualification/metadata.json" > "$evidence/qualification/plan.json" || return
    # Resolve every product catalog alias, including declarations not used by Cargo.
    jq -r '.catalog[] | [.alias,.path,.manifest] | @tsv' "$evidence/qualification/plan.json" \
        > "$evidence/qualification/catalog" || return
    while IFS=$'\t' read -r alias path manifest; do
        canonical="$(cd "$directory/$path" && pwd -P)/Cargo.toml" || return
        [[ "$canonical" == "$manifest" ]] || { echo "qualification alias $alias does not resolve to its product member" >&2; return 1; }
    done < "$evidence/qualification/catalog"
    jq -r '. as $m | .packages[] | select(.id as $id | $m.workspace_members | index($id)) | .manifest_path' \
        "$evidence/qualification/metadata.json" > "$evidence/qualification/members" || return
    while IFS= read -r manifest; do
        path="${manifest#"$root"/}"
        release_metadata_regular "$root" "$path" || return 1
        jq -e --arg path "$path" '[.shared_metadata_files[]?, .groups[].metadata_files[]?] |
            index($path) | not' "$root/$policy" > /dev/null || return
        # Product requirements must inherit this workspace's catalog, so no
        # hidden member requirement can lag the selected version transition.
        "${YQ:-yq}" -p toml -o json '.' "$manifest" | jq -L "$script_dir" -e \
            --slurpfile plan "$evidence/qualification/plan.json" --slurpfile product "$evidence/metadata.json" '
            include "cargo-qualification-release";
            qualification_member_catalog($product[0]; $plan[0].catalog)' > /dev/null || return
    done < "$evidence/qualification/members"
    jq --slurpfile qualification "$evidence/qualification/plan.json" '
        .manifests += [$qualification[0].manifest] |
        .files += ["qualification/Cargo.toml", "qualification/Cargo.lock"] |
        .qualification = $qualification[0] |
        if (.files | unique | length) != (.files | length)
        then error("qualification release files overlap existing ownership") else . end' \
        "$evidence/plan.json" > "$evidence/complete-plan.json" || return
    mv -- "$evidence/complete-plan.json" "$evidence/plan.json"
}

# shellcheck disable=SC2154 # Caller supplies the admitted transaction evidence.
qualification_render_lock() {
    local original="$1" candidate="$2" package path
    [[ -f "$temporary/admitted/qualification/plan.json" ]] || return 0
    for path in Cargo.toml Cargo.lock; do
        cmp -- "$temporary/admitted/qualification/inputs/$path" "$original/qualification/$path" || return
    done
    local packages=()
    jq -r '.members[]' "$temporary/admitted/qualification/plan.json" > "$temporary/qualification-packages" || return
    while IFS= read -r package; do packages+=("$package"); done < "$temporary/qualification-packages"
    if [[ ${#packages[@]} == 0 ]]; then
        cp -p -- "$original/qualification/Cargo.lock" "$candidate/qualification/Cargo.lock"
    else
        "$CARGO_RELEASE_METADATA_EDITOR" --max-input-bytes "$CARGO_RELEASE_METADATA_MAX_BYTES" lock \
            "$original/qualification/Cargo.lock" "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "${packages[@]}" \
            > "$candidate/qualification/Cargo.lock"
    fi
}

# shellcheck disable=SC2154 # Caller supplies the admitted transaction evidence.
qualification_check_preserved() {
    [[ -f "$temporary/admitted/qualification/plan.json" ]] || return 0
    jq -n -e --slurpfile before "$temporary/admitted/qualification/metadata.json" \
        --slurpfile after "$temporary/prepared/qualification/metadata.json" \
        --slurpfile product "$temporary/admitted/metadata.json" --slurpfile plan "$temporary/admitted/plan.json" '
        [$product[0].packages[] | select(.name as $n | $plan[0].members | index($n)) | .manifest_path] as $paths |
        def unrelated($m): [$m.packages[] | select(.manifest_path as $p | $paths | index($p) | not) | {id,version}] | sort_by(.id);
        unrelated($before[0]) == unrelated($after[0])' > /dev/null
}
