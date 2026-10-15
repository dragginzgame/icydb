# shellcheck shell=bash

# Shared Tooling owns portable file hashing; report subjects and schema stay local.
wasm_report_sha256() {
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$1"
}

# One product-owned roster serves Rust admission and portable shell selection.
wasm_report_default_canisters() {
    awk -F '\t' '$2 == "default" { print $1 }' "$ROOT/testing/build/src/wasm_measurement_subjects.tsv"
}

wasm_report_canister_is_maintained_subject() {
    local subject _selection
    while IFS=$'\t' read -r subject _selection; do
        [[ "$subject" != "$1" ]] || return 0
    done < "$ROOT/testing/build/src/wasm_measurement_subjects.tsv"
    return 1
}

wasm_report_sql_variants() {
    case "$1:${2:-no}" in
        both:yes)
            printf '%s\n' sql-on sql-off
            ;;
        both:*)
            return 2
            ;;
        sql-on:*)
            printf '%s\n' sql-on
            ;;
        sql-off:*)
            printf '%s\n' sql-off
            ;;
        *)
            return 1
            ;;
    esac
}

wasm_report_size_suffix() {
    local variant_count="${2:-1}"

    case "$1" in
        sql-off)
            printf '%s\n' ".sql-off"
            ;;
        sql-on)
            (( variant_count <= 1 )) || printf '%s\n' ".sql-on"
            ;;
        *)
            return 1
            ;;
    esac
}
