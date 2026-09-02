# shellcheck shell=bash

wasm_report_default_canisters() {
    printf '%s\n' \
        default_empty \
        default_empty_metrics \
        default_empty_metrics_extended \
        one_entity_dynamic_query \
        one_entity_reachable_operations \
        one_entity_typed_query \
        one_entity_sql_query \
        request_future_scale \
        ten_entity_typed_query \
        ten_entity_reachable_operations \
        sql_perf \
        sql
}

wasm_report_canister_is_maintained_subject() {
    case "$1" in
        default_empty|default_empty_metrics|default_empty_metrics_extended|group_path_sql_query|one_entity_dynamic_query|one_entity_reachable_operations|one_entity_typed_query|one_entity_sql_query|request_future_scale|ten_entity_typed_query|ten_entity_reachable_operations|sql_perf|sql)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
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
