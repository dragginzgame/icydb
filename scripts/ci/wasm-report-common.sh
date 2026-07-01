wasm_report_default_canisters() {
    printf '%s\n' \
        default_empty \
        default_empty_metrics \
        one_entity_fluent_rows \
        one_entity_fluent_execute \
        one_entity_sql_query \
        ten_entity_fluent_rows
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
