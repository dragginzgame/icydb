//! Module: metrics report rendering.
//! Responsibility: render generated event metrics reports into CLI text and tables.
//! Does not own: endpoint calls, candid decoding, or config surface gating.
//! Boundary: receives decoded metrics reports and returns user-facing text.

use icydb::metrics::{EventCounters, EventReport};

use crate::table::{ColumnAlign, append_indented_table};

use super::super::render::yes_no;

type MetricsEntityRow = [String; 6];

const METRICS_ENTITY_HEADERS: [&str; 6] = ["entity", "load", "save", "delete", "success", "errors"];
const METRICS_ENTITY_ALIGNMENTS: [ColumnAlign; 6] = [
    ColumnAlign::Left,
    ColumnAlign::Right,
    ColumnAlign::Right,
    ColumnAlign::Right,
    ColumnAlign::Right,
    ColumnAlign::Right,
];

pub(super) fn render_metrics_report(report: &EventReport) -> String {
    let mut output = String::new();

    append_metrics_report_header(&mut output, report);

    if let Some(counters) = report.counters() {
        append_metrics_counters(&mut output, counters);
    } else {
        output.push_str("  counters: none\n");
    }
    output.push('\n');

    let entity_rows = report
        .entity_counters()
        .iter()
        .map(metrics_entity_row)
        .collect::<Vec<_>>();
    append_metrics_entity_table(&mut output, entity_rows.as_slice());

    output
}

fn metrics_entity_row(entity: &icydb::metrics::EntitySummary) -> MetricsEntityRow {
    [
        entity.path().to_string(),
        entity.load_calls().to_string(),
        entity.save_calls().to_string(),
        entity.delete_calls().to_string(),
        entity.exec_success().to_string(),
        entity_exec_errors(entity).to_string(),
    ]
}

fn append_metrics_report_header(output: &mut String, report: &EventReport) {
    output.push_str("IcyDB metrics\n");
    output.push_str(
        format!(
            "  active window start ms: {}\n  requested window start ms: {}\n  window filter matched: {}\n  entities: {}\n",
            report.active_window_start_ms(),
            optional_u64(report.requested_window_start_ms()),
            yes_no(report.window_filter_matched()),
            report.entity_counters().len(),
        )
        .as_str(),
    );
}

fn append_metrics_counters(output: &mut String, counters: &EventCounters) {
    let ops = counters.ops();
    output.push_str(
        format!(
            "  window: {}..{} ({} ms)\n  calls: load={} save={} delete={}\n  execution: success={} errors={} aborted={}\n  rows: loaded={} saved={} deleted={} scanned={} filtered={} emitted={}\n  sql writes: insert={} insert_select={} update={} delete={} matched={} mutated={} returning={}\n  cache: query_plan_hits={} query_plan_misses={} sql_hits={} sql_misses={}\n",
            counters.window_start_ms(),
            counters.window_end_ms(),
            counters.window_duration_ms(),
            ops.load_calls(),
            ops.save_calls(),
            ops.delete_calls(),
            ops.exec_success(),
            ops_exec_errors(ops),
            ops.exec_aborted(),
            ops.rows_loaded(),
            ops.rows_saved(),
            ops.rows_deleted(),
            ops.rows_scanned(),
            ops.rows_filtered(),
            ops.rows_emitted(),
            ops.sql_insert_calls(),
            ops.sql_insert_select_calls(),
            ops.sql_update_calls(),
            ops.sql_delete_calls(),
            ops.sql_write_matched_rows(),
            ops.sql_write_mutated_rows(),
            ops.sql_write_returning_rows(),
            ops.cache_shared_query_plan_hits(),
            ops.cache_shared_query_plan_misses(),
            ops.cache_sql_compiled_command_hits(),
            ops.cache_sql_compiled_command_misses(),
        )
        .as_str(),
    );
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn append_metrics_entity_table(output: &mut String, rows: &[MetricsEntityRow]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &METRICS_ENTITY_HEADERS,
        rows,
        &METRICS_ENTITY_ALIGNMENTS,
    );
}

const fn ops_exec_errors(ops: &icydb::metrics::EventOps) -> u64 {
    ops.exec_error_corruption()
        .saturating_add(ops.exec_error_incompatible_persisted_format())
        .saturating_add(ops.exec_error_not_found())
        .saturating_add(ops.exec_error_internal())
        .saturating_add(ops.exec_error_conflict())
        .saturating_add(ops.exec_error_unsupported())
        .saturating_add(ops.exec_error_invariant_violation())
}

const fn entity_exec_errors(entity: &icydb::metrics::EntitySummary) -> u64 {
    entity
        .exec_error_corruption()
        .saturating_add(entity.exec_error_incompatible_persisted_format())
        .saturating_add(entity.exec_error_not_found())
        .saturating_add(entity.exec_error_internal())
        .saturating_add(entity.exec_error_conflict())
        .saturating_add(entity.exec_error_unsupported())
        .saturating_add(entity.exec_error_invariant_violation())
}
