//! Module: metrics report rendering.
//! Responsibility: render generated event metrics reports into CLI text and tables.
//! Does not own: endpoint calls, candid decoding, or config surface gating.
//! Boundary: receives decoded metrics reports and returns user-facing text.

use icydb::metrics::{
    CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport, EntitySummary,
    EventCounters, EventOps, EventReport, compact_metric_code,
};

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

pub(super) fn render_metrics_report(report: &CompactMetricsReport) -> String {
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

pub(super) fn render_extended_metrics_report(report: &EventReport) -> String {
    let mut output = String::new();

    append_extended_metrics_report_header(&mut output, report);

    if let Some(counters) = report.counters() {
        append_extended_metrics_counters(&mut output, counters);
    } else {
        output.push_str("  counters: none\n");
    }
    output.push('\n');

    let entity_rows = report
        .entity_counters()
        .iter()
        .map(extended_metrics_entity_row)
        .collect::<Vec<_>>();
    append_metrics_entity_table(&mut output, entity_rows.as_slice());

    output
}

fn metrics_entity_row(entity: &CompactEntityMetrics) -> MetricsEntityRow {
    let metrics = entity.metrics();

    [
        entity.path().to_string(),
        metric_value(metrics, compact_metric_code::LOAD_CALLS).to_string(),
        metric_value(metrics, compact_metric_code::SAVE_CALLS).to_string(),
        metric_value(metrics, compact_metric_code::DELETE_CALLS).to_string(),
        metric_value(metrics, compact_metric_code::EXEC_SUCCESS).to_string(),
        metric_value(metrics, compact_metric_code::EXEC_ERRORS).to_string(),
    ]
}

fn extended_metrics_entity_row(entity: &EntitySummary) -> MetricsEntityRow {
    [
        entity.path().to_string(),
        entity.load_calls().to_string(),
        entity.save_calls().to_string(),
        entity.delete_calls().to_string(),
        entity.exec_success().to_string(),
        entity_exec_errors(entity).to_string(),
    ]
}

fn append_metrics_report_header(output: &mut String, report: &CompactMetricsReport) {
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

fn append_extended_metrics_report_header(output: &mut String, report: &EventReport) {
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

fn append_metrics_counters(output: &mut String, counters: &CompactEventCounters) {
    let metrics = counters.metrics();
    output.push_str(
        format!(
            "  window: {}..{} ({} ms)\n  calls: load={} save={} delete={}\n  execution: success={} errors={} aborted={}\n  rows: loaded={} saved={} deleted={} scanned={} filtered={} emitted={}\n  sql writes: insert={} insert_select={} update={} delete={} matched={} mutated={} returning={}\n  cache: query_plan_hits={} query_plan_misses={} sql_hits={} sql_misses={}\n",
            counters.window_start_ms(),
            counters.window_end_ms(),
            counters.window_duration_ms(),
            metric_value(metrics, compact_metric_code::LOAD_CALLS),
            metric_value(metrics, compact_metric_code::SAVE_CALLS),
            metric_value(metrics, compact_metric_code::DELETE_CALLS),
            metric_value(metrics, compact_metric_code::EXEC_SUCCESS),
            metric_value(metrics, compact_metric_code::EXEC_ERRORS),
            metric_value(metrics, compact_metric_code::EXEC_ABORTED),
            metric_value(metrics, compact_metric_code::ROWS_LOADED),
            metric_value(metrics, compact_metric_code::ROWS_SAVED),
            metric_value(metrics, compact_metric_code::ROWS_DELETED),
            metric_value(metrics, compact_metric_code::ROWS_SCANNED),
            metric_value(metrics, compact_metric_code::ROWS_FILTERED),
            metric_value(metrics, compact_metric_code::ROWS_EMITTED),
            metric_value(metrics, compact_metric_code::SQL_INSERT_CALLS),
            metric_value(metrics, compact_metric_code::SQL_INSERT_SELECT_CALLS),
            metric_value(metrics, compact_metric_code::SQL_UPDATE_CALLS),
            metric_value(metrics, compact_metric_code::SQL_DELETE_CALLS),
            metric_value(metrics, compact_metric_code::SQL_WRITE_MATCHED_ROWS),
            metric_value(metrics, compact_metric_code::SQL_WRITE_MUTATED_ROWS),
            metric_value(metrics, compact_metric_code::SQL_WRITE_RETURNING_ROWS),
            metric_value(metrics, compact_metric_code::CACHE_SHARED_QUERY_PLAN_HITS),
            metric_value(metrics, compact_metric_code::CACHE_SHARED_QUERY_PLAN_MISSES),
            metric_value(metrics, compact_metric_code::CACHE_SQL_COMPILED_COMMAND_HITS),
            metric_value(metrics, compact_metric_code::CACHE_SQL_COMPILED_COMMAND_MISSES),
        )
        .as_str(),
    );
}

fn append_extended_metrics_counters(output: &mut String, counters: &EventCounters) {
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

fn metric_value(metrics: &[CompactMetric], code: u16) -> u64 {
    metrics
        .iter()
        .find(|metric| metric.code() == code)
        .map_or(0, CompactMetric::value)
}

const fn ops_exec_errors(ops: &EventOps) -> u64 {
    ops.exec_error_corruption()
        .saturating_add(ops.exec_error_incompatible_persisted_format())
        .saturating_add(ops.exec_error_not_found())
        .saturating_add(ops.exec_error_internal())
        .saturating_add(ops.exec_error_conflict())
        .saturating_add(ops.exec_error_unsupported())
        .saturating_add(ops.exec_error_invariant_violation())
}

const fn entity_exec_errors(entity: &EntitySummary) -> u64 {
    entity
        .exec_error_corruption()
        .saturating_add(entity.exec_error_incompatible_persisted_format())
        .saturating_add(entity.exec_error_not_found())
        .saturating_add(entity.exec_error_internal())
        .saturating_add(entity.exec_error_conflict())
        .saturating_add(entity.exec_error_unsupported())
        .saturating_add(entity.exec_error_invariant_violation())
}
