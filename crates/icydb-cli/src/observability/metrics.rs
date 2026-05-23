//! Module: metrics command handling.
//! Responsibility: call generated metrics endpoints and render human metrics reports.
//! Does not own: config surface gating, generic ICP command construction, or other observability reports.
//! Boundary: exposes the metrics command runner and test-covered report helpers through observability.

use candid::Decode;
use icydb::metrics::{EventCounters, EventReport};

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    config::{METRICS_ENDPOINT, METRICS_RESET_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
    table::{ColumnAlign, append_indented_table},
};

use super::{call_query, call_update, render::yes_no};

/// Read or reset the generated metrics endpoints.
pub(super) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    let endpoint = if args.reset() {
        METRICS_RESET_ENDPOINT
    } else {
        METRICS_ENDPOINT
    };
    require_configured_endpoint(target.canister_name(), endpoint)?;
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let candid_arg = metrics_candid_arg(args.window_start_ms());
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        endpoint.method(),
        candid_arg.as_str(),
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::metrics::EventReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_metrics_report(&report));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics method '{}' failed on canister '{}' in environment '{}': {err}",
            endpoint.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    let candid_bytes = call_update(
        target.environment(),
        target.canister_name(),
        METRICS_RESET_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(candid_bytes.as_slice(), Result<(), icydb::Error>)
        .map_err(|err| err.to_string())?;

    match response {
        Ok(()) => {
            println!(
                "Reset metrics on canister '{}' in environment '{}'.",
                target.canister_name(),
                target.environment(),
            );

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics reset method '{}' failed on canister '{}' in environment '{}': {err}",
            METRICS_RESET_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

pub(super) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
    match window_start_ms {
        Some(value) => format!("(opt ({value} : nat64))"),
        None => "(null)".to_string(),
    }
}

pub(super) fn render_metrics_report(report: &EventReport) -> String {
    let mut output = String::new();

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

    if let Some(counters) = report.counters() {
        append_metrics_counters(&mut output, counters);
    } else {
        output.push_str("  counters: none\n");
    }
    output.push('\n');

    let entity_rows = report
        .entity_counters()
        .iter()
        .map(|entity| {
            [
                entity.path().to_string(),
                entity.load_calls().to_string(),
                entity.save_calls().to_string(),
                entity.delete_calls().to_string(),
                entity.exec_success().to_string(),
                entity_exec_errors(entity).to_string(),
            ]
        })
        .collect::<Vec<_>>();
    append_metrics_entity_table(&mut output, entity_rows.as_slice());

    output
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

fn append_metrics_entity_table(output: &mut String, rows: &[[String; 6]]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &["entity", "load", "save", "delete", "success", "errors"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
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
