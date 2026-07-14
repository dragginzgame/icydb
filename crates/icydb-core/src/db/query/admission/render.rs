//! Module: db::query::admission::render
//! Responsibility: stable verbose EXPLAIN text rendering for read admission.
//! Does not own: admission policy, plan summary extraction, or diagnostics.
//! Boundary: keeps display formatting separate from admission decisions.

use std::fmt::Write as _;

use super::{QueryAdmissionRejection, QueryAdmissionSummary};

pub(super) fn render_text_block(summary: &QueryAdmissionSummary) -> String {
    let mut out = String::from("admission:");
    push_text_field(&mut out, "lane", summary.lane().as_str());
    push_text_field(&mut out, "decision", summary.decision().as_str());
    push_text_field(
        &mut out,
        "reason",
        summary
            .rejection()
            .map_or("none", QueryAdmissionRejection::as_str),
    );
    push_text_field(&mut out, "plan_shape", summary.plan_shape().as_str());
    push_text_field(
        &mut out,
        "selected_access",
        summary.selected_access().as_str(),
    );
    push_text_field(
        &mut out,
        "selected_index",
        summary.selected_index().unwrap_or("none"),
    );
    push_text_option_u32(&mut out, "limit", summary.limit());
    push_text_option_u32(&mut out, "offset", summary.offset());
    push_text_option_u64(&mut out, "scan_bound", summary.scan_bound());
    push_text_field(
        &mut out,
        "scan_bound_kind",
        summary.scan_bound_kind().as_str(),
    );
    push_text_option_u32(&mut out, "returned_row_bound", summary.returned_row_bound());
    push_text_field(
        &mut out,
        "returned_row_bound_kind",
        summary.returned_row_bound_kind().as_str(),
    );
    push_text_option_u32(
        &mut out,
        "primary_key_input_terms",
        summary.primary_key_input_terms(),
    );
    push_text_option_u32(
        &mut out,
        "primary_key_input_payload_bytes",
        summary.primary_key_input_payload_bytes(),
    );
    push_text_field(
        &mut out,
        "residual_filter",
        summary.residual_filter().as_str(),
    );
    push_text_field(&mut out, "ordering", summary.ordering().as_str());
    push_text_bool(
        &mut out,
        "materialized_sort",
        summary.materialization().materialized_sort(),
    );
    push_text_option_u32(
        &mut out,
        "materialized_rows",
        summary.materialization().materialized_rows(),
    );
    push_text_field(
        &mut out,
        "materialized_row_bound_kind",
        summary.materialization().row_bound_kind().as_str(),
    );

    if let Some(grouped) = summary.grouped() {
        push_text_bool(&mut out, "grouped", true);
        push_text_u64(
            &mut out,
            "group_field_count",
            u64::from(grouped.group_field_count()),
        );
        push_text_u64(
            &mut out,
            "aggregate_count",
            u64::from(grouped.aggregate_count()),
        );
        push_text_u64(
            &mut out,
            "distinct_aggregate_count",
            u64::from(grouped.distinct_aggregate_count()),
        );
        push_text_u64(&mut out, "max_groups", grouped.max_groups());
        push_text_u64(&mut out, "max_group_bytes", grouped.max_group_bytes());
        push_text_bool(&mut out, "having_filter", grouped.has_having_filter());
    } else {
        push_text_bool(&mut out, "grouped", false);
    }

    out
}

fn push_text_field(out: &mut String, key: &str, value: &str) {
    out.push('\n');
    out.push_str("  ");
    out.push_str(key);
    out.push('=');
    out.push_str(value);
}

fn push_text_bool(out: &mut String, key: &str, value: bool) {
    push_text_field(out, key, if value { "true" } else { "false" });
}

fn push_text_u64(out: &mut String, key: &str, value: u64) {
    out.push('\n');
    out.push_str("  ");
    out.push_str(key);
    out.push('=');
    let _ = write!(out, "{value}");
}

fn push_text_option_u32(out: &mut String, key: &str, value: Option<u32>) {
    match value {
        Some(value) => push_text_u64(out, key, u64::from(value)),
        None => push_text_field(out, key, "none"),
    }
}

fn push_text_option_u64(out: &mut String, key: &str, value: Option<u64>) {
    match value {
        Some(value) => push_text_u64(out, key, value),
        None => push_text_field(out, key, "none"),
    }
}
