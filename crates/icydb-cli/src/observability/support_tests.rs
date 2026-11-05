use crate::observability::{method_error as base_method_error, metrics, render, schema, snapshot};

pub(crate) fn method_error(
    label: &str,
    environment: &str,
    canister: &str,
    method: &str,
    detail: &str,
) -> String {
    base_method_error(label, environment, canister, method, detail)
}

pub(crate) fn render_field_list(fields: &[String]) -> String {
    render::render_field_list(fields)
}

pub(crate) fn render_metrics_report(report: &icydb::metrics::MetricsReport) -> String {
    metrics::render_metrics_report(report)
}

pub(crate) fn render_schema_report(report: &[icydb::db::EntitySchemaDescription]) -> String {
    schema::render_schema_report(report)
}

pub(crate) fn render_snapshot_report(report: &icydb::db::StorageReport) -> String {
    snapshot::render_snapshot_report(report)
}

pub(crate) const fn yes_no(value: bool) -> &'static str {
    render::yes_no(value)
}
