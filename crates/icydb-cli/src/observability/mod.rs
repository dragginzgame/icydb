//! Module: observability command integration.
//! Responsibility: dispatch metrics, schema, schema-check, and snapshot canister calls.
//! Does not own: endpoint configuration, ICP process construction, or CLI argument parsing.
//! Boundary: decodes raw canister responses and delegates report rendering to submodules.

mod metrics;
mod render;
mod schema;
mod schema_check;
mod snapshot;

use crate::cli::{CanisterTarget, MetricsArgs};
use crate::icp::{call_query_hex, call_update_hex};

pub(crate) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    metrics::run_metrics_command(args)
}

pub(crate) fn run_schema_show_command(target: CanisterTarget) -> Result<(), String> {
    schema::run_schema_show_command(target)
}

pub(crate) fn run_schema_check_command(target: CanisterTarget) -> Result<(), String> {
    schema_check::run_schema_check_command(target)
}

pub(crate) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    snapshot::run_snapshot_command(target)
}

fn call_query(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    call_query_hex(
        environment,
        canister,
        method,
        candid_arg,
        call_error_mapper("query", environment, canister, method),
    )
}

fn call_update(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    call_update_hex(
        environment,
        canister,
        method,
        candid_arg,
        call_error_mapper("update", environment, canister, method),
    )
}

fn call_error_mapper<'a>(
    label: &'a str,
    environment: &'a str,
    canister: &'a str,
    method: &'a str,
) -> impl FnOnce(&str) -> String + 'a {
    move |stderr| method_error(label, environment, canister, method, stderr)
}

fn endpoint_result_error(
    label: &str,
    target: &CanisterTarget,
    method: &str,
    err: icydb::Error,
) -> String {
    let detail = err.to_string();

    method_error(
        label,
        target.environment(),
        target.canister_name(),
        method,
        detail.as_str(),
    )
}

fn method_error(
    label: &str,
    environment: &str,
    canister: &str,
    method: &str,
    detail: &str,
) -> String {
    format!(
        "IcyDB {label} method '{method}' failed on canister '{canister}' in environment '{environment}': {detail}",
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) fn method_error(
        label: &str,
        environment: &str,
        canister: &str,
        method: &str,
        detail: &str,
    ) -> String {
        super::method_error(label, environment, canister, method, detail)
    }

    pub(crate) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
        super::metrics::metrics_candid_arg(window_start_ms)
    }

    pub(crate) fn decode_metrics_report(
        candid_bytes: &[u8],
    ) -> Result<Result<icydb::metrics::EventReport, icydb::Error>, String> {
        super::metrics::decode_metrics_report(candid_bytes)
    }

    pub(crate) fn decode_metrics_reset_response(
        candid_bytes: &[u8],
    ) -> Result<Result<(), icydb::Error>, String> {
        super::metrics::decode_metrics_reset_response(candid_bytes)
    }

    pub(crate) fn decode_schema_report(
        candid_bytes: &[u8],
    ) -> Result<Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error>, String> {
        super::schema::decode_schema_report(candid_bytes)
    }

    pub(crate) fn decode_schema_check_report(
        candid_bytes: &[u8],
    ) -> Result<Result<Vec<icydb::db::EntitySchemaCheckDescription>, icydb::Error>, String> {
        super::schema_check::decode_schema_check_report(candid_bytes)
    }

    pub(crate) fn decode_snapshot_report(
        candid_bytes: &[u8],
    ) -> Result<Result<icydb::db::StorageReport, icydb::Error>, String> {
        super::snapshot::decode_snapshot_report(candid_bytes)
    }

    pub(crate) fn render_field_list(fields: &[String]) -> String {
        super::render::render_field_list(fields)
    }

    pub(crate) fn render_metrics_report(report: &icydb::metrics::EventReport) -> String {
        super::metrics::render_metrics_report(report)
    }

    pub(crate) fn render_schema_report(report: &[icydb::db::EntitySchemaDescription]) -> String {
        super::schema::render_schema_report(report)
    }

    pub(crate) fn render_schema_check_report(
        report: &[icydb::db::EntitySchemaCheckDescription],
    ) -> String {
        super::schema_check::render_schema_check_report(report)
    }

    pub(crate) fn render_snapshot_report(report: &icydb::db::StorageReport) -> String {
        super::snapshot::render_snapshot_report(report)
    }

    pub(crate) const fn yes_no(value: bool) -> &'static str {
        super::render::yes_no(value)
    }
}
