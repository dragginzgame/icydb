//! Module: observability command integration.
//! Responsibility: dispatch metrics, schema, schema-check, and snapshot canister calls.
//! Does not own: endpoint configuration, ICP process construction, or CLI argument parsing.
//! Boundary: decodes raw canister responses and delegates report rendering to submodules.

mod metrics;
mod render;
mod schema;
mod schema_check;
mod snapshot;

use std::process::Stdio;

use crate::icp::{hex_response_bytes, icp_query_command, icp_update_command};

pub(crate) use metrics::run_metrics_command;
pub(crate) use schema::run_schema_show_command;
pub(crate) use schema_check::run_schema_check_command;
pub(crate) use snapshot::run_snapshot_command;

#[cfg(test)]
pub(crate) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
    metrics::metrics_candid_arg(window_start_ms)
}

#[cfg(test)]
pub(crate) fn render_metrics_report(report: &icydb::metrics::EventReport) -> String {
    metrics::render_metrics_report(report)
}

#[cfg(test)]
pub(crate) fn render_schema_report(report: &[icydb::db::EntitySchemaDescription]) -> String {
    schema::render_schema_report(report)
}

#[cfg(test)]
pub(crate) fn render_schema_check_report(
    report: &[icydb::db::EntitySchemaCheckDescription],
) -> String {
    schema_check::render_schema_check_report(report)
}

#[cfg(test)]
pub(crate) fn render_snapshot_report(report: &icydb::db::StorageReport) -> String {
    snapshot::render_snapshot_report(report)
}

fn call_query(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    let output = icp_query_command(environment, canister, method, candid_arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "IcyDB query method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim(),
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}

fn call_update(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    let output = icp_update_command(environment, canister, method, candid_arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "IcyDB update method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim(),
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}
