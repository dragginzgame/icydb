mod metrics;
mod render;
mod schema;
mod schema_check;
mod snapshot;

use std::process::Stdio;

use crate::icp::{hex_response_bytes, icp_query_command, icp_update_command};

pub(crate) use metrics::run_metrics_command;
#[cfg(test)]
pub(crate) use metrics::{metrics_candid_arg, render_metrics_report};
#[cfg(test)]
pub(crate) use schema::render_schema_report;
pub(crate) use schema::run_schema_show_command;
#[cfg(test)]
pub(crate) use schema_check::render_schema_check_report;
pub(crate) use schema_check::run_schema_check_command;
#[cfg(test)]
pub(crate) use snapshot::render_snapshot_report;
pub(crate) use snapshot::run_snapshot_command;

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
