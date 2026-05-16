use std::process::Stdio;

use candid::Decode;

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    icp::require_created_canister,
    shell::{hex_response_bytes, icp_query_command, icp_update_command},
};

pub(crate) const SNAPSHOT_METHOD: &str = "__icydb_snapshot";
pub(crate) const METRICS_METHOD: &str = "__icydb_metrics";
pub(crate) const METRICS_RESET_METHOD: &str = "__icydb_metrics_reset";

/// Read and print the generated storage snapshot endpoint.
pub(crate) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SNAPSHOT_METHOD,
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::db::StorageReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            println!("{report:#?}");

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB snapshot method '{SNAPSHOT_METHOD}' failed on canister '{}' in environment '{}': {err}",
            target.canister_name(),
            target.environment(),
        )),
    }
}

/// Read or reset the generated metrics endpoints.
pub(crate) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let candid_arg = metrics_candid_arg(args.window_start_ms());
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        METRICS_METHOD,
        candid_arg.as_str(),
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::metrics::EventReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            println!("{report:#?}");

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics method '{METRICS_METHOD}' failed on canister '{}' in environment '{}': {err}",
            target.canister_name(),
            target.environment(),
        )),
    }
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    let candid_bytes = call_update(
        target.environment(),
        target.canister_name(),
        METRICS_RESET_METHOD,
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
            "IcyDB metrics reset method '{METRICS_RESET_METHOD}' failed on canister '{}' in environment '{}': {err}",
            target.canister_name(),
            target.environment(),
        )),
    }
}

pub(crate) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
    match window_start_ms {
        Some(value) => format!("(opt ({value} : nat64))"),
        None => "(null)".to_string(),
    }
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
