//! Module: metrics command handling.
//! Responsibility: call generated metrics endpoints and render human metrics reports.
//! Does not own: config surface gating, generic ICP command construction, or other observability reports.
//! Boundary: exposes the metrics command runner and test-covered report helpers through observability.

use candid::Decode;

mod render;

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    config::{
        ConfiguredEndpoint, METRICS_ENDPOINT, METRICS_RESET_ENDPOINT, require_configured_endpoint,
    },
    icp::require_created_canister,
};

use super::{call_query, call_update, endpoint_result_error};

/// Read or reset the generated metrics endpoints.
pub(super) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    let endpoint = metrics_endpoint(args.reset());

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
    let response = decode_metrics_report(candid_bytes.as_slice())?;

    match response {
        Ok(report) => {
            print!("{}", render_metrics_report(&report));

            Ok(())
        }
        Err(err) => Err(endpoint_result_error(
            "metrics",
            target,
            endpoint.method(),
            err,
        )),
    }
}

const fn metrics_endpoint(reset: bool) -> ConfiguredEndpoint {
    if reset {
        METRICS_RESET_ENDPOINT
    } else {
        METRICS_ENDPOINT
    }
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    let candid_bytes = call_update(
        target.environment(),
        target.canister_name(),
        METRICS_RESET_ENDPOINT.method(),
        "()",
    )?;
    let response = decode_metrics_reset_response(candid_bytes.as_slice())?;

    match response {
        Ok(()) => {
            println!(
                "Reset metrics on canister '{}' in environment '{}'.",
                target.canister_name(),
                target.environment(),
            );

            Ok(())
        }
        Err(err) => Err(endpoint_result_error(
            "metrics reset",
            target,
            METRICS_RESET_ENDPOINT.method(),
            err,
        )),
    }
}

pub(super) fn decode_metrics_report(
    candid_bytes: &[u8],
) -> Result<Result<icydb::metrics::EventReport, icydb::Error>, String> {
    Decode!(
        candid_bytes,
        Result<icydb::metrics::EventReport, icydb::Error>
    )
    .map_err(|err| err.to_string())
}

pub(super) fn decode_metrics_reset_response(
    candid_bytes: &[u8],
) -> Result<Result<(), icydb::Error>, String> {
    Decode!(candid_bytes, Result<(), icydb::Error>).map_err(|err| err.to_string())
}

pub(super) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
    match window_start_ms {
        Some(value) => format!("(opt ({value} : nat64))"),
        None => "(null)".to_string(),
    }
}

pub(super) fn render_metrics_report(report: &icydb::metrics::EventReport) -> String {
    render::render_metrics_report(report)
}
