//! Module: metrics command handling.
//! Responsibility: call generated metrics endpoints and render entity-cost reports.
//! Does not own: endpoint publication or generic ICP command construction.
//! Boundary: one report query and one reset update.

use candid::Decode;

mod render;

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    endpoint::{METRICS_ENDPOINT, METRICS_RESET_ENDPOINT},
    icp::require_created_canister,
    observability::{call_query, call_update, endpoint_result_error},
};

/// Read or reset the generated metrics endpoints.
pub(super) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        METRICS_ENDPOINT.method(),
        "()",
    )?;
    match decode_metrics_report(candid_bytes.as_slice())? {
        Ok(report) => {
            print!("{}", render_metrics_report(&report));
            Ok(())
        }
        Err(err) => Err(endpoint_result_error(
            "metrics",
            target,
            METRICS_ENDPOINT.method(),
            err,
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
    match decode_metrics_reset_response(candid_bytes.as_slice())? {
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
) -> Result<Result<icydb::metrics::MetricsReport, icydb::Error>, String> {
    Decode!(
        candid_bytes,
        Result<icydb::metrics::MetricsReport, icydb::Error>
    )
    .map_err(|err| err.to_string())
}

pub(super) fn decode_metrics_reset_response(
    candid_bytes: &[u8],
) -> Result<Result<(), icydb::Error>, String> {
    Decode!(candid_bytes, Result<(), icydb::Error>).map_err(|err| err.to_string())
}

pub(super) fn render_metrics_report(report: &icydb::metrics::MetricsReport) -> String {
    render::render_metrics_report(report)
}
