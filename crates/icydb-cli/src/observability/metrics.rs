//! Module: metrics command handling.
//! Responsibility: call generated metrics endpoints and render entity-cost reports.
//! Does not own: endpoint publication or generic ICP command construction.
//! Boundary: one report query and one reset update.

mod render;

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    endpoint::{METRICS_ENDPOINT, METRICS_RESET_ENDPOINT},
    icp::require_created_canister,
    observability::{call_query, call_update, decode_endpoint_result},
};

/// Read or reset the generated metrics endpoints.
pub(super) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let wire_bytes = call_query(
        target.environment(),
        target.canister_name(),
        METRICS_ENDPOINT.method(),
        (),
    )?;
    let report = decode_endpoint_result::<icydb::metrics::MetricsReport>(
        &wire_bytes,
        "metrics",
        target.environment(),
        target.canister_name(),
        METRICS_ENDPOINT.method(),
    )?;
    print!("{}", render_metrics_report(&report));
    Ok(())
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    let wire_bytes = call_update(
        target.environment(),
        target.canister_name(),
        METRICS_RESET_ENDPOINT.method(),
        (),
    )?;
    decode_endpoint_result::<()>(
        &wire_bytes,
        "metrics reset",
        target.environment(),
        target.canister_name(),
        METRICS_RESET_ENDPOINT.method(),
    )?;
    println!(
        "Reset metrics on canister '{}' in environment '{}'.",
        target.canister_name(),
        target.environment(),
    );
    Ok(())
}

pub(super) fn render_metrics_report(report: &icydb::metrics::MetricsReport) -> String {
    render::render_metrics_report(report)
}
