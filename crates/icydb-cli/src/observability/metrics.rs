//! Module: metrics command handling.
//! Responsibility: call generated metrics endpoints and render entity-cost reports.
//! Does not own: endpoint publication or generic ICP command construction.
//! Boundary: one report query and one reset update.

mod render;

use icydb::__macro::RpcMethod;

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    icp::{IcpCallKind, require_created_canister},
    observability::call_endpoint,
};

/// Read or reset the generated metrics endpoints.
pub(super) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let report = call_endpoint::<icydb::metrics::MetricsReport>(
        target.environment(),
        target.canister_name(),
        RpcMethod::Metrics,
        (),
        "metrics",
        IcpCallKind::Query,
    )?;
    print!("{}", render_metrics_report(&report));
    Ok(())
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    call_endpoint::<()>(
        target.environment(),
        target.canister_name(),
        RpcMethod::MetricsReset,
        (),
        "metrics reset",
        IcpCallKind::Update,
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
