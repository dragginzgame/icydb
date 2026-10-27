//! Module: snapshot command handling.
//! Responsibility: call the generated storage snapshot endpoint and render storage reports.
//! Does not own: stable-memory inspection, endpoint publication, or generic ICP command construction.
//! Boundary: exposes the snapshot command and test-covered report rendering through observability.

mod render;

use canic_icydb::wire::Endpoint;

use crate::{
    cli::CanisterTarget,
    icp::{IcpCallKind, require_created_canister},
    observability::call_endpoint,
};

/// Read and print the generated storage snapshot endpoint.
pub(super) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    require_created_canister(target.environment(), target.canister_name())?;
    let report = call_endpoint::<icydb::db::StorageReport>(
        target.environment(),
        target.canister_name(),
        Endpoint::Snapshot,
        (),
        "snapshot",
        IcpCallKind::Query,
    )?;
    print!("{}", render_snapshot_report(&report));
    Ok(())
}

pub(super) fn render_snapshot_report(report: &icydb::db::StorageReport) -> String {
    render::render_snapshot_report(report)
}
