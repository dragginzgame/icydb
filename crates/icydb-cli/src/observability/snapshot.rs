//! Module: snapshot command handling.
//! Responsibility: call the generated storage snapshot endpoint and render storage reports.
//! Does not own: stable-memory inspection, config surface gating, or generic ICP command construction.
//! Boundary: exposes the snapshot command and test-covered report rendering through observability.

use candid::Decode;

mod render;

use crate::{
    cli::CanisterTarget,
    config::{SNAPSHOT_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
};

use super::{call_query, endpoint_result_error};

/// Read and print the generated storage snapshot endpoint.
pub(super) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SNAPSHOT_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SNAPSHOT_ENDPOINT.method(),
        "()",
    )?;
    let response = decode_snapshot_report(candid_bytes.as_slice())?;

    match response {
        Ok(report) => {
            print!("{}", render_snapshot_report(&report));

            Ok(())
        }
        Err(err) => Err(endpoint_result_error(
            "snapshot",
            &target,
            SNAPSHOT_ENDPOINT.method(),
            err,
        )),
    }
}

pub(super) fn decode_snapshot_report(
    candid_bytes: &[u8],
) -> Result<Result<icydb::db::StorageReport, icydb::Error>, String> {
    Decode!(
        candid_bytes,
        Result<icydb::db::StorageReport, icydb::Error>
    )
    .map_err(|err| err.to_string())
}

pub(super) fn render_snapshot_report(report: &icydb::db::StorageReport) -> String {
    render::render_snapshot_report(report)
}
