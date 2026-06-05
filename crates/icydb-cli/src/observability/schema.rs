//! Module: schema report command handling.
//! Responsibility: call the generated accepted-schema endpoint and render schema reports.
//! Does not own: schema reconciliation, config surface gating, or generic ICP command construction.
//! Boundary: exposes the schema show command and test-covered report rendering through observability.

use candid::Decode;

mod render;

use crate::{
    cli::CanisterTarget,
    config::{SCHEMA_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
};

use super::{call_query, endpoint_result_error};

/// Read and print the generated accepted-schema endpoint.
pub(super) fn run_schema_show_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SCHEMA_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SCHEMA_ENDPOINT.method(),
        "()",
    )?;
    let response = decode_schema_report(candid_bytes.as_slice())?;

    match response {
        Ok(report) => {
            print!("{}", render_schema_report(report.as_slice()));

            Ok(())
        }
        Err(err) => Err(endpoint_result_error(
            "schema",
            &target,
            SCHEMA_ENDPOINT.method(),
            err,
        )),
    }
}

pub(super) fn decode_schema_report(
    candid_bytes: &[u8],
) -> Result<Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error>, String> {
    Decode!(
        candid_bytes,
        Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error>
    )
    .map_err(|err| err.to_string())
}

pub(super) fn render_schema_report(report: &[icydb::db::EntitySchemaDescription]) -> String {
    render::render_schema_report(report)
}
