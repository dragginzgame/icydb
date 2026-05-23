//! Module: schema check command handling.
//! Responsibility: call the generated schema-check endpoint and report generated-vs-accepted drift.
//! Does not own: schema mutation semantics, config surface gating, or generic ICP command construction.
//! Boundary: exposes the schema-check command and test-covered report rendering through observability.

mod analysis;
mod render;

use crate::{
    cli::CanisterTarget,
    config::{SCHEMA_CHECK_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
};
use candid::Decode;

use self::{analysis::analyze_schema_check, render::render_schema_check_report_from_summary};
use super::call_query;

/// Read and print the generated-vs-accepted schema check endpoint.
pub(super) fn run_schema_check_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SCHEMA_CHECK_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SCHEMA_CHECK_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<Vec<icydb::db::EntitySchemaCheckDescription>, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            let summary = analyze_schema_check(report.as_slice());
            print!("{}", render_schema_check_report_from_summary(&summary));
            if summary.mismatches == 0 {
                Ok(())
            } else {
                Err(format!(
                    "IcyDB schema check found {} mismatch(es) on canister '{}' in environment '{}'",
                    summary.mismatches,
                    target.canister_name(),
                    target.environment(),
                ))
            }
        }
        Err(err) => Err(format!(
            "IcyDB schema check method '{}' failed on canister '{}' in environment '{}': {err}",
            SCHEMA_CHECK_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

#[cfg(test)]
pub(super) fn render_schema_check_report(
    report: &[icydb::db::EntitySchemaCheckDescription],
) -> String {
    let summary = analyze_schema_check(report);

    render_schema_check_report_from_summary(&summary)
}
