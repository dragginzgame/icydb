//! Module: schema report command handling.
//! Responsibility: call the generated accepted-schema endpoint and render schema reports.
//! Does not own: schema reconciliation, endpoint publication, or generic ICP command construction.
//! Boundary: exposes the schema show command and test-covered report rendering through observability.

use candid::Decode;

mod render;

use crate::{
    cli::{CanisterTarget, DiagnosticArtifactArgs},
    diagnostic::artifact::DiagnosticSchemaArtifact,
    endpoint::SCHEMA_ENDPOINT,
    icp::require_created_canister,
    observability::call_query,
};

/// Read and print the generated accepted-schema endpoint.
pub(super) fn run_schema_show_command(target: CanisterTarget) -> Result<(), String> {
    let report = load_schema_report(target.environment(), target.canister_name())?;
    print!("{}", render_schema_report(report.as_slice()));
    Ok(())
}

pub(super) fn run_diagnostic_artifact_command(args: DiagnosticArtifactArgs) -> Result<(), String> {
    let target = args.target();
    let report = load_schema_report(target.environment(), target.canister_name())?;
    let artifact = DiagnosticSchemaArtifact::from_report(
        target.environment(),
        target.canister_name(),
        report.as_slice(),
    )?;
    artifact.write_new(args.output())?;
    println!("wrote {}", args.output().display());
    Ok(())
}

pub(super) fn load_schema_report(
    environment: &str,
    canister: &str,
) -> Result<Vec<icydb::db::EntitySchemaDescription>, String> {
    require_created_canister(environment, canister)?;
    let candid_bytes = call_query(environment, canister, SCHEMA_ENDPOINT.method(), "()")?;
    let response = decode_schema_report(candid_bytes.as_slice())?;
    response.map_err(|err| {
        crate::observability::method_error(
            "schema",
            environment,
            canister,
            SCHEMA_ENDPOINT.method(),
            crate::diagnostic::render_error(&err).as_str(),
        )
    })
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
