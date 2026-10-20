//! Module: schema report command handling.
//! Responsibility: call the generated accepted-schema endpoint and render schema reports.
//! Does not own: schema reconciliation, endpoint publication, or generic ICP command construction.
//! Boundary: exposes the schema show command and test-covered report rendering through observability.

mod render;

use icydb::wire::Endpoint;

use crate::{
    cli::{CanisterTarget, DiagnosticArtifactArgs, DiagnosticSourceMetadataArgs},
    diagnostic::artifact::DiagnosticSchemaArtifact,
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

pub(super) fn run_diagnostic_source_metadata_command(
    args: DiagnosticSourceMetadataArgs,
) -> Result<(), String> {
    let metadata = DiagnosticSchemaArtifact::read_deployment(args.artifact())?
        .bind_to_source(args.source())?;
    metadata.write_new(args.output())?;
    println!("wrote {}", args.output().display());
    Ok(())
}

pub(super) fn load_schema_report(
    environment: &str,
    canister: &str,
) -> Result<Vec<icydb::db::EntitySchemaDescription>, String> {
    require_created_canister(environment, canister)?;
    call_query(environment, canister, Endpoint::Schema, (), "schema")
}

pub(super) fn render_schema_report(report: &[icydb::db::EntitySchemaDescription]) -> String {
    render::render_schema_report(report)
}
