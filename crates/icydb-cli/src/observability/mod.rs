//! Module: observability command integration.
//! Responsibility: dispatch metrics, schema, and snapshot canister calls.
//! Does not own: endpoint publication, ICP process construction, or CLI argument parsing.
//! Boundary: decodes raw canister responses and delegates report rendering to submodules.

mod metrics;
mod migration;
pub(crate) mod render;
mod schema;
mod snapshot;

use crate::cli::{
    CanisterTarget, DiagnosticArtifactArgs, DiagnosticSourceMetadataArgs, MetricsArgs,
    SchemaMigrationCommand,
};
use crate::icp::{IcpCallKind, call_query_hex, call_update_hex};

use icydb::__macro::RpcMethod;
use serde::de::DeserializeOwned;

pub(crate) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    metrics::run_metrics_command(args)
}

pub(crate) fn run_schema_show_command(target: CanisterTarget) -> Result<(), String> {
    schema::run_schema_show_command(target)
}

pub(crate) fn run_diagnostic_artifact_command(args: DiagnosticArtifactArgs) -> Result<(), String> {
    schema::run_diagnostic_artifact_command(args)
}

pub(crate) fn run_diagnostic_source_metadata_command(
    args: DiagnosticSourceMetadataArgs,
) -> Result<(), String> {
    schema::run_diagnostic_source_metadata_command(args)
}

pub(crate) fn load_schema_report(
    environment: &str,
    canister: &str,
) -> Result<Vec<icydb::db::EntitySchemaDescription>, String> {
    schema::load_schema_report(environment, canister)
}

pub(crate) fn run_schema_migration_command(command: SchemaMigrationCommand) -> Result<(), String> {
    migration::run_schema_migration_command(command)
}

pub(crate) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    snapshot::run_snapshot_command(target)
}

// The call lane stays explicit so request/reply handling cannot erase the
// uncertainty semantics of a dispatched update.
fn call_endpoint<T: serde::Serialize + DeserializeOwned>(
    environment: &str,
    canister: &str,
    endpoint: RpcMethod,
    arguments: impl serde::Serialize,
    label: &str,
    kind: IcpCallKind,
) -> Result<T, String> {
    let method = endpoint.name();
    let arguments = endpoint
        .encode_request(
            arguments,
            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
        )
        .map_err(|error| error.to_string())?;
    let response = match kind {
        IcpCallKind::Query => call_query_hex(environment, canister, method, &arguments, |stderr| {
            method_error("query", environment, canister, method, stderr)
        }),
        IcpCallKind::Update => {
            call_update_hex(environment, canister, method, &arguments, |stderr| {
                method_error("update", environment, canister, method, stderr)
            })
        }
    }?;
    decode_endpoint_result(&response, label, environment, canister, endpoint, kind)
}

// Wire shape failures retain their decoder diagnostic. A successfully decoded
// endpoint error gains the same target context and rich facts in every command.
pub(crate) fn decode_endpoint_result<T: serde::Serialize + DeserializeOwned>(
    bytes: &[u8],
    label: &str,
    environment: &str,
    canister: &str,
    endpoint: RpcMethod,
    kind: IcpCallKind,
) -> Result<T, String> {
    let response = endpoint
        .decode_response::<Result<T, icydb::Error>>(
            bytes,
            icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES),
        )
        .map_err(|error| {
            kind.reply_error(crate::diagnostic::render_wire_decode_error(endpoint, error))
        })?;
    response.map_err(|error| {
        method_error(
            label,
            environment,
            canister,
            endpoint.name(),
            &crate::diagnostic::render_error(&error),
        )
    })
}

fn method_error(
    label: &str,
    environment: &str,
    canister: &str,
    method: &str,
    detail: &str,
) -> String {
    format!(
        "IcyDB {label} method '{method}' failed on canister '{canister}' in environment '{environment}': {detail}",
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use crate::observability::{
        method_error as base_method_error, metrics, render, schema, snapshot,
    };

    pub(crate) fn method_error(
        label: &str,
        environment: &str,
        canister: &str,
        method: &str,
        detail: &str,
    ) -> String {
        base_method_error(label, environment, canister, method, detail)
    }

    pub(crate) fn render_field_list(fields: &[String]) -> String {
        render::render_field_list(fields)
    }

    pub(crate) fn render_metrics_report(report: &icydb::metrics::MetricsReport) -> String {
        metrics::render_metrics_report(report)
    }

    pub(crate) fn render_schema_report(report: &[icydb::db::EntitySchemaDescription]) -> String {
        schema::render_schema_report(report)
    }

    pub(crate) fn render_snapshot_report(report: &icydb::db::StorageReport) -> String {
        snapshot::render_snapshot_report(report)
    }

    pub(crate) const fn yes_no(value: bool) -> &'static str {
        render::yes_no(value)
    }
}
