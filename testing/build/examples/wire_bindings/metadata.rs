//! Wire owns native layout collection and rendering; IcyDB owns public names and policy.

use std::{collections::BTreeMap, error::Error, fs, path::Path};

use ic_wire::typescript::{RUNTIME, SchemaRegistry, generate_registry};
use serde_json::{Value, json};

use crate::bulk;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Retain public browser type names while Wire collects their actual layouts.
#[expect(
    clippy::too_many_lines,
    reason = "one inventory of maintained public browser type names"
)]
pub(super) fn registry() -> Result<SchemaRegistry> {
    let mut registry = SchemaRegistry::new();
    macro_rules! register {
        ($ty:ty, $name:literal) => {
            registry
                .register::<$ty>($name)
                .map_err(|error| format!("Wire registry: {error:?}"))?;
        };
    }
    register!(icydb::types::Account, "Account");
    register!(
        icydb_core::error::ConstraintValidationFindingOutput,
        "ConstraintValidationFindingOutput"
    );
    register!(
        icydb_core::db::ConstraintValidationProgressDescription,
        "ConstraintValidationProgressDescription"
    );
    register!(
        icydb_core::error::ConstraintValuePath,
        "ConstraintValuePath"
    );
    register!(
        icydb_core::error::ConstraintValuePathComponent,
        "ConstraintValuePathComponent"
    );
    register!(
        icydb_core::metrics::ConvergenceMetrics,
        "ConvergenceMetrics"
    );
    register!(icydb_core::db::DataStoreSnapshot, "DataStoreSnapshot");
    register!(
        icydb_core::db::DatabaseIncarnationId,
        "DatabaseIncarnationId"
    );
    register!(icydb_core::db::DeepIntegrityPage, "DeepIntegrityPage");
    register!(
        icydb_core::db::DeepIntegrityPageStatus,
        "DeepIntegrityPageStatus"
    );
    register!(icydb::DiagnosticFact, "DiagnosticFact");
    register!(
        icydb_core::db::EntityCatalogDescription,
        "EntityCatalogDescription"
    );
    register!(
        icydb_core::db::EntityConstraintDescription,
        "EntityConstraintDescription"
    );
    register!(
        icydb_core::db::EntityFieldDescription,
        "EntityFieldDescription"
    );
    register!(
        icydb_core::db::EntityIdentityDescription,
        "EntityIdentityDescription"
    );
    register!(
        icydb_core::db::EntityIndexDescription,
        "EntityIndexDescription"
    );
    register!(icydb_core::metrics::EntityMetrics, "EntityMetrics");
    register!(
        icydb_core::db::EntityRelationCardinality,
        "EntityRelationCardinality"
    );
    register!(
        icydb_core::db::EntityRelationDescription,
        "EntityRelationDescription"
    );
    register!(
        icydb_core::db::EntitySchemaDescription,
        "EntitySchemaDescription"
    );
    register!(icydb_core::db::EntitySnapshot, "EntitySnapshot");
    register!(icydb::Error, "Error");
    register!(
        icydb_core::db::ExactBacklogMeasurement,
        "ExactBacklogMeasurement"
    );
    register!(icydb_schema::ExpectedAcceptedHead, "ExpectedAcceptedHead");
    register!(icydb_core::db::IndexState, "IndexState");
    register!(icydb_core::db::IndexStoreSnapshot, "IndexStoreSnapshot");
    register!(
        icydb_core::metrics::InstructionMetrics,
        "InstructionMetrics"
    );
    register!(
        icydb_core::db::IntegrityAbortReceipt,
        "IntegrityAbortReceipt"
    );
    register!(icydb_core::db::IntegrityAbortStatus, "IntegrityAbortStatus");
    register!(
        icydb_core::db::IntegrityAuthorityClass,
        "IntegrityAuthorityClass"
    );
    register!(
        icydb_core::db::IntegrityAuthorityDiagnostic,
        "IntegrityAuthorityDiagnostic"
    );
    register!(icydb::db::IntegrityCheckError, "IntegrityCheckError");
    register!(icydb_core::db::IntegrityCheckResult, "IntegrityCheckResult");
    register!(
        icydb_core::db::IntegrityEntityIdentity,
        "IntegrityEntityIdentity"
    );
    register!(icydb_core::db::IntegrityFinding, "IntegrityFinding");
    register!(
        icydb_core::db::IntegrityFindingClass,
        "IntegrityFindingClass"
    );
    register!(icydb_core::db::IntegrityFindingKind, "IntegrityFindingKind");
    register!(icydb_core::db::IntegrityJobError, "IntegrityJobError");
    register!(icydb_core::db::IntegrityJobId, "IntegrityJobId");
    register!(icydb_core::db::IntegrityJobReceipt, "IntegrityJobReceipt");
    register!(
        icydb_core::db::IntegrityPendingTerminal,
        "IntegrityPendingTerminal"
    );
    register!(icydb_core::db::IntegrityPhase, "IntegrityPhase");
    register!(
        icydb_core::db::IntegrityResourceDiagnostic,
        "IntegrityResourceDiagnostic"
    );
    register!(icydb_core::db::IntegritySeverity, "IntegritySeverity");
    register!(
        icydb_core::db::IntegrityTerminalOutcome,
        "IntegrityTerminalOutcome"
    );
    register!(
        icydb_core::db::IntegrityVerifierFamily,
        "IntegrityVerifierFamily"
    );
    register!(icydb_core::db::MemoryAllocation, "MemoryAllocation");
    register!(
        icydb_core::db::MemoryAllocationBinding,
        "MemoryAllocationBinding"
    );
    register!(icydb_core::db::MemoryAllocations, "MemoryAllocations");
    register!(
        icydb_core::db::MemoryCatalogDescription,
        "MemoryCatalogDescription"
    );
    register!(icydb_core::db::MemoryExtent, "MemoryExtent");
    register!(icydb_core::metrics::MetricsReport, "MetricsReport");
    register!(icydb::value::PublicEnumValue, "PublicEnumValue");
    register!(icydb::value::PublicValue, "PublicValue");
    register!(icydb::QueryFieldDiagnostic, "QueryFieldDiagnostic");
    register!(icydb_core::db::QuickIntegrityResult, "QuickIntegrityResult");
    register!(icydb_core::db::QuickIntegrityStatus, "QuickIntegrityStatus");
    register!(icydb_core::db::RowProjectionOutput, "RowProjectionOutput");
    register!(
        icydb_core::metrics::SchemaLifecycleMetrics,
        "SchemaLifecycleMetrics"
    );
    register!(
        icydb_core::db::SchemaMigrationCommand,
        "SchemaMigrationCommand"
    );
    register!(
        icydb_core::db::SchemaMigrationEntityTransition,
        "SchemaMigrationEntityTransition"
    );
    register!(
        icydb_core::db::SchemaMigrationFinding,
        "SchemaMigrationFinding"
    );
    register!(
        icydb_core::db::SchemaMigrationFindingKind,
        "SchemaMigrationFindingKind"
    );
    register!(
        icydb_core::db::SchemaMigrationTransformReason,
        "SchemaMigrationTransformReason"
    );
    register!(icydb_core::db::SchemaMigrationPhase, "SchemaMigrationPhase");
    register!(
        icydb_core::db::SchemaMigrationReceipt,
        "SchemaMigrationReceipt"
    );
    register!(
        icydb_core::db::SchemaMigrationStatusPage,
        "SchemaMigrationStatusPage"
    );
    register!(
        icydb_core::db::SchemaMigrationStatusRequest,
        "SchemaMigrationStatusRequest"
    );
    register!(icydb_core::db::SchemaStoreSnapshot, "SchemaStoreSnapshot");
    register!(icydb_core::db::SqlColumnDefault, "SqlColumnDefault");
    register!(icydb_core::db::SqlColumnExtra, "SqlColumnExtra");
    register!(icydb_core::db::SqlColumnKey, "SqlColumnKey");
    register!(icydb_core::db::SqlColumnSummary, "SqlColumnSummary");
    register!(
        icydb::db::sql::SqlConstraintValidationOutput,
        "SqlConstraintValidationOutput"
    );
    register!(icydb_core::db::SqlDescribeOutput, "SqlDescribeOutput");
    register!(icydb::db::sql::SqlGroupedRowsOutput, "SqlGroupedRowsOutput");
    register!(icydb::db::SqlIntegrityError, "SqlIntegrityError");
    register!(icydb::db::sql::SqlQueryResult, "SqlQueryResult");
    register!(icydb_core::db::SqlShowColumnsOutput, "SqlShowColumnsOutput");
    register!(
        icydb_core::db::SqlShowRelationsOutput,
        "SqlShowRelationsOutput"
    );
    register!(icydb_core::db::StorageReport, "StorageReport");
    register!(
        icydb_core::db::StoreAllocationIdentityCapability,
        "StoreAllocationIdentityCapability"
    );
    register!(
        icydb_core::db::StoreCatalogDescription,
        "StoreCatalogDescription"
    );
    register!(
        icydb_core::db::StoreCommitParticipation,
        "StoreCommitParticipation"
    );
    register!(icydb_core::db::StoreDurability, "StoreDurability");
    register!(
        icydb_core::db::StoreRecoveryCapability,
        "StoreRecoveryCapability"
    );
    register!(
        icydb_core::db::StoreSchemaMetadataCapability,
        "StoreSchemaMetadataCapability"
    );
    register!(
        icydb_core::db::StoreSnapshotStorageMode,
        "StoreSnapshotStorageMode"
    );
    Ok(registry)
}

pub(super) fn write(out: &Path, types: &BTreeMap<String, Value>, methods: &[Value]) -> Result<()> {
    let mut clean = false;
    let source = generate_registry(
        types,
        methods,
        "./native-runtime.js",
        &bulk::acknowledgements(),
        |report| {
            eprint!("{report}");
            clean = report.is_clean();
        },
    )
    .map_err(|error| format!("Wire TypeScript generation failed: {error:?}"))?;
    if !clean {
        return Err("Wire bulk-field review requires attention; see generator diagnostics".into());
    }
    fs::write(out.join("icydb.ts"), source)?;
    fs::write(
        out.join("native-runtime.ts"),
        include_str!("../../../wire-browser/native-runtime.ts"),
    )?;
    fs::write(out.join("wire-runtime.ts"), RUNTIME)?;
    // Independent policy projection for the consumer check, not a copy of the
    // method metadata under test. Native allocation has no browser equivalent.
    fs::write(
        out.join("budget-policy.json"),
        serde_json::to_vec_pretty(&json!({
            "requestBytes": icydb_schema::wire::MAX_REQUEST_BYTES,
            "responseBytes": icydb_schema::wire::MAX_REPLY_BYTES,
            "requestNodes": icydb_schema::wire::BROWSER_MAX_NODES,
            "responseNodes": icydb_schema::wire::BROWSER_MAX_NODES,
            "requestDepth": icydb_schema::wire::BROWSER_MAX_DEPTH,
            "responseDepth": icydb_schema::wire::BROWSER_MAX_DEPTH,
        }))?,
    )?;

    fs::write(
        out.join("contracts.json"),
        serde_json::to_vec_pretty(&json!({"types":types,"methods":methods}))?,
    )?;
    Ok(())
}
