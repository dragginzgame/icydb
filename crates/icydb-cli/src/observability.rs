use std::{collections::BTreeMap, process::Stdio};

use candid::Decode;
use icydb::{
    db::{
        EntityFieldDescription, EntityIndexDescription, EntitySchemaCheckDescription,
        EntitySchemaDescription, StorageReport,
    },
    metrics::{EventCounters, EventReport},
};

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    config::{
        METRICS_ENDPOINT, METRICS_RESET_ENDPOINT, SCHEMA_CHECK_ENDPOINT, SCHEMA_ENDPOINT,
        SNAPSHOT_ENDPOINT, require_configured_endpoint,
    },
    icp::{hex_response_bytes, icp_query_command, icp_update_command, require_created_canister},
    table::{ColumnAlign, append_indented_table},
};

/// Read and print the generated storage snapshot endpoint.
pub(crate) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SNAPSHOT_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SNAPSHOT_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::db::StorageReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_snapshot_report(&report));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB snapshot method '{}' failed on canister '{}' in environment '{}': {err}",
            SNAPSHOT_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

/// Read or reset the generated metrics endpoints.
pub(crate) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    let endpoint = if args.reset() {
        METRICS_RESET_ENDPOINT
    } else {
        METRICS_ENDPOINT
    };
    require_configured_endpoint(target.canister_name(), endpoint)?;
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let candid_arg = metrics_candid_arg(args.window_start_ms());
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        endpoint.method(),
        candid_arg.as_str(),
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::metrics::EventReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_metrics_report(&report));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics method '{}' failed on canister '{}' in environment '{}': {err}",
            endpoint.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    let candid_bytes = call_update(
        target.environment(),
        target.canister_name(),
        METRICS_RESET_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(candid_bytes.as_slice(), Result<(), icydb::Error>)
        .map_err(|err| err.to_string())?;

    match response {
        Ok(()) => {
            println!(
                "Reset metrics on canister '{}' in environment '{}'.",
                target.canister_name(),
                target.environment(),
            );

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics reset method '{}' failed on canister '{}' in environment '{}': {err}",
            METRICS_RESET_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

/// Read and print the generated accepted-schema endpoint.
pub(crate) fn run_schema_show_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SCHEMA_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SCHEMA_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_schema_report(report.as_slice()));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB schema method '{}' failed on canister '{}' in environment '{}': {err}",
            SCHEMA_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

/// Read and print the generated-vs-accepted schema check endpoint.
pub(crate) fn run_schema_check_command(target: CanisterTarget) -> Result<(), String> {
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
            print!("{}", render_schema_check_report(report.as_slice()));
            let summary = analyze_schema_check(report.as_slice());
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

pub(crate) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
    match window_start_ms {
        Some(value) => format!("(opt ({value} : nat64))"),
        None => "(null)".to_string(),
    }
}

pub(crate) fn render_schema_report(report: &[EntitySchemaDescription]) -> String {
    let mut output = String::new();
    let entity_rows = report
        .iter()
        .map(|entity| {
            [
                entity.entity_name().to_string(),
                entity.fields().len().to_string(),
                entity.indexes().len().to_string(),
                entity.relations().len().to_string(),
                entity.primary_key().to_string(),
                entity.entity_path().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let field_rows = report
        .iter()
        .flat_map(|entity| {
            entity.fields().iter().map(|field| {
                [
                    entity.entity_name().to_string(),
                    field.name().to_string(),
                    field
                        .slot()
                        .map_or_else(|| "-".to_string(), |slot| slot.to_string()),
                    field.kind().to_string(),
                    yes_no(field.nullable()).to_string(),
                    yes_no(field.primary_key()).to_string(),
                    yes_no(field.queryable()).to_string(),
                    field.origin().to_string(),
                ]
            })
        })
        .collect::<Vec<_>>();
    let index_rows = report
        .iter()
        .flat_map(|entity| {
            entity.indexes().iter().map(|index| {
                [
                    entity.entity_name().to_string(),
                    index.name().to_string(),
                    render_field_list(index.fields()),
                    yes_no(index.unique()).to_string(),
                    index.origin().to_string(),
                ]
            })
        })
        .collect::<Vec<_>>();
    let relation_rows = report
        .iter()
        .flat_map(|entity| {
            entity.relations().iter().map(|relation| {
                [
                    entity.entity_name().to_string(),
                    relation.field().to_string(),
                    relation.target_entity_name().to_string(),
                    format!("{:?}", relation.strength()),
                    format!("{:?}", relation.cardinality()),
                ]
            })
        })
        .collect::<Vec<_>>();

    output.push_str("IcyDB schema\n");
    output.push_str(
        format!(
            "  entities: {}\n  fields: {}\n  indexes: {}\n  relations: {}\n\n",
            report.len(),
            field_rows.len(),
            index_rows.len(),
            relation_rows.len(),
        )
        .as_str(),
    );
    append_schema_entity_table(&mut output, entity_rows.as_slice());
    output.push('\n');
    append_schema_field_table(&mut output, field_rows.as_slice());
    output.push('\n');
    append_schema_index_table(&mut output, index_rows.as_slice());
    output.push('\n');
    append_schema_relation_table(&mut output, relation_rows.as_slice());

    output
}

pub(crate) fn render_schema_check_report(report: &[EntitySchemaCheckDescription]) -> String {
    let summary = analyze_schema_check(report);

    render_schema_check_report_from_summary(&summary)
}

#[derive(Debug)]
struct SchemaCheckSummary {
    status: &'static str,
    entities: usize,
    accepted_only_fields: usize,
    accepted_ddl_indexes: usize,
    mismatches: usize,
    recommendations: Vec<String>,
    entity_rows: Vec<[String; 8]>,
    drift_rows: Vec<[String; 4]>,
    mismatch_rows: Vec<[String; 4]>,
}

fn analyze_schema_check(report: &[EntitySchemaCheckDescription]) -> SchemaCheckSummary {
    let mut accepted_only_fields = 0;
    let mut accepted_ddl_indexes = 0;
    let mut mismatches = 0;
    let mut accepted_only_generated_fields = 0;
    let mut generated_only_fields = 0;
    let mut field_default_mismatches = 0;
    let mut field_nullability_mismatches = 0;
    let mut accepted_only_generated_indexes = 0;
    let mut generated_only_indexes = 0;
    let mut index_contract_mismatches = 0;
    let mut entity_rows = Vec::with_capacity(report.len());
    let mut drift_rows = Vec::new();
    let mut mismatch_rows = Vec::new();

    for entity in report {
        let analysis = analyze_entity_schema_check(entity);
        accepted_only_fields += analysis.accepted_only_fields;
        accepted_ddl_indexes += analysis.accepted_ddl_indexes;
        mismatches += analysis.mismatches;
        accepted_only_generated_fields += analysis.accepted_only_generated_fields;
        generated_only_fields += analysis.generated_only_fields;
        field_default_mismatches += analysis.field_default_mismatches;
        field_nullability_mismatches += analysis.field_nullability_mismatches;
        accepted_only_generated_indexes += analysis.accepted_only_generated_indexes;
        generated_only_indexes += analysis.generated_only_indexes;
        index_contract_mismatches += analysis.index_contract_mismatches;
        drift_rows.extend(analysis.drift_rows);
        mismatch_rows.extend(analysis.mismatch_rows);
        entity_rows.push(analysis.entity_row);
    }

    let status = if mismatches > 0 {
        "mismatch"
    } else if accepted_only_fields > 0 || accepted_ddl_indexes > 0 {
        "drift"
    } else {
        "ok"
    };

    SchemaCheckSummary {
        status,
        entities: report.len(),
        accepted_only_fields,
        accepted_ddl_indexes,
        mismatches,
        recommendations: schema_check_recommendations(&SchemaCheckRecommendationFacts {
            mismatches,
            accepted_only_fields,
            accepted_ddl_indexes,
            accepted_only_generated_fields,
            generated_only_fields,
            field_default_mismatches,
            field_nullability_mismatches,
            accepted_only_generated_indexes,
            generated_only_indexes,
            index_contract_mismatches,
        }),
        entity_rows,
        drift_rows,
        mismatch_rows,
    }
}

struct EntitySchemaCheckAnalysis {
    accepted_only_fields: usize,
    accepted_ddl_indexes: usize,
    mismatches: usize,
    accepted_only_generated_fields: usize,
    generated_only_fields: usize,
    field_default_mismatches: usize,
    field_nullability_mismatches: usize,
    accepted_only_generated_indexes: usize,
    generated_only_indexes: usize,
    index_contract_mismatches: usize,
    entity_row: [String; 8],
    drift_rows: Vec<[String; 4]>,
    mismatch_rows: Vec<[String; 4]>,
}

fn analyze_entity_schema_check(entity: &EntitySchemaCheckDescription) -> EntitySchemaCheckAnalysis {
    let generated = entity.generated();
    let accepted = entity.accepted();
    let entity_name = accepted.entity_name();
    let identity = analyze_entity_schema_identity(generated, accepted, entity_name);
    let fields = analyze_entity_schema_fields(generated, accepted, entity_name);
    let indexes = analyze_entity_schema_indexes(generated, accepted, entity_name);
    let accepted_only_fields = fields.accepted_only;
    let accepted_ddl_indexes = indexes.accepted_ddl;
    let mismatches = identity.mismatches + fields.mismatches + indexes.mismatches;
    let accepted_only_generated_fields = fields.accepted_only_generated;
    let generated_only_fields = fields.generated_only;
    let field_default_mismatches = fields.default_mismatches;
    let field_nullability_mismatches = fields.nullability_mismatches;
    let accepted_only_generated_indexes = indexes.accepted_only_generated;
    let generated_only_indexes = indexes.generated_only;
    let index_contract_mismatches = indexes.contract_mismatches;
    let drift_rows = [fields.drift_rows, indexes.drift_rows].concat();
    let mismatch_rows = [
        identity.mismatch_rows,
        fields.mismatch_rows,
        indexes.mismatch_rows,
    ]
    .concat();

    let status = if mismatches > 0 {
        "mismatch"
    } else if accepted_only_fields > 0 || accepted_ddl_indexes > 0 {
        "drift"
    } else {
        "ok"
    };

    EntitySchemaCheckAnalysis {
        accepted_only_fields,
        accepted_ddl_indexes,
        mismatches,
        accepted_only_generated_fields,
        generated_only_fields,
        field_default_mismatches,
        field_nullability_mismatches,
        accepted_only_generated_indexes,
        generated_only_indexes,
        index_contract_mismatches,
        entity_row: [
            accepted.entity_name().to_string(),
            status.to_string(),
            generated.fields().len().to_string(),
            accepted.fields().len().to_string(),
            generated.indexes().len().to_string(),
            accepted.indexes().len().to_string(),
            accepted_only_fields.to_string(),
            mismatches.to_string(),
        ],
        drift_rows,
        mismatch_rows,
    }
}

struct SchemaCheckFieldAnalysis {
    accepted_only: usize,
    accepted_only_generated: usize,
    generated_only: usize,
    default_mismatches: usize,
    nullability_mismatches: usize,
    mismatches: usize,
    drift_rows: Vec<[String; 4]>,
    mismatch_rows: Vec<[String; 4]>,
}

struct SchemaCheckIndexAnalysis {
    accepted_ddl: usize,
    accepted_only_generated: usize,
    generated_only: usize,
    contract_mismatches: usize,
    mismatches: usize,
    drift_rows: Vec<[String; 4]>,
    mismatch_rows: Vec<[String; 4]>,
}

struct SchemaCheckIdentityAnalysis {
    mismatches: usize,
    mismatch_rows: Vec<[String; 4]>,
}

fn analyze_entity_schema_identity(
    generated: &EntitySchemaDescription,
    accepted: &EntitySchemaDescription,
    entity_name: &str,
) -> SchemaCheckIdentityAnalysis {
    let mut mismatches = 0;
    let mut mismatch_rows = Vec::new();

    if generated.entity_path() != accepted.entity_path() {
        mismatches += 1;
        mismatch_rows.push(schema_check_detail_row(
            entity_name,
            "entity path",
            generated.entity_path(),
            accepted.entity_path(),
        ));
    }
    if generated.primary_key() != accepted.primary_key() {
        mismatches += 1;
        mismatch_rows.push(schema_check_detail_row(
            entity_name,
            "primary key",
            generated.primary_key(),
            accepted.primary_key(),
        ));
    }

    SchemaCheckIdentityAnalysis {
        mismatches,
        mismatch_rows,
    }
}

fn analyze_entity_schema_fields(
    generated: &EntitySchemaDescription,
    accepted: &EntitySchemaDescription,
    entity_name: &str,
) -> SchemaCheckFieldAnalysis {
    let generated_fields = generated
        .fields()
        .iter()
        .map(|field| (field.name(), field))
        .collect::<BTreeMap<_, _>>();
    let accepted_fields = accepted
        .fields()
        .iter()
        .map(|field| (field.name(), field))
        .collect::<BTreeMap<_, _>>();
    let mut accepted_only = 0;
    let mut accepted_only_generated = 0;
    let mut generated_only = 0;
    let mut default_mismatches = 0;
    let mut nullability_mismatches = 0;
    let mut mismatches = 0;
    let mut drift_rows = Vec::new();
    let mut mismatch_rows = Vec::new();

    for (name, accepted_field) in &accepted_fields {
        match generated_fields.get(name) {
            Some(generated_field) if *generated_field == *accepted_field => {}
            Some(generated_field) => {
                mismatches += 1;
                if generated_field.nullable() != accepted_field.nullable() {
                    nullability_mismatches += 1;
                }
                if field_default_signature(generated_field)
                    != field_default_signature(accepted_field)
                {
                    default_mismatches += 1;
                }
                mismatch_rows.push(schema_check_detail_row(
                    entity_name,
                    "field",
                    field_signature(generated_field).as_str(),
                    field_signature(accepted_field).as_str(),
                ));
            }
            None if accepted_field.origin() == "ddl" => {
                accepted_only += 1;
                drift_rows.push(schema_check_detail_row(
                    entity_name,
                    "accepted-only field",
                    "-",
                    field_signature(accepted_field).as_str(),
                ));
            }
            None => {
                mismatches += 1;
                accepted_only_generated += 1;
                mismatch_rows.push(schema_check_detail_row(
                    entity_name,
                    "accepted-only generated field",
                    "-",
                    field_signature(accepted_field).as_str(),
                ));
            }
        }
    }
    for (name, generated_field) in &generated_fields {
        if !accepted_fields.contains_key(name) {
            mismatches += 1;
            generated_only += 1;
            mismatch_rows.push(schema_check_detail_row(
                entity_name,
                "generated-only field",
                field_signature(generated_field).as_str(),
                "-",
            ));
        }
    }

    SchemaCheckFieldAnalysis {
        accepted_only,
        accepted_only_generated,
        generated_only,
        default_mismatches,
        nullability_mismatches,
        mismatches,
        drift_rows,
        mismatch_rows,
    }
}

fn analyze_entity_schema_indexes(
    generated: &EntitySchemaDescription,
    accepted: &EntitySchemaDescription,
    entity_name: &str,
) -> SchemaCheckIndexAnalysis {
    let generated_indexes = generated
        .indexes()
        .iter()
        .map(|index| (index.name(), index))
        .collect::<BTreeMap<_, _>>();
    let accepted_indexes = accepted
        .indexes()
        .iter()
        .map(|index| (index.name(), index))
        .collect::<BTreeMap<_, _>>();
    let mut accepted_ddl = 0;
    let mut accepted_only_generated = 0;
    let mut generated_only = 0;
    let mut contract_mismatches = 0;
    let mut mismatches = 0;
    let mut drift_rows = Vec::new();
    let mut mismatch_rows = Vec::new();

    for (name, accepted_index) in &accepted_indexes {
        match generated_indexes.get(name) {
            Some(generated_index) if indexes_match(generated_index, accepted_index) => {}
            Some(generated_index) => {
                mismatches += 1;
                contract_mismatches += 1;
                mismatch_rows.push(schema_check_detail_row(
                    entity_name,
                    "index",
                    index_signature(generated_index).as_str(),
                    index_signature(accepted_index).as_str(),
                ));
            }
            None if accepted_index.origin() == "ddl" => {
                accepted_ddl += 1;
                drift_rows.push(schema_check_detail_row(
                    entity_name,
                    "DDL index",
                    "-",
                    index_signature(accepted_index).as_str(),
                ));
            }
            None => {
                mismatches += 1;
                accepted_only_generated += 1;
                mismatch_rows.push(schema_check_detail_row(
                    entity_name,
                    "accepted-only generated index",
                    "-",
                    index_signature(accepted_index).as_str(),
                ));
            }
        }
    }
    for (name, generated_index) in &generated_indexes {
        if !accepted_indexes.contains_key(name) {
            mismatches += 1;
            generated_only += 1;
            mismatch_rows.push(schema_check_detail_row(
                entity_name,
                "generated-only index",
                index_signature(generated_index).as_str(),
                "-",
            ));
        }
    }

    SchemaCheckIndexAnalysis {
        accepted_ddl,
        accepted_only_generated,
        generated_only,
        contract_mismatches,
        mismatches,
        drift_rows,
        mismatch_rows,
    }
}

fn render_schema_check_report_from_summary(summary: &SchemaCheckSummary) -> String {
    let mut output = String::new();

    output.push_str("IcyDB schema check\n");
    output.push_str(
        format!(
            "  status: {}\n  entities: {}\n  accepted-only fields: {}\n  DDL-owned indexes: {}\n  mismatches: {}\n\n",
            summary.status,
            summary.entities,
            summary.accepted_only_fields,
            summary.accepted_ddl_indexes,
            summary.mismatches,
        )
        .as_str(),
    );
    append_schema_check_entity_table(&mut output, summary.entity_rows.as_slice());
    output.push('\n');
    append_schema_check_detail_table(&mut output, "accepted drift", summary.drift_rows.as_slice());
    output.push('\n');
    append_schema_check_detail_table(&mut output, "mismatches", summary.mismatch_rows.as_slice());
    output.push('\n');
    append_schema_check_recommendations(&mut output, summary.recommendations.as_slice());

    output
}

struct SchemaCheckRecommendationFacts {
    mismatches: usize,
    accepted_only_fields: usize,
    accepted_ddl_indexes: usize,
    accepted_only_generated_fields: usize,
    generated_only_fields: usize,
    field_default_mismatches: usize,
    field_nullability_mismatches: usize,
    accepted_only_generated_indexes: usize,
    generated_only_indexes: usize,
    index_contract_mismatches: usize,
}

fn schema_check_recommendations(facts: &SchemaCheckRecommendationFacts) -> Vec<String> {
    let mut recommendations = Vec::new();

    if facts.mismatches > 0 {
        recommendations.push(
            "fix: resolve generated-vs-accepted mismatches before relying on schema parity"
                .to_string(),
        );
    }
    if facts.generated_only_fields > 0 {
        recommendations.push(
            "action: generated-only fields need an accepted additive transition before deploy"
                .to_string(),
        );
    }
    if facts.accepted_only_generated_fields > 0 {
        recommendations.push(
            "fix: accepted-only generated fields require an explicit retained-slot removal policy"
                .to_string(),
        );
    }
    if facts.field_default_mismatches > 0 {
        recommendations.push(
            "fix: default drift requires an explicit ALTER COLUMN SET/DROP DEFAULT flow"
                .to_string(),
        );
    }
    if facts.field_nullability_mismatches > 0 {
        recommendations.push(
            "fix: nullability drift requires an explicit ALTER COLUMN SET/DROP NOT NULL flow"
                .to_string(),
        );
    }
    if facts.generated_only_indexes > 0 {
        recommendations.push(
            "action: generated-only indexes need accepted index publication before planner parity"
                .to_string(),
        );
    }
    if facts.accepted_only_generated_indexes > 0 {
        recommendations.push(
            "fix: accepted-only generated indexes require explicit index removal or generated schema restoration"
                .to_string(),
        );
    }
    if facts.index_contract_mismatches > 0 {
        recommendations.push(
            "fix: index contract drift requires explicit index replacement, not same-name mutation"
                .to_string(),
        );
    }
    if facts.accepted_only_fields > 0 {
        recommendations.push(
            "ok: DDL-owned accepted fields are preserved catalog drift across upgrade".to_string(),
        );
        recommendations.push(
            "action: add DDL-owned fields to Rust schema only when an explicit adoption flow exists"
                .to_string(),
        );
    }
    if facts.accepted_ddl_indexes > 0 {
        recommendations.push(
            "ok: DDL-owned accepted indexes remain planner-visible catalog drift".to_string(),
        );
    }
    if recommendations.is_empty() {
        recommendations.push("ok: generated and accepted schema are aligned".to_string());
    }

    recommendations
}

fn append_schema_check_recommendations(output: &mut String, recommendations: &[String]) {
    output.push_str("recommendations\n");
    for recommendation in recommendations {
        output.push_str("  ");
        output.push_str(recommendation);
        output.push('\n');
    }
}

pub(crate) fn render_snapshot_report(report: &StorageReport) -> String {
    let mut output = String::new();
    let data_rows = report
        .storage_data()
        .iter()
        .map(|row| {
            (
                row.path(),
                row.entries().to_string(),
                row.memory_bytes().to_string(),
            )
        })
        .collect::<Vec<_>>();
    let index_rows = report
        .storage_index()
        .iter()
        .map(|row| {
            (
                row.path(),
                row.entries().to_string(),
                row.user_entries().to_string(),
                row.system_entries().to_string(),
                row.memory_bytes().to_string(),
                format!("{:?}", row.state()),
            )
        })
        .collect::<Vec<_>>();
    let entity_rows = report
        .entity_storage()
        .iter()
        .map(|row| {
            (
                row.path(),
                row.store(),
                row.entries().to_string(),
                row.memory_bytes().to_string(),
            )
        })
        .collect::<Vec<_>>();

    output.push_str("IcyDB storage snapshot\n");
    output.push_str(
        format!(
            "  data stores: {}\n  index stores: {}\n  entities: {}\n  corrupted keys: {}\n  corrupted entries: {}\n",
            report.storage_data().len(),
            report.storage_index().len(),
            report.entity_storage().len(),
            report.corrupted_keys(),
            report.corrupted_entries(),
        )
        .as_str(),
    );
    output.push('\n');
    append_data_store_table(&mut output, data_rows.as_slice());
    output.push('\n');
    append_index_store_table(&mut output, index_rows.as_slice());
    output.push('\n');
    append_entity_table(&mut output, entity_rows.as_slice());

    output
}

pub(crate) fn render_metrics_report(report: &EventReport) -> String {
    let mut output = String::new();

    output.push_str("IcyDB metrics\n");
    output.push_str(
        format!(
            "  active window start ms: {}\n  requested window start ms: {}\n  window filter matched: {}\n  entities: {}\n",
            report.active_window_start_ms(),
            optional_u64(report.requested_window_start_ms()),
            yes_no(report.window_filter_matched()),
            report.entity_counters().len(),
        )
        .as_str(),
    );

    if let Some(counters) = report.counters() {
        append_metrics_counters(&mut output, counters);
    } else {
        output.push_str("  counters: none\n");
    }
    output.push('\n');

    let entity_rows = report
        .entity_counters()
        .iter()
        .map(|entity| {
            (
                entity.path(),
                entity.load_calls().to_string(),
                entity.save_calls().to_string(),
                entity.delete_calls().to_string(),
                entity.exec_success().to_string(),
                entity_exec_errors(entity).to_string(),
            )
        })
        .collect::<Vec<_>>();
    append_metrics_entity_table(&mut output, entity_rows.as_slice());

    output
}

fn append_metrics_counters(output: &mut String, counters: &EventCounters) {
    let ops = counters.ops();
    output.push_str(
        format!(
            "  window: {}..{} ({} ms)\n  calls: load={} save={} delete={}\n  execution: success={} errors={} aborted={}\n  rows: loaded={} saved={} deleted={} scanned={} filtered={} emitted={}\n  sql writes: insert={} insert_select={} update={} delete={} matched={} mutated={} returning={}\n  cache: query_plan_hits={} query_plan_misses={} sql_hits={} sql_misses={}\n",
            counters.window_start_ms(),
            counters.window_end_ms(),
            counters.window_duration_ms(),
            ops.load_calls(),
            ops.save_calls(),
            ops.delete_calls(),
            ops.exec_success(),
            ops_exec_errors(ops),
            ops.exec_aborted(),
            ops.rows_loaded(),
            ops.rows_saved(),
            ops.rows_deleted(),
            ops.rows_scanned(),
            ops.rows_filtered(),
            ops.rows_emitted(),
            ops.sql_insert_calls(),
            ops.sql_insert_select_calls(),
            ops.sql_update_calls(),
            ops.sql_delete_calls(),
            ops.sql_write_matched_rows(),
            ops.sql_write_mutated_rows(),
            ops.sql_write_returning_rows(),
            ops.cache_shared_query_plan_hits(),
            ops.cache_shared_query_plan_misses(),
            ops.cache_sql_compiled_command_hits(),
            ops.cache_sql_compiled_command_misses(),
        )
        .as_str(),
    );
}

fn append_schema_entity_table(output: &mut String, rows: &[[String; 6]]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &[
            "entity",
            "fields",
            "indexes",
            "relations",
            "primary key",
            "path",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_field_table(output: &mut String, rows: &[[String; 8]]) {
    output.push_str("fields\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &[
            "entity",
            "field",
            "slot",
            "type",
            "nullable",
            "pk",
            "queryable",
            "origin",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_index_table(output: &mut String, rows: &[[String; 5]]) {
    output.push_str("indexes\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &["entity", "index", "fields", "unique", "origin"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_relation_table(output: &mut String, rows: &[[String; 5]]) {
    output.push_str("relations\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &["entity", "field", "target", "strength", "cardinality"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_check_entity_table(output: &mut String, rows: &[[String; 8]]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &[
            "entity",
            "status",
            "gen fields",
            "acc fields",
            "gen indexes",
            "acc indexes",
            "acc-only fields",
            "mismatches",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
    );
}

fn append_schema_check_detail_table(output: &mut String, title: &str, rows: &[[String; 4]]) {
    output.push_str(title);
    output.push('\n');
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &["entity", "kind", "generated", "accepted"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn schema_check_detail_row(
    entity: &str,
    kind: &str,
    generated: &str,
    accepted: &str,
) -> [String; 4] {
    [
        entity.to_string(),
        kind.to_string(),
        generated.to_string(),
        accepted.to_string(),
    ]
}

fn indexes_match(generated: &EntityIndexDescription, accepted: &EntityIndexDescription) -> bool {
    generated.unique() == accepted.unique()
        && generated.fields() == accepted.fields()
        && accepted.origin() == "generated"
}

fn field_signature(field: &EntityFieldDescription) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        field.name(),
        field
            .slot()
            .map_or_else(|| "-".to_string(), |slot| slot.to_string()),
        field.kind(),
        yes_no(field.nullable()),
        yes_no(field.primary_key()),
        yes_no(field.queryable()),
        field.origin(),
    )
}

fn field_default_signature(field: &EntityFieldDescription) -> &str {
    field
        .kind()
        .split_once(" default=")
        .map_or("", |(_, default)| default)
}

fn index_signature(index: &EntityIndexDescription) -> String {
    format!(
        "{}:{}:{}:{}",
        index.name(),
        render_field_list(index.fields()),
        yes_no(index.unique()),
        index.origin(),
    )
}

fn append_data_store_table(output: &mut String, rows: &[(&str, String, String)]) {
    output.push_str("data stores\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let path_width = table_width("path", rows.iter().map(|(path, _, _)| *path));
    let entries_width = table_width(
        "entries",
        rows.iter().map(|(_, entries, _)| entries.as_str()),
    );
    let bytes_width = table_width("bytes", rows.iter().map(|(_, _, bytes)| bytes.as_str()));
    output.push_str(
        format!(
            "  {path:<path_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n",
            path = "path",
            entries = "entries",
            bytes = "bytes",
        )
        .as_str(),
    );
    for (path, entries, bytes) in rows {
        output.push_str(
            format!("  {path:<path_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n")
                .as_str(),
        );
    }
}

fn append_index_store_table(
    output: &mut String,
    rows: &[(&str, String, String, String, String, String)],
) {
    output.push_str("index stores\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let path_width = table_width("path", rows.iter().map(|(path, _, _, _, _, _)| *path));
    let entries_width = table_width(
        "entries",
        rows.iter().map(|(_, entries, _, _, _, _)| entries.as_str()),
    );
    let user_width = table_width(
        "user",
        rows.iter().map(|(_, _, user, _, _, _)| user.as_str()),
    );
    let system_width = table_width(
        "system",
        rows.iter().map(|(_, _, _, system, _, _)| system.as_str()),
    );
    let bytes_width = table_width(
        "bytes",
        rows.iter().map(|(_, _, _, _, bytes, _)| bytes.as_str()),
    );
    let state_width = table_width(
        "state",
        rows.iter().map(|(_, _, _, _, _, state)| state.as_str()),
    );
    output.push_str(
        format!(
            "  {path:<path_width$}  {entries:>entries_width$}  {user:>user_width$}  {system:>system_width$}  {bytes:>bytes_width$}  {state:<state_width$}\n",
            path = "path",
            entries = "entries",
            user = "user",
            system = "system",
            bytes = "bytes",
            state = "state",
        )
        .as_str(),
    );
    for (path, entries, user, system, bytes, state) in rows {
        output.push_str(
            format!(
                "  {path:<path_width$}  {entries:>entries_width$}  {user:>user_width$}  {system:>system_width$}  {bytes:>bytes_width$}  {state:<state_width$}\n"
            )
            .as_str(),
        );
    }
}

fn append_entity_table(output: &mut String, rows: &[(&str, &str, String, String)]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let entity_width = table_width("entity", rows.iter().map(|(entity, _, _, _)| *entity));
    let store_width = table_width("store", rows.iter().map(|(_, store, _, _)| *store));
    let entries_width = table_width(
        "entries",
        rows.iter().map(|(_, _, entries, _)| entries.as_str()),
    );
    let bytes_width = table_width("bytes", rows.iter().map(|(_, _, _, bytes)| bytes.as_str()));
    output.push_str(
        format!(
            "  {entity:<entity_width$}  {store:<store_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n",
            entity = "entity",
            store = "store",
            entries = "entries",
            bytes = "bytes",
        )
        .as_str(),
    );
    for (entity, store, entries, bytes) in rows {
        output.push_str(
            format!(
                "  {entity:<entity_width$}  {store:<store_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n"
            )
            .as_str(),
        );
    }
}

fn append_metrics_entity_table(
    output: &mut String,
    rows: &[(&str, String, String, String, String, String)],
) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let entity_width = table_width("entity", rows.iter().map(|(entity, _, _, _, _, _)| *entity));
    let load_width = table_width(
        "load",
        rows.iter().map(|(_, load, _, _, _, _)| load.as_str()),
    );
    let save_width = table_width(
        "save",
        rows.iter().map(|(_, _, save, _, _, _)| save.as_str()),
    );
    let delete_width = table_width(
        "delete",
        rows.iter().map(|(_, _, _, delete, _, _)| delete.as_str()),
    );
    let success_width = table_width(
        "success",
        rows.iter().map(|(_, _, _, _, success, _)| success.as_str()),
    );
    let errors_width = table_width(
        "errors",
        rows.iter().map(|(_, _, _, _, _, errors)| errors.as_str()),
    );
    output.push_str(
        format!(
            "  {entity:<entity_width$}  {load:>load_width$}  {save:>save_width$}  {delete:>delete_width$}  {success:>success_width$}  {errors:>errors_width$}\n",
            entity = "entity",
            load = "load",
            save = "save",
            delete = "delete",
            success = "success",
            errors = "errors",
        )
        .as_str(),
    );
    for (entity, load, save, delete, success, errors) in rows {
        output.push_str(
            format!(
                "  {entity:<entity_width$}  {load:>load_width$}  {save:>save_width$}  {delete:>delete_width$}  {success:>success_width$}  {errors:>errors_width$}\n"
            )
            .as_str(),
        );
    }
}

const fn ops_exec_errors(ops: &icydb::metrics::EventOps) -> u64 {
    ops.exec_error_corruption()
        .saturating_add(ops.exec_error_incompatible_persisted_format())
        .saturating_add(ops.exec_error_not_found())
        .saturating_add(ops.exec_error_internal())
        .saturating_add(ops.exec_error_conflict())
        .saturating_add(ops.exec_error_unsupported())
        .saturating_add(ops.exec_error_invariant_violation())
}

const fn entity_exec_errors(entity: &icydb::metrics::EntitySummary) -> u64 {
    entity
        .exec_error_corruption()
        .saturating_add(entity.exec_error_incompatible_persisted_format())
        .saturating_add(entity.exec_error_not_found())
        .saturating_add(entity.exec_error_internal())
        .saturating_add(entity.exec_error_conflict())
        .saturating_add(entity.exec_error_unsupported())
        .saturating_add(entity.exec_error_invariant_violation())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn render_field_list(fields: &[String]) -> String {
    if fields.is_empty() {
        "-".to_string()
    } else {
        fields.join(", ")
    }
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn table_width<'a>(heading: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values.map(str::len).max().unwrap_or(0).max(heading.len())
}

fn call_query(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    let output = icp_query_command(environment, canister, method, candid_arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "IcyDB query method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim(),
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}

fn call_update(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    let output = icp_update_command(environment, canister, method, candid_arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "IcyDB update method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim(),
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}
