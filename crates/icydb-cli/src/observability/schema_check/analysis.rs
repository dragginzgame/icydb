//! Module: schema-check report analysis.
//! Responsibility: summarize generated-vs-accepted schema check descriptions.
//! Does not own: canister calls, text rendering, or schema mutation semantics.
//! Boundary: turns runtime descriptions into table rows and recommendations.

use std::collections::BTreeMap;

use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntitySchemaCheckDescription,
    EntitySchemaDescription,
};

use crate::observability::render::{render_field_list, yes_no};

#[derive(Debug)]
pub(super) struct SchemaCheckSummary {
    pub(super) status: &'static str,
    pub(super) entities: usize,
    pub(super) accepted_only_fields: usize,
    pub(super) accepted_ddl_indexes: usize,
    pub(super) mismatches: usize,
    pub(super) recommendations: Vec<String>,
    pub(super) entity_rows: Vec<[String; 8]>,
    pub(super) drift_rows: Vec<[String; 4]>,
    pub(super) mismatch_rows: Vec<[String; 4]>,
}

pub(super) fn analyze_schema_check(report: &[EntitySchemaCheckDescription]) -> SchemaCheckSummary {
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
