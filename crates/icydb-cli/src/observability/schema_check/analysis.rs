//! Module: schema-check report analysis.
//! Responsibility: summarize generated-vs-accepted schema check descriptions.
//! Does not own: canister calls, text rendering, or schema mutation semantics.
//! Boundary: turns runtime descriptions into table rows and recommendations.

mod fields;
mod indexes;

use icydb::db::{EntitySchemaCheckDescription, EntitySchemaDescription};

use crate::observability::{
    render::render_field_list,
    schema_check::recommendations::{SchemaCheckRecommendationFacts, schema_check_recommendations},
};

use self::{fields::analyze_entity_schema_fields, indexes::analyze_entity_schema_indexes};

const DDL_ORIGIN: &str = "ddl";
const GENERATED_ORIGIN: &str = "generated";

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

    SchemaCheckSummary {
        status: schema_check_status(mismatches, accepted_only_fields, accepted_ddl_indexes),
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
            schema_check_status(mismatches, accepted_only_fields, accepted_ddl_indexes).to_string(),
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

const fn schema_check_status(
    mismatches: usize,
    accepted_only_fields: usize,
    accepted_ddl_indexes: usize,
) -> &'static str {
    if mismatches > 0 {
        "mismatch"
    } else if accepted_only_fields > 0 || accepted_ddl_indexes > 0 {
        "drift"
    } else {
        "ok"
    }
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
    if generated.primary_key_fields() != accepted.primary_key_fields() {
        mismatches += 1;
        let generated_primary_key = render_field_list(generated.primary_key_fields());
        let accepted_primary_key = render_field_list(accepted.primary_key_fields());
        mismatch_rows.push(schema_check_detail_row(
            entity_name,
            "primary key",
            generated_primary_key.as_str(),
            accepted_primary_key.as_str(),
        ));
    }

    SchemaCheckIdentityAnalysis {
        mismatches,
        mismatch_rows,
    }
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
