//! Module: schema-check field comparison.
//! Responsibility: classify generated-vs-accepted field drift and mismatches.
//! Does not own: entity identity checks, index comparison, or recommendation text.
//! Boundary: returns aggregate field facts and detail rows to schema-check analysis.

use std::collections::BTreeMap;

use icydb::db::{EntityFieldDescription, EntitySchemaDescription};

use crate::observability::{
    render::yes_no,
    schema_check::analysis::{DDL_ORIGIN, schema_check_detail_row},
};

pub(super) struct SchemaCheckFieldAnalysis {
    pub(super) accepted_only: usize,
    pub(super) accepted_only_generated: usize,
    pub(super) generated_only: usize,
    pub(super) default_mismatches: usize,
    pub(super) nullability_mismatches: usize,
    pub(super) mismatches: usize,
    pub(super) drift_rows: Vec<[String; 4]>,
    pub(super) mismatch_rows: Vec<[String; 4]>,
}

pub(super) fn analyze_entity_schema_fields(
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
            None if accepted_field.origin() == DDL_ORIGIN => {
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
