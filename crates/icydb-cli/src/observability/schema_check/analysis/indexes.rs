//! Module: schema-check index comparison.
//! Responsibility: classify generated-vs-accepted index drift and mismatches.
//! Does not own: entity identity checks, field comparison, or recommendation text.
//! Boundary: returns aggregate index facts and detail rows to schema-check analysis.

use std::collections::BTreeMap;

use icydb::db::{EntityIndexDescription, EntitySchemaDescription};

use crate::observability::render::{render_field_list, yes_no};

use super::{DDL_ORIGIN, GENERATED_ORIGIN, schema_check_detail_row};

pub(super) struct SchemaCheckIndexAnalysis {
    pub(super) accepted_ddl: usize,
    pub(super) accepted_only_generated: usize,
    pub(super) generated_only: usize,
    pub(super) contract_mismatches: usize,
    pub(super) mismatches: usize,
    pub(super) drift_rows: Vec<[String; 4]>,
    pub(super) mismatch_rows: Vec<[String; 4]>,
}

pub(super) fn analyze_entity_schema_indexes(
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
            None if accepted_index.origin() == DDL_ORIGIN => {
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

fn indexes_match(generated: &EntityIndexDescription, accepted: &EntityIndexDescription) -> bool {
    generated.unique() == accepted.unique()
        && generated.fields() == accepted.fields()
        && generated.origin() == GENERATED_ORIGIN
        && accepted.origin() == GENERATED_ORIGIN
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
