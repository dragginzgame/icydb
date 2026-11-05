//! Conservative construction admission for the runtime-to-input enum seam.
//! No schema interpretation or alternate value conversion lives here.

use crate::{
    db::{
        QueryError,
        predicate::CoercionId,
        query::preparation::PreparationWork,
        resource::{
            budget::sort_comparison_allowance, construction::admit_value_comparison_repetitions,
        },
    },
    value::{InputValue, Value},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Admit inspection and payload scans before conversion or canonical validation.
/// Canonical enums use the ordinary copy owner; return whether that path applies.
pub(super) fn admit_enum_input_construction(
    value: &Value,
    work: &PreparationWork<'_>,
) -> Result<bool, QueryError> {
    let (bytes, contains_enum) = input_construction_bytes(value, work)?;
    if !contains_enum {
        work.charge(Resource::TemporaryBytes, bytes)?;
        work.charge(Resource::PredicateExpressionSteps, bytes)?;
    }
    Ok(contains_enum)
}

fn input_construction_bytes(
    value: &Value,
    work: &PreparationWork<'_>,
) -> Result<(u64, bool), QueryError> {
    // Includes inspection and the following accepted validation/conversion visit.
    work.charge(Resource::NestedValueSteps, 2)?;
    // Eight cells per source node cover cumulative geometric Vec growth during
    // lifting (<4 cells per item), accepted output and record reorder scratch.
    // Nodes include map keys and a root cell, so singleton containers are covered.
    // Scalar payloads move from lifted input into output rather than copying again.
    let mut bytes = (size_of::<InputValue>().max(size_of::<Value>()) as u64) * 8;
    let mut contains_enum = false;
    match value {
        Value::Text(text) => {
            // Catalog validation may scan UTF-8 or compare record labels before
            // the admitted copy. Reserve that work even for canonical enums;
            // later copy/conversion charges own their separate byte work.
            work.charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
            bytes = bytes.saturating_add(text.len() as u64);
        }
        Value::Blob(blob) => bytes = bytes.saturating_add(blob.len() as u64),
        Value::List(values) => {
            admit_collection_ordering(values.len(), values.iter(), work)?;
            for value in values {
                let (child_bytes, child_enum) = input_construction_bytes(value, work)?;
                bytes = bytes.saturating_add(child_bytes);
                contains_enum |= child_enum;
            }
        }
        Value::Map(entries) => {
            admit_collection_ordering(entries.len(), entries.iter().map(|(key, _)| key), work)?;
            for (key, value) in entries {
                for child in [key, value] {
                    let (child_bytes, child_enum) = input_construction_bytes(child, work)?;
                    bytes = bytes.saturating_add(child_bytes);
                    contains_enum |= child_enum;
                }
            }
        }
        Value::Enum(value) => {
            contains_enum = true;
            if let Some(payload) = value.payload() {
                input_construction_bytes(payload, work)?;
            }
        }
        _ => {}
    }
    Ok((bytes, contains_enum))
}

// Catalog admission owns whether a list is a set or a map is a record. Reserve
// possible ordering and adjacent uniqueness checks without interpreting that
// schema again here. Canonical values may only need the adjacent scan; ordinary
// lists may need neither. Both retain this conservative preflight allowance.
fn admit_collection_ordering<'a>(
    count: usize,
    values: impl Iterator<Item = &'a Value>,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if count < 2 {
        return Ok(());
    }
    let comparisons = sort_comparison_allowance(count).saturating_add(count as u64 - 1);
    work.charge(Resource::SortEntries, count as u64)?;
    work.charge(Resource::SortComparisons, comparisons)?;
    admit_value_comparison_repetitions(
        values,
        comparisons,
        CoercionId::Strict,
        &mut |resource, amount| work.charge(resource, amount),
    )
}
