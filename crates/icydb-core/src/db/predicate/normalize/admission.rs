//! Conservative backing admission for the existing runtime-to-input enum seam.
//! No schema interpretation or alternate value conversion lives here.

use crate::{
    db::{QueryError, query::preparation::PreparationWork},
    value::{InputValue, Value},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Inspect before conversion; canonical enum operands instead use validation and
/// the ordinary admitted copy owner. Return whether that canonical path applies.
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
        Value::Text(text) => bytes = bytes.saturating_add(text.len() as u64),
        Value::Blob(blob) => bytes = bytes.saturating_add(blob.len() as u64),
        Value::IntBig(value) => {
            bytes = bytes.saturating_add(value.magnitude_bits().div_ceil(64) * 8);
        }
        Value::NatBig(value) => {
            bytes = bytes.saturating_add(value.magnitude_bits().div_ceil(64) * 8);
        }
        Value::List(values) => {
            for value in values {
                let (child_bytes, child_enum) = input_construction_bytes(value, work)?;
                bytes = bytes.saturating_add(child_bytes);
                contains_enum |= child_enum;
            }
        }
        Value::Map(entries) => {
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
