//! Module: db::query::plan::primary_key_input_resource
//! Responsibility: planner-owned primary-key input resource summaries.
//! Does not own: read-admission policy, key-access selection, or execution.
//! Boundary: estimates pre-execution key-list work for exact-key admission.

use crate::db::resource::construction::ConstructionBudget;

use crate::{
    db::query::plan::PrimaryKeyInputResourceSummary,
    error::InternalError,
    types::AccountStorageCodec,
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Build resource facts for one raw model-level key value list.
pub(in crate::db::query) fn primary_key_input_resource_from_value_list(
    values: &[Value],
    budget: &dyn ConstructionBudget,
) -> Result<Option<PrimaryKeyInputResourceSummary>, InternalError> {
    if values.is_empty() {
        return Ok(None);
    }

    let estimated_payload_bytes = values.iter().try_fold(0u32, |total, value| {
        Ok::<_, InternalError>(total.saturating_add(estimate_value_payload_bytes(value, budget)?))
    })?;

    Ok(Some(PrimaryKeyInputResourceSummary::new(
        u32::try_from(values.len()).unwrap_or(u32::MAX),
        estimated_payload_bytes,
    )))
}

/// Visit payload structure under the caller's authority without encoding or copying.
pub(super) fn estimate_value_payload_bytes(
    value: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<u32, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    Ok(match value {
        Value::Account(_) => crate::types::Account::STORED_SIZE,
        Value::Blob(bytes) => byte_len_u32(bytes.len()),
        Value::Bool(_) => 1,
        Value::Date(_) | Value::Float32(_) => 4,
        Value::Decimal(_) => 20,
        Value::Duration(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Nat64(_)
        | Value::Timestamp(_) => 8,
        Value::Enum(value) => estimate_enum_payload_bytes(value, budget)?,
        Value::Int128(_) | Value::Nat128(_) | Value::Ulid(_) => 16,
        Value::List(values) => values.iter().try_fold(0u32, |total, value| {
            Ok::<_, InternalError>(
                total.saturating_add(estimate_value_payload_bytes(value, budget)?),
            )
        })?,
        Value::Map(entries) => entries.iter().try_fold(0u32, |total, (key, value)| {
            Ok::<_, InternalError>(
                total
                    .saturating_add(estimate_value_payload_bytes(key, budget)?)
                    .saturating_add(estimate_value_payload_bytes(value, budget)?),
            )
        })?,
        Value::Null | Value::Unit => 0,
        Value::Principal(value) => byte_len_u32(value.as_slice().len()),
        Value::Subaccount(_) | Value::U256(_) => 32,
        Value::Text(value) => byte_len_u32(value.len()),
    })
}

fn estimate_enum_payload_bytes(
    value: &ValueEnum,
    budget: &dyn ConstructionBudget,
) -> Result<u32, InternalError> {
    let mut bytes = 9_u32;
    if let Some(payload) = value.payload() {
        bytes = bytes.saturating_add(estimate_value_payload_bytes(payload, budget)?);
    }

    Ok(bytes)
}

fn byte_len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests;
