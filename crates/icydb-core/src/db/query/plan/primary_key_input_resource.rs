//! Module: db::query::plan::primary_key_input_resource
//! Responsibility: planner-owned primary-key input resource summaries.
//! Does not own: read-admission policy, key-access selection, or execution.
//! Boundary: estimates pre-execution key-list work for exact-key admission.

use crate::{
    db::query::plan::PrimaryKeyInputResourceSummary,
    value::{Value, ValueEnum},
};

/// Build resource facts for one raw model-level key value list.
#[must_use]
pub(in crate::db::query) fn primary_key_input_resource_from_value_list(
    values: &[Value],
) -> Option<PrimaryKeyInputResourceSummary> {
    primary_key_input_resource_from_value_iter(
        u32::try_from(values.len()).unwrap_or(u32::MAX),
        values.iter(),
    )
}

fn primary_key_input_resource_from_value_iter<'a, I>(
    raw_term_count: u32,
    values: I,
) -> Option<PrimaryKeyInputResourceSummary>
where
    I: IntoIterator<Item = &'a Value>,
{
    if raw_term_count == 0 {
        return None;
    }

    let estimated_payload_bytes = values.into_iter().fold(0u32, |total, value| {
        total.saturating_add(estimate_value_payload_bytes(value))
    });

    Some(PrimaryKeyInputResourceSummary::new(
        raw_term_count,
        estimated_payload_bytes,
    ))
}

fn estimate_value_payload_bytes(value: &Value) -> u32 {
    match value {
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
        Value::Enum(value) => estimate_enum_payload_bytes(value),
        Value::Int128(_) | Value::Nat128(_) | Value::Ulid(_) => 16,
        Value::IntBig(value) => byte_len_u32(value.to_leb128().len()),
        Value::List(values) => values.iter().fold(0u32, |total, value| {
            total.saturating_add(estimate_value_payload_bytes(value))
        }),
        Value::Map(entries) => entries.iter().fold(0u32, |total, (key, value)| {
            total
                .saturating_add(estimate_value_payload_bytes(key))
                .saturating_add(estimate_value_payload_bytes(value))
        }),
        Value::NatBig(value) => byte_len_u32(value.to_leb128().len()),
        Value::Null | Value::Unit => 0,
        Value::Principal(value) => byte_len_u32(value.as_slice().len()),
        Value::Subaccount(_) => 32,
        Value::Text(value) => byte_len_u32(value.len()),
    }
}

fn estimate_enum_payload_bytes(value: &ValueEnum) -> u32 {
    let mut bytes = 9_u32;
    if let Some(payload) = value.payload() {
        bytes = bytes.saturating_add(estimate_value_payload_bytes(payload));
    }

    bytes
}

fn byte_len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}
