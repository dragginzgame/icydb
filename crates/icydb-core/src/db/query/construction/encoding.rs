//! Shared construction bounds for encoded values and canonical value hashes.
//! Owns admission only: canonical encoding and map comparison stay with their writers.

use crate::{
    db::{predicate::CoercionId, query::construction::ConstructionBudget},
    error::InternalError,
    value::{Value, hash_value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

// Covers incoming framing, tags/counts and fixed scalar payloads in both writers.
// Accounts additionally reserve owner/subaccount framing below.
pub(in crate::db) const ENCODING_NODE_BYTES: u64 = 64;

impl dyn ConstructionBudget + '_ {
    /// Admit traversal, streamed bytes and map scratch before canonical hashing.
    /// The hash writer and its successful identity are shared with other consumers.
    pub(in crate::db) fn hash_value(&self, value: &Value) -> Result<[u8; 16], InternalError> {
        self.admit_value_hash(value)?;
        hash_value(value)
    }

    /// Admit the same hash work when a shared encoder owns the later write.
    /// This sizes only; it must not compute and discard a duplicate digest.
    pub(in crate::db) fn admit_value_hash(&self, value: &Value) -> Result<(), InternalError> {
        let bytes = encoded_value_capacity(value, CoercionId::Strict, self)?;
        self.charge(Resource::PredicateExpressionSteps, bytes.saturating_mul(2))
    }
}

/// Bound encoded extent and admit coercion/map scratch without encoding or sorting.
/// Nested comparison work is separate; this is not a complete sorting-work bound.
pub(in crate::db) fn encoded_value_capacity(
    value: &Value,
    coercion: CoercionId,
    budget: &dyn ConstructionBudget,
) -> Result<u64, InternalError> {
    budget.charge(Resource::NestedValueSteps, 1)?;
    let mut bytes = ENCODING_NODE_BYTES;
    match value {
        Value::Text(text) => {
            let len = text.len() as u64;
            if matches!(coercion, CoercionId::TextCasefold) {
                let (backing, steps) = lower_text_construction_allowance(text.len());
                budget.charge(Resource::TemporaryBytes, backing)?;
                budget.charge(Resource::PredicateExpressionSteps, steps)?;
                bytes = bytes.saturating_add(len.saturating_mul(2));
            } else {
                bytes = bytes.saturating_add(len);
            }
        }
        Value::Blob(blob) => bytes = bytes.saturating_add(blob.len() as u64),
        Value::IntBig(integer) => {
            // Exact signed length may inspect trailing zero limbs. Admit that
            // scan before sizing, not after an unbounded metadata helper.
            budget.charge(
                Resource::PredicateExpressionSteps,
                integer.magnitude_bits().div_ceil(32),
            )?;
            bytes = bytes.saturating_add(integer.leb128_len());
        }
        Value::NatBig(integer) => bytes = bytes.saturating_add(integer.leb128_len()),
        Value::Account(_) => bytes = bytes.saturating_add(ENCODING_NODE_BYTES),
        Value::List(items) => {
            for item in items {
                bytes =
                    bytes.saturating_add(encoded_value_capacity(item, CoercionId::Strict, budget)?);
            }
        }
        Value::Map(entries) => {
            // ordered_map_entries may retain references when input is unordered.
            // Include Vec's small-allocation floor without doing a second sort.
            if !entries.is_empty() {
                budget.charge(
                    Resource::TemporaryBytes,
                    (entries.len().max(4) as u64)
                        .saturating_mul(size_of::<&(Value, Value)>() as u64),
                )?;
            }
            for (key, value) in entries {
                bytes = bytes
                    .saturating_add(encoded_value_capacity(key, CoercionId::Strict, budget)?)
                    .saturating_add(encoded_value_capacity(value, CoercionId::Strict, budget)?);
            }
        }
        Value::Enum(value) => {
            if let Some(payload) = value.payload() {
                bytes = bytes.saturating_add(encoded_value_capacity(
                    payload,
                    CoercionId::Strict,
                    budget,
                )?);
            }
        }
        Value::Bool(_)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::Duration(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Int128(_)
        | Value::Nat64(_)
        | Value::Nat128(_)
        | Value::Null
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Timestamp(_)
        | Value::U256(_)
        | Value::Ulid(_)
        | Value::Unit => {}
    }
    Ok(bytes)
}
