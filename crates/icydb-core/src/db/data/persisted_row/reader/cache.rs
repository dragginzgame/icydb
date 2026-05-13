use crate::{
    db::data::{
        ScalarValueRef, StructuralRowContract, StructuralRowFieldBytes,
        persisted_row::{
            codec::ScalarSlotValueRef, contract::decode_scalar_slot_value_from_row_contract,
        },
    },
    error::InternalError,
    model::field::LeafCodec,
    value::Value,
};
use std::cell::OnceCell;

///
/// ValidatedScalarSlotValue
///
/// ValidatedScalarSlotValue stores the compact post-validation shape for one
/// scalar slot.
/// Payload-backed scalar variants keep only enough state to prove validation
/// happened, while fixed-width variants can be reconstructed without decoding
/// the persisted bytes again.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::data::persisted_row) enum ValidatedScalarSlotValue {
    Null,
    Blob,
    Bool(bool),
    Date(crate::types::Date),
    Duration(crate::types::Duration),
    Float32(crate::types::Float32),
    Float64(crate::types::Float64),
    Int(i64),
    Principal(crate::types::Principal),
    Subaccount(crate::types::Subaccount),
    Text,
    Timestamp(crate::types::Timestamp),
    Nat(u64),
    Ulid(crate::types::Ulid),
    Unit,
}

///
/// CachedSlotValue
///
/// CachedSlotValue tracks whether one slot has already been validated, and
/// whether its semantic runtime `Value` has been materialized yet, during the
/// current structural row access pass.
///

#[derive(Debug)]
pub(in crate::db::data::persisted_row) enum CachedSlotValue {
    Scalar {
        validated: OnceCell<ValidatedScalarSlotValue>,
        materialized: OnceCell<Value>,
    },
    Deferred {
        materialized: OnceCell<Value>,
    },
}

// Build the initial per-slot cache shape from the static field contract only.
// This avoids a row-open decode loop while still letting access-time readers
// branch cheaply by leaf codec.
pub(super) fn build_initial_slot_cache(contract: &StructuralRowContract) -> Vec<CachedSlotValue> {
    (0..contract.field_count())
        .map(|slot| {
            match contract
                .field_leaf_codec(slot)
                .expect("cache initialization only visits declared structural slots")
            {
                LeafCodec::Scalar(_) => CachedSlotValue::Scalar {
                    validated: OnceCell::new(),
                    materialized: OnceCell::new(),
                },
                LeafCodec::StructuralFallback => CachedSlotValue::Deferred {
                    materialized: OnceCell::new(),
                },
            }
        })
        .collect()
}

// Freeze one validated scalar slot into a compact cache state that preserves
// fixed-width scalar payloads by value and defers payload-backed scalar
// materialization until a caller actually asks for a runtime `Value`.
pub(super) const fn validated_scalar_slot_value(
    value: ScalarSlotValueRef<'_>,
) -> ValidatedScalarSlotValue {
    match value {
        ScalarSlotValueRef::Null => ValidatedScalarSlotValue::Null,
        ScalarSlotValueRef::Value(value) => match value {
            ScalarValueRef::Blob(_) => ValidatedScalarSlotValue::Blob,
            ScalarValueRef::Bool(value) => ValidatedScalarSlotValue::Bool(value),
            ScalarValueRef::Date(value) => ValidatedScalarSlotValue::Date(value),
            ScalarValueRef::Duration(value) => ValidatedScalarSlotValue::Duration(value),
            ScalarValueRef::Float32(value) => ValidatedScalarSlotValue::Float32(value),
            ScalarValueRef::Float64(value) => ValidatedScalarSlotValue::Float64(value),
            ScalarValueRef::Int(value) => ValidatedScalarSlotValue::Int(value),
            ScalarValueRef::Principal(value) => ValidatedScalarSlotValue::Principal(value),
            ScalarValueRef::Subaccount(value) => ValidatedScalarSlotValue::Subaccount(value),
            ScalarValueRef::Text(_) => ValidatedScalarSlotValue::Text,
            ScalarValueRef::Timestamp(value) => ValidatedScalarSlotValue::Timestamp(value),
            ScalarValueRef::Nat(value) => ValidatedScalarSlotValue::Nat(value),
            ScalarValueRef::Ulid(value) => ValidatedScalarSlotValue::Ulid(value),
            ScalarValueRef::Unit => ValidatedScalarSlotValue::Unit,
        },
    }
}

// Borrow one scalar slot view from the validated cache without rebuilding
// fixed-width scalar values from persisted bytes.
pub(super) fn scalar_slot_value_ref_from_validated<'a>(
    validated: ValidatedScalarSlotValue,
    contract: StructuralRowContract,
    field_bytes: &'a StructuralRowFieldBytes<'a>,
    slot: usize,
) -> Result<ScalarSlotValueRef<'a>, InternalError> {
    match validated {
        ValidatedScalarSlotValue::Null => Ok(ScalarSlotValueRef::Null),
        ValidatedScalarSlotValue::Blob | ValidatedScalarSlotValue::Text => {
            let field_name = contract.field_name(slot)?;
            let raw_value = field_bytes
                .field(slot)
                .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))?;
            decode_payload_backed_validated_scalar_slot_value(contract, raw_value, slot)
        }
        ValidatedScalarSlotValue::Bool(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Bool(value)))
        }
        ValidatedScalarSlotValue::Date(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Date(value)))
        }
        ValidatedScalarSlotValue::Duration(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Duration(value)))
        }
        ValidatedScalarSlotValue::Float32(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Float32(value)))
        }
        ValidatedScalarSlotValue::Float64(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Float64(value)))
        }
        ValidatedScalarSlotValue::Int(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Int(value)))
        }
        ValidatedScalarSlotValue::Principal(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Principal(value)))
        }
        ValidatedScalarSlotValue::Subaccount(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Subaccount(value)))
        }
        ValidatedScalarSlotValue::Timestamp(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(value)))
        }
        ValidatedScalarSlotValue::Nat(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Nat(value)))
        }
        ValidatedScalarSlotValue::Ulid(value) => {
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(value)))
        }
        ValidatedScalarSlotValue::Unit => Ok(ScalarSlotValueRef::Value(ScalarValueRef::Unit)),
    }
}

// Decode a payload-backed scalar after scalar validation has already proved the
// slot shape. Accepted row-layout contracts remain the first authority for
// saved-schema rows, while generated-compatible contracts are retained only for
// generated-only readers.
fn decode_payload_backed_validated_scalar_slot_value(
    contract: StructuralRowContract,
    raw_value: &[u8],
    slot: usize,
) -> Result<ScalarSlotValueRef<'_>, InternalError> {
    decode_scalar_slot_value_from_row_contract(
        &contract,
        slot,
        raw_value,
        "accepted validated scalar cache routed through non-scalar field contract",
        "validated scalar cache routed through non-scalar field contract",
    )
}

// Materialize one validated scalar slot into the runtime `Value` enum.
pub(super) fn materialize_validated_scalar_slot_value(
    validated: ValidatedScalarSlotValue,
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    slot: usize,
) -> Result<Value, InternalError> {
    match scalar_slot_value_ref_from_validated(validated, contract, field_bytes, slot)? {
        ScalarSlotValueRef::Null => Ok(Value::Null),
        ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
    }
}
