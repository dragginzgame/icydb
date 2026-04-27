//! Module: db::data::persisted_row::codec
//! Defines the persisted row-slot codec boundary and re-exports the leaf codec
//! families owned by this directory module.

mod by_kind;
mod meta;
mod scalar;
pub(super) mod strategy;
mod structured;
mod traversal;

use crate::db::data::persisted_row::codec::strategy::StorageStrategy;
use crate::{
    db::data::storage::{decode as storage_decode, encode as storage_encode},
    error::InternalError,
    value::Value,
};

pub use by_kind::{
    decode_persisted_option_slot_payload_by_kind, decode_persisted_slot_payload_by_kind,
    encode_persisted_slot_payload_by_kind,
};
pub use meta::{
    decode_persisted_option_slot_payload_by_meta, decode_persisted_slot_payload_by_meta,
    encode_persisted_option_slot_payload_by_meta, encode_persisted_slot_payload_by_meta,
};
pub use scalar::{
    PersistedScalar, ScalarSlotValueRef, ScalarValueRef,
    decode_persisted_option_scalar_slot_payload, decode_persisted_scalar_slot_payload,
    encode_persisted_option_scalar_slot_payload, encode_persisted_scalar_slot_payload,
};
pub(super) use scalar::{decode_scalar_slot_value, encode_scalar_slot_value};
pub use structured::{
    decode_persisted_custom_many_slot_payload, decode_persisted_custom_slot_payload,
    encode_persisted_custom_many_slot_payload, encode_persisted_custom_slot_payload,
};

// Encode the null sentinel selected by the storage lane. This is behavior, not
// lane identity, so it stays outside `StorageStrategy`.
fn encode_null_with_strategy(
    strategy: StorageStrategy,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError> {
    match strategy {
        StorageStrategy::Scalar => Ok(scalar::encode_null_slot_payload()),
        StorageStrategy::ByKind(kind) => {
            by_kind::encode_explicit_value(kind, &Value::Null, field_name)
        }
        StorageStrategy::Structured => Ok(storage_encode::null()),
    }
}

// Single internal encode gateway for persisted slot payloads. Public entry
// points select a strategy and provide only the type-specific non-null encoder.
pub(in crate::db::data::persisted_row::codec) fn encode_with_strategy<T: ?Sized>(
    strategy: StorageStrategy,
    value: Option<&T>,
    field_name: &'static str,
    encode_value: impl FnOnce(StorageStrategy, &T, &'static str) -> Result<Vec<u8>, InternalError>,
) -> Result<Vec<u8>, InternalError> {
    value.map_or_else(
        || encode_null_with_strategy(strategy, field_name),
        |value| encode_value(strategy, value, field_name),
    )
}

// Single internal decode gateway for nullable persisted slot payloads. Strategy-
// specific null decoding stays in the provided decoder; required decode paths
// use `decode_required_with_strategy` so required-null classification happens
// exactly once.
pub(in crate::db::data::persisted_row::codec) fn decode_with_strategy<T>(
    strategy: StorageStrategy,
    bytes: &[u8],
    field_name: &'static str,
    decode_value: impl FnOnce(StorageStrategy, &[u8], &'static str) -> Result<Option<T>, InternalError>,
) -> Result<Option<T>, InternalError> {
    decode_value(strategy, bytes, field_name)
}

// Decode a required payload through the shared strategy gate. Required-null
// classification happens here once, while the caller still supplies the exact
// error message for its public contract.
pub(in crate::db::data::persisted_row::codec) fn decode_required_with_strategy<T>(
    strategy: StorageStrategy,
    bytes: &[u8],
    field_name: &'static str,
    null_message: &'static str,
    decode_value: impl FnOnce(StorageStrategy, &[u8], &'static str) -> Result<Option<T>, InternalError>,
) -> Result<T, InternalError> {
    let decoded = decode_value(strategy, bytes, field_name)?;

    require_decoded(decoded, || {
        InternalError::persisted_row_field_decode_failed(field_name, null_message)
    })
}

// Convert a decoded optional payload into a required payload while letting each
// caller keep ownership of its exact error classification and message.
pub(in crate::db::data::persisted_row::codec) fn require_decoded<T>(
    value: Option<T>,
    err: impl FnOnce() -> InternalError,
) -> Result<T, InternalError> {
    value.ok_or_else(err)
}

// Encode a runtime `Value` through the selected storage lane. Scalar is
// intentionally excluded because scalar slots use typed scalar payload codecs
// rather than the runtime `Value` union.
pub(in crate::db::data::persisted_row::codec) fn encode_runtime_value_with_strategy(
    strategy: StorageStrategy,
    value: &Value,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError> {
    match strategy {
        StorageStrategy::Scalar => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            "scalar strategy does not encode runtime values",
        )),
        StorageStrategy::ByKind(kind) => by_kind::encode_explicit_value(kind, value, field_name),
        StorageStrategy::Structured => storage_encode::value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
    }
}

// Decode one optional runtime `Value` through the selected storage lane. Both
// structural null encodings and explicit runtime Value::Null collapse to `None`
// here so higher-level decode paths do not repeat that policy.
pub(in crate::db::data::persisted_row::codec) fn decode_runtime_value_option_with_strategy(
    strategy: StorageStrategy,
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<Value>, InternalError> {
    let runtime_value = match strategy {
        StorageStrategy::Scalar => {
            return Err(InternalError::persisted_row_field_decode_failed(
                field_name,
                "scalar strategy does not decode runtime values",
            ));
        }
        StorageStrategy::ByKind(kind) => by_kind::decode_explicit_value(bytes, kind, field_name)?,
        StorageStrategy::Structured => Some(
            storage_decode::value(bytes)
                .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?,
        ),
    };

    Ok(runtime_value.filter(|value| !matches!(value, Value::Null)))
}
