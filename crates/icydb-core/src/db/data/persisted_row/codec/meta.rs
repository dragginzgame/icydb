use crate::{
    db::data::persisted_row::codec::{
        decode_runtime_value_option_with_strategy, decode_with_strategy,
        encode_runtime_value_with_strategy, encode_with_strategy, require_decoded,
        strategy::StorageStrategy,
    },
    error::InternalError,
    traits::{FieldTypeMeta, PersistedFieldMetaCodec, RuntimeValueDecode, RuntimeValueEncode},
    value::Value,
};

// The field-meta lane is entirely selected by `FieldTypeMeta`, so one blanket
// impl keeps generated/runtime/container owners on the same storage contract
// instead of re-emitting identical per-type forwarding bodies.
impl<T> PersistedFieldMetaCodec for T
where
    T: FieldTypeMeta + RuntimeValueEncode + RuntimeValueDecode,
{
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_meta(Some(self), field_name)
    }

    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_meta(bytes, field_name)
    }

    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_meta(value.as_ref(), field_name)
    }

    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_meta_option(bytes, field_name)
    }
}

/// Decode one persisted slot payload using the field type's own runtime field
/// metadata.
pub fn decode_persisted_slot_payload_by_meta<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    T::decode_persisted_slot_payload_by_meta(bytes, field_name)
}

/// Decode one optional persisted slot payload using the inner field type's own
/// runtime field metadata.
pub fn decode_persisted_option_slot_payload_by_meta<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    T::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
}

/// Encode one persisted slot payload using the field type's own runtime field
/// metadata.
pub fn encode_persisted_slot_payload_by_meta<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    value.encode_persisted_slot_payload_by_meta(field_name)
}

/// Encode one optional persisted slot payload using the inner field type's own
/// runtime field metadata.
pub fn encode_persisted_option_slot_payload_by_meta<T>(
    value: &Option<T>,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    T::encode_persisted_option_slot_payload_by_meta(value, field_name)
}

pub(super) fn encode_meta<T>(
    value: Option<&T>,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: FieldTypeMeta + RuntimeValueEncode,
{
    let runtime_value = value.map(RuntimeValueEncode::to_value);
    let strategy = StorageStrategy::from_field_storage(T::STORAGE_DECODE, T::KIND);

    encode_with_strategy(
        strategy,
        runtime_value.as_ref(),
        field_name,
        encode_runtime_value_with_strategy,
    )
}

pub(super) fn decode_meta<T>(bytes: &[u8], field_name: &'static str) -> Result<T, InternalError>
where
    T: FieldTypeMeta + RuntimeValueDecode,
{
    let runtime_value = require_decoded(decode_meta_value_option::<T>(bytes, field_name)?, || {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            "unexpected null for non-nullable field",
        )
    })?;

    decode_runtime_value_as_meta(&runtime_value, field_name)
}

pub(super) fn decode_meta_option<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: FieldTypeMeta + RuntimeValueDecode,
{
    let Some(runtime_value) = decode_meta_value_option::<T>(bytes, field_name)? else {
        return Ok(None);
    };

    decode_runtime_value_as_meta(&runtime_value, field_name).map(Some)
}

fn decode_meta_value_option<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<Value>, InternalError>
where
    T: FieldTypeMeta,
{
    let strategy = StorageStrategy::from_field_storage(T::STORAGE_DECODE, T::KIND);

    decode_with_strategy(
        strategy,
        bytes,
        field_name,
        decode_runtime_value_option_with_strategy,
    )
}

fn decode_runtime_value_as_meta<T>(
    value: &Value,
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: FieldTypeMeta + RuntimeValueDecode,
{
    T::from_value(value).ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            format!("payload does not match {}", std::any::type_name::<T>()),
        )
    })
}
