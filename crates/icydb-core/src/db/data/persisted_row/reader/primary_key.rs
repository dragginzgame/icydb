use crate::{
    db::data::{
        StructuralFieldDecodeContract, StructuralRowContract, StructuralRowFieldBytes,
        persisted_row::{
            codec::{ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value},
            reader::metrics::StructuralReadProbe,
        },
    },
    error::InternalError,
    model::field::{FieldKind, LeafCodec},
    value::{StorageKey, Value},
};

// Convert one scalar slot fast-path value into its storage-key form when the
// field kind is storage-key-compatible.
const fn storage_key_from_scalar_ref(value: ScalarValueRef<'_>) -> Option<StorageKey> {
    match value {
        ScalarValueRef::Int(value) => Some(StorageKey::Int(value)),
        ScalarValueRef::Principal(value) => Some(StorageKey::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(StorageKey::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(StorageKey::Timestamp(value)),
        ScalarValueRef::Uint(value) => Some(StorageKey::Uint(value)),
        ScalarValueRef::Ulid(value) => Some(StorageKey::Ulid(value)),
        ScalarValueRef::Unit => Some(StorageKey::Unit),
        _ => None,
    }
}

// Validate the persisted primary-key payload against one authoritative storage
// key directly from structural field bytes.
pub(super) fn validate_storage_key_from_field_bytes(
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    expected_key: StorageKey,
) -> Result<(), InternalError> {
    let primary_key_field = contract.field_decode_contract(contract.primary_key_slot())?;
    let primary_key_slot = contract.primary_key_slot();
    let raw_value = field_bytes.field(primary_key_slot).ok_or_else(|| {
        InternalError::persisted_row_declared_field_missing(primary_key_field.name())
    })?;

    validate_storage_key_from_primary_key_bytes_with_field(
        raw_value,
        primary_key_field,
        expected_key,
    )
}

// Validate the persisted primary-key payload directly from caller-supplied raw
// field bytes so both full-span and narrow sparse reads share one decode rule.
pub(super) fn validate_storage_key_from_primary_key_bytes_with_field(
    raw_value: &[u8],
    field: StructuralFieldDecodeContract,
    expected_key: StorageKey,
) -> Result<(), InternalError> {
    let decoded_key = match field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            match decode_scalar_slot_value(raw_value, codec, field.name())? {
                ScalarSlotValueRef::Null => {
                    return Err(InternalError::persisted_row_primary_key_slot_missing(
                        expected_key,
                    ));
                }
                ScalarSlotValueRef::Value(value) => {
                    storage_key_from_scalar_ref(value).ok_or_else(|| {
                        InternalError::persisted_row_primary_key_not_storage_encodable(
                            expected_key,
                            format!(
                                "scalar primary-key field '{}' is not storage-key compatible",
                                field.name()
                            ),
                        )
                    })?
                }
            }
        }
        LeafCodec::StructuralFallback => crate::db::data::decode_storage_key_field_bytes(
            raw_value,
            field.kind(),
        )
        .map_err(|err| {
            InternalError::persisted_row_primary_key_not_storage_encodable(expected_key, err)
        })?,
    };

    if decoded_key != expected_key {
        return Err(InternalError::persisted_row_key_mismatch(
            expected_key,
            decoded_key,
        ));
    }

    Ok(())
}

// Materialize the already-validated primary-key slot directly from the
// authoritative storage key carried by the row boundary.
pub(super) fn materialize_primary_key_slot_value_from_expected_key(
    field: StructuralFieldDecodeContract,
    expected_key: StorageKey,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    probe.record_validated_slot();
    if matches!(field.leaf_codec(), LeafCodec::StructuralFallback) {
        probe.record_validated_non_scalar();
        probe.record_materialized_non_scalar();
    }

    match (field.kind(), expected_key) {
        (FieldKind::Account, StorageKey::Account(value)) => Ok(Value::Account(value)),
        (FieldKind::Int, StorageKey::Int(value)) => Ok(Value::Int(value)),
        (FieldKind::Principal, StorageKey::Principal(value)) => Ok(Value::Principal(value)),
        (FieldKind::Relation { key_kind, .. }, storage_key) => {
            materialize_primary_key_value_from_kind(*key_kind, storage_key)
        }
        (FieldKind::Subaccount, StorageKey::Subaccount(value)) => Ok(Value::Subaccount(value)),
        (FieldKind::Timestamp, StorageKey::Timestamp(value)) => Ok(Value::Timestamp(value)),
        (FieldKind::Uint, StorageKey::Uint(value)) => Ok(Value::Uint(value)),
        (FieldKind::Ulid, StorageKey::Ulid(value)) => Ok(Value::Ulid(value)),
        (FieldKind::Unit, StorageKey::Unit) => Ok(Value::Unit),
        (kind, storage_key) => Err(InternalError::persisted_row_decode_failed(format!(
            "validated primary-key storage key does not match field kind: field='{}' kind={kind:?} storage_key={storage_key:?}",
            field.name(),
        ))),
    }
}

// Rebuild one semantic primary-key value from the already-authoritative
// storage key using the field-kind compatibility contract. Relation keys reuse
// the same scalar storage-key shape as their declared target key kind.
fn materialize_primary_key_value_from_kind(
    kind: FieldKind,
    storage_key: StorageKey,
) -> Result<Value, InternalError> {
    match (kind, storage_key) {
        (FieldKind::Account, StorageKey::Account(value)) => Ok(Value::Account(value)),
        (FieldKind::Int, StorageKey::Int(value)) => Ok(Value::Int(value)),
        (FieldKind::Principal, StorageKey::Principal(value)) => Ok(Value::Principal(value)),
        (FieldKind::Relation { key_kind, .. }, storage_key) => {
            materialize_primary_key_value_from_kind(*key_kind, storage_key)
        }
        (FieldKind::Subaccount, StorageKey::Subaccount(value)) => Ok(Value::Subaccount(value)),
        (FieldKind::Timestamp, StorageKey::Timestamp(value)) => Ok(Value::Timestamp(value)),
        (FieldKind::Uint, StorageKey::Uint(value)) => Ok(Value::Uint(value)),
        (FieldKind::Ulid, StorageKey::Ulid(value)) => Ok(Value::Ulid(value)),
        (FieldKind::Unit, StorageKey::Unit) => Ok(Value::Unit),
        (kind, storage_key) => Err(InternalError::persisted_row_decode_failed(format!(
            "validated primary-key storage key does not match field kind kind={kind:?} storage_key={storage_key:?}",
        ))),
    }
}
