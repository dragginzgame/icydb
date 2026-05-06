use crate::{
    db::{
        data::{
            StructuralFieldDecodeContract, StructuralRowContract, StructuralRowFieldBytes,
            decode_runtime_value_from_accepted_field_contract,
            persisted_row::{
                codec::{ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value},
                reader::metrics::StructuralReadProbe,
            },
        },
        schema::{AcceptedFieldDecodeContract, PersistedFieldKind},
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
    let primary_key_slot = contract.primary_key_slot();
    let primary_key_name = contract.field_name(primary_key_slot)?;
    let raw_value = field_bytes
        .field(primary_key_slot)
        .ok_or_else(|| InternalError::persisted_row_declared_field_missing(primary_key_name))?;

    validate_storage_key_from_primary_key_bytes_with_contract(&contract, raw_value, expected_key)
}

// Validate one raw primary-key payload through the row contract owner. Sparse
// row readers that do not own a full field-span wrapper use this so they do not
// repeat accepted-vs-generated primary-key selection locally.
pub(super) fn validate_storage_key_from_primary_key_bytes_with_contract(
    contract: &StructuralRowContract,
    raw_value: &[u8],
    expected_key: StorageKey,
) -> Result<(), InternalError> {
    let primary_key_slot = contract.primary_key_slot();
    if let Some(primary_key_field) = contract.accepted_field_decode_contract(primary_key_slot) {
        return validate_storage_key_from_primary_key_bytes_with_accepted_field(
            raw_value,
            primary_key_field,
            expected_key,
        );
    }

    let primary_key_field = contract.field_decode_contract(primary_key_slot)?;

    validate_storage_key_from_primary_key_bytes_with_field(
        raw_value,
        primary_key_field,
        expected_key,
    )
}

// Validate one primary-key payload through accepted persisted schema metadata.
// This is the schema-runtime counterpart to the generated-compatible helper
// above and keeps accepted primary-key decode from reopening `FieldKind`.
fn validate_storage_key_from_primary_key_bytes_with_accepted_field(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
    expected_key: StorageKey,
) -> Result<(), InternalError> {
    let decoded_key = match field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            match decode_scalar_slot_value(raw_value, codec, field.field_name())? {
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
                                field.field_name()
                            ),
                        )
                    })?
                }
            }
        }
        LeafCodec::StructuralFallback => {
            let value = decode_runtime_value_from_accepted_field_contract(field, raw_value)
                .map_err(|err| {
                    InternalError::persisted_row_primary_key_not_storage_encodable(
                        expected_key,
                        err,
                    )
                })?;

            if matches!(value, Value::Null) {
                return Err(InternalError::persisted_row_primary_key_slot_missing(
                    expected_key,
                ));
            }

            value.as_storage_key().ok_or_else(|| {
                InternalError::persisted_row_primary_key_not_storage_encodable(
                    expected_key,
                    format!(
                        "primary-key field '{}' is not storage-key compatible",
                        field.field_name()
                    ),
                )
            })?
        }
    };

    if decoded_key != expected_key {
        return Err(InternalError::persisted_row_key_mismatch(
            expected_key,
            decoded_key,
        ));
    }

    Ok(())
}

// Validate the persisted primary-key payload directly from caller-supplied raw
// field bytes so both full-span and narrow sparse reads share one decode rule.
fn validate_storage_key_from_primary_key_bytes_with_field(
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

// Materialize one accepted-schema primary-key slot from the authoritative row
// key. This mirrors generated primary-key materialization while keeping
// relation-key recursion on accepted persisted kind metadata.
pub(super) fn materialize_primary_key_slot_value_from_expected_key_with_accepted_field(
    field: AcceptedFieldDecodeContract<'_>,
    expected_key: StorageKey,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    probe.record_validated_slot();
    if matches!(field.leaf_codec(), LeafCodec::StructuralFallback) {
        probe.record_validated_non_scalar();
        probe.record_materialized_non_scalar();
    }

    materialize_primary_key_value_from_persisted_kind(field.kind(), expected_key).map_err(|err| {
        InternalError::persisted_row_decode_failed(format!(
            "{err}: field='{}' kind={:?}",
            field.field_name(),
            field.kind(),
        ))
    })
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

// Rebuild one primary-key runtime value through the accepted persisted kind.
// Only storage-key-compatible shapes are accepted; relation keys recurse to
// their declared target-key kind exactly like the generated bridge.
fn materialize_primary_key_value_from_persisted_kind(
    kind: &PersistedFieldKind,
    storage_key: StorageKey,
) -> Result<Value, String> {
    match (kind, storage_key) {
        (PersistedFieldKind::Account, StorageKey::Account(value)) => Ok(Value::Account(value)),
        (PersistedFieldKind::Int, StorageKey::Int(value)) => Ok(Value::Int(value)),
        (PersistedFieldKind::Principal, StorageKey::Principal(value)) => {
            Ok(Value::Principal(value))
        }
        (PersistedFieldKind::Relation { key_kind, .. }, storage_key) => {
            materialize_primary_key_value_from_persisted_kind(key_kind, storage_key)
        }
        (PersistedFieldKind::Subaccount, StorageKey::Subaccount(value)) => {
            Ok(Value::Subaccount(value))
        }
        (PersistedFieldKind::Timestamp, StorageKey::Timestamp(value)) => {
            Ok(Value::Timestamp(value))
        }
        (PersistedFieldKind::Uint, StorageKey::Uint(value)) => Ok(Value::Uint(value)),
        (PersistedFieldKind::Ulid, StorageKey::Ulid(value)) => Ok(Value::Ulid(value)),
        (PersistedFieldKind::Unit, StorageKey::Unit) => Ok(Value::Unit),
        (kind, storage_key) => Err(format!(
            "validated primary-key storage key does not match accepted field kind: kind={kind:?} storage_key={storage_key:?}",
        )),
    }
}
