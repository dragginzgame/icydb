use crate::{
    db::data::structural_field::{
        binary::{push_binary_list_len, push_binary_null},
        storage_key::supports_storage_key_binary_kind,
    },
    error::InternalError,
    model::field::FieldKind,
    value::{StorageKey, Value, storage_key_from_runtime_value},
};

/// Encode strong-relation target keys into the owner-local Structural Binary
/// v1 storage-key lane.
pub(in crate::db) fn encode_relation_target_storage_keys_binary_bytes(
    keys: &[StorageKey],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_relation_target_storage_keys_binary_into(&mut encoded, keys, kind, field_name)?;

    Ok(encoded)
}

/// Encode one canonical `StorageKey` into the owner-local Structural Binary v1
/// storage-key lane.
pub(in crate::db) fn encode_storage_key_field_binary_bytes(
    key: StorageKey,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_storage_key_field_binary_into(&mut encoded, key, kind, field_name)?;

    Ok(encoded)
}

/// Encode one storage-key-compatible runtime value through the owner-local
/// Structural Binary v1 lane.
pub(in crate::db) fn encode_storage_key_binary_value_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !supports_storage_key_binary_kind(kind) {
        return Ok(None);
    }

    let encoded = match kind {
        FieldKind::Relation { .. } => {
            let keys = match value {
                Value::Null => Vec::new(),
                value => vec![storage_key_from_runtime_value(value).map_err(|err| {
                    InternalError::persisted_row_field_encode_failed(field_name, err)
                })?],
            };
            encode_relation_target_storage_keys_binary_bytes(&keys, kind, field_name)?
        }
        FieldKind::List(FieldKind::Relation { .. })
        | FieldKind::Set(FieldKind::Relation { .. }) => {
            let Value::List(items) = value else {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!("field kind {kind:?} does not accept runtime value {value:?}"),
                ));
            };
            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                if matches!(item, Value::Null) {
                    continue;
                }
                keys.push(storage_key_from_runtime_value(item).map_err(|err| {
                    InternalError::persisted_row_field_encode_failed(field_name, err)
                })?);
            }
            encode_relation_target_storage_keys_binary_bytes(&keys, kind, field_name)?
        }
        _ if matches!(value, Value::Null) => {
            let mut encoded = Vec::new();
            push_binary_null(&mut encoded);
            encoded
        }
        _ => encode_storage_key_field_binary_bytes(
            storage_key_from_runtime_value(value)
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?,
            kind,
            field_name,
        )?,
    };

    Ok(Some(encoded))
}

// Encode one strong-relation field into the storage-key Structural Binary v1
// lane without routing through runtime `Value`.
fn encode_relation_target_storage_keys_binary_into(
    out: &mut Vec<u8>,
    keys: &[StorageKey],
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => match keys {
            [] => {
                push_binary_null(out);
                Ok(())
            }
            [key] => encode_storage_key_field_binary_into(out, *key, *key_kind, field_name),
            _ => Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                "singular relation field received more than one target key",
            )),
        },
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            push_binary_list_len(out, keys.len());
            for key in keys {
                encode_storage_key_field_binary_into(out, *key, **key_kind, field_name)?;
            }

            Ok(())
        }
        other => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "invalid strong relation field kind during structural binary encode: {other:?}"
            ),
        )),
    }
}

// Encode one storage-key-compatible field into the owner-local Structural
// Binary v1 storage-key lane.
fn encode_storage_key_field_binary_into(
    out: &mut Vec<u8>,
    key: StorageKey,
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, key) {
        (FieldKind::Relation { key_kind, .. }, key) => {
            encode_storage_key_field_binary_into(out, key, *key_kind, field_name)
        }
        _ => crate::db::data::structural_field::storage_key::scalar::encode_scalar_storage_key_field_binary_into(
            out, key, kind, field_name,
        ),
    }
}
