//! Module: data::structural_field::primary_key_component::encode
//! Responsibility: primary-key-component Structural Binary encode.
//! Does not own: relation indexing policy, runtime row encode, or generic value-storage envelopes.
//! Boundary: callers provide field-kind authority; this module writes primary-key-component payloads only.

use crate::{
    db::data::structural_field::{
        binary::{push_binary_list_len, push_binary_null},
        primary_key_component::supports_primary_key_component_binary_kind,
    },
    db::key_taxonomy::PrimaryKeyComponent,
    error::InternalError,
    model::field::FieldKind,
    value::Value,
};

/// Encode strong-relation target keys into the owner-local Structural Binary
/// v1 primary-key-component lane.
pub(in crate::db) fn encode_relation_target_primary_key_components_binary_bytes(
    keys: &[PrimaryKeyComponent],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_relation_target_primary_key_components_binary_into(
        &mut encoded,
        keys,
        kind,
        field_name,
    )?;

    Ok(encoded)
}

/// Encode one canonical primary-key component into the owner-local Structural Binary v1
/// primary-key-component lane.
pub(in crate::db) fn encode_primary_key_component_field_binary_bytes(
    key: PrimaryKeyComponent,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_primary_key_component_field_binary_into(&mut encoded, key, kind, field_name)?;

    Ok(encoded)
}

/// Encode one primary-key-component runtime value through the owner-local
/// Structural Binary v1 lane.
pub(in crate::db) fn encode_primary_key_component_binary_value_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(None);
    }

    let encoded = match kind {
        FieldKind::Relation { .. } => {
            let keys = match value {
                Value::Null => Vec::new(),
                value => vec![primary_key_component_from_runtime_value(value, field_name)?],
            };
            encode_relation_target_primary_key_components_binary_bytes(&keys, kind, field_name)?
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
                keys.push(primary_key_component_from_runtime_value(item, field_name)?);
            }
            encode_relation_target_primary_key_components_binary_bytes(&keys, kind, field_name)?
        }
        _ if matches!(value, Value::Null) => {
            let mut encoded = Vec::new();
            push_binary_null(&mut encoded);
            encoded
        }
        _ => encode_primary_key_component_field_binary_bytes(
            primary_key_component_from_runtime_value(value, field_name)?,
            kind,
            field_name,
        )?,
    };

    Ok(Some(encoded))
}

// Encode one strong-relation field into the primary-key-component Structural Binary v1
// lane without routing through runtime `Value`.
fn encode_relation_target_primary_key_components_binary_into(
    out: &mut Vec<u8>,
    keys: &[PrimaryKeyComponent],
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => match keys {
            [] => {
                push_binary_null(out);
                Ok(())
            }
            [key] => {
                encode_primary_key_component_field_binary_into(out, *key, *key_kind, field_name)
            }
            _ => Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                "singular relation field received more than one target key",
            )),
        },
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            push_binary_list_len(out, keys.len());
            for key in keys {
                encode_primary_key_component_field_binary_into(out, *key, **key_kind, field_name)?;
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

// Encode one primary-key-component field into the owner-local Structural
// Binary v1 lane.
fn encode_primary_key_component_field_binary_into(
    out: &mut Vec<u8>,
    key: PrimaryKeyComponent,
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, key) {
        (FieldKind::Relation { key_kind, .. }, key) => {
            encode_primary_key_component_field_binary_into(out, key, *key_kind, field_name)
        }
        _ => crate::db::data::structural_field::primary_key_component::scalar::encode_scalar_primary_key_component_field_binary_into(
            out, key, kind, field_name,
        ),
    }
}

fn primary_key_component_from_runtime_value(
    value: &Value,
    field_name: &str,
) -> Result<PrimaryKeyComponent, InternalError> {
    PrimaryKeyComponent::from_runtime_value(value).ok_or_else(|| {
        InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("runtime value {value:?} is not admitted as a primary-key component"),
        )
    })
}
