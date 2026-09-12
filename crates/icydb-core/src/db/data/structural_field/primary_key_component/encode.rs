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
    db::schema::AcceptedFieldKind,
    error::InternalError,
    value::Value,
};

/// Encode one primary-key-component runtime value through the owner-local
/// Structural Binary v1 lane.
pub(in crate::db) fn push_primary_key_component_binary_value_bytes(
    out: &mut Vec<u8>,
    kind: &AcceptedFieldKind,
    value: &Value,
    field_name: &str,
) -> Result<bool, InternalError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(false);
    }
    match kind {
        AcceptedFieldKind::Relation { .. } => match value {
            Value::Null => push_binary_null(out),
            value => encode_primary_key_component_field_binary_into(
                out,
                primary_key_component_from_runtime_value(value, field_name)?,
                kind,
                field_name,
            )?,
        },
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner)
            if matches!(inner.as_ref(), AcceptedFieldKind::Relation { .. }) =>
        {
            let Value::List(items) = value else {
                return Err(InternalError::persisted_row_field_encode_internal(
                    field_name,
                ));
            };
            push_binary_list_len(
                out,
                items
                    .iter()
                    .filter(|item| !matches!(item, Value::Null))
                    .count(),
            );
            for item in items {
                if matches!(item, Value::Null) {
                    continue;
                }
                encode_primary_key_component_field_binary_into(
                    out,
                    primary_key_component_from_runtime_value(item, field_name)?,
                    inner,
                    field_name,
                )?;
            }
        }
        _ if matches!(value, Value::Null) => push_binary_null(out),
        _ => encode_primary_key_component_field_binary_into(
            out,
            primary_key_component_from_runtime_value(value, field_name)?,
            kind,
            field_name,
        )?,
    }
    Ok(true)
}

// Encode one primary-key-component field into the owner-local Structural
// Binary v1 lane.
pub(super) fn encode_primary_key_component_field_binary_into(
    out: &mut Vec<u8>,
    key: PrimaryKeyComponent,
    kind: &AcceptedFieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, key) {
        (AcceptedFieldKind::Relation { key_kind, .. }, key) => {
            encode_primary_key_component_field_binary_into(out, key, key_kind, field_name)
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
    PrimaryKeyComponent::from_runtime_value(value)
        .ok_or_else(|| InternalError::persisted_row_field_encode_internal(field_name))
}
