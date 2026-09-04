//! Module: data::structural_field::primary_key_component::decode
//! Responsibility: primary-key-component Structural Binary decode and validation.
//! Does not own: relation indexing policy, runtime row decode, or generic value-storage envelopes.
//! Boundary: callers provide field-kind authority; this module returns primary-key components/runtime values only.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            CompleteBinaryValue, TAG_LIST, TAG_NULL,
            parse_binary_head as parse_structural_binary_head,
            walk_binary_list_items as walk_structural_binary_list_items,
        },
        primary_key_component::{
            scalar::{
                decode_account_primary_key_component, decode_int_primary_key_component,
                decode_int128_primary_key_component, decode_nat_primary_key_component,
                decode_nat128_primary_key_component, decode_principal_primary_key_component,
                decode_subaccount_primary_key_component, decode_timestamp_primary_key_component,
                decode_u256_primary_key_component, decode_ulid_primary_key_component,
                decode_unit_primary_key_component,
            },
            supports_primary_key_component_binary_kind,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
    db::schema::AcceptedFieldKind,
    value::Value,
};

/// Decode one accepted relation field payload from Structural Binary v1
/// directly into target primary-key components.
pub(in crate::db) fn decode_accepted_relation_target_primary_key_components_binary_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Vec<PrimaryKeyComponent>, FieldDecodeError> {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => Ok(
            decode_optional_primary_key_component_field_binary_bytes(raw_bytes, key_kind)?
                .into_iter()
                .collect(),
        ),
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => match inner.as_ref() {
            AcceptedFieldKind::Relation { key_kind, .. } => {
                decode_relation_primary_key_component_binary_list_bytes(raw_bytes, key_kind)
            }
            _ => Err(FieldDecodeError::new()),
        },
        _ => Err(FieldDecodeError::new()),
    }
}

/// Decode one primary-key-component Structural Binary v1 field payload
/// directly into its canonical `PrimaryKeyComponent` form.
#[cfg(test)]
pub(in crate::db) fn decode_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    decode_primary_key_component_field(&CompleteBinaryValue::parse(raw_bytes)?, kind)
}

// Decode a scalar component after the complete root has been validated once.
fn decode_primary_key_component_field(
    root: &CompleteBinaryValue<'_>,
    kind: &AcceptedFieldKind,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    match kind {
        AcceptedFieldKind::Account => decode_account_primary_key_component(root),
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64 => decode_int_primary_key_component(root),
        AcceptedFieldKind::Int128 => decode_int128_primary_key_component(root),
        AcceptedFieldKind::Principal => decode_principal_primary_key_component(root),
        AcceptedFieldKind::Relation { key_kind, .. } => {
            decode_primary_key_component_field(root, key_kind)
        }
        AcceptedFieldKind::Subaccount => decode_subaccount_primary_key_component(root),
        AcceptedFieldKind::Timestamp => decode_timestamp_primary_key_component(root),
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => decode_nat_primary_key_component(root),
        AcceptedFieldKind::Nat128 => decode_nat128_primary_key_component(root),
        AcceptedFieldKind::Ulid => decode_ulid_primary_key_component(root),
        AcceptedFieldKind::Unit => decode_unit_primary_key_component(root),
        AcceptedFieldKind::U256 => decode_u256_primary_key_component(root),
        _ => Err(FieldDecodeError::new()),
    }
}

/// Decode one Structural Binary v1 primary-key-component field payload
/// directly into its semantic runtime value.
pub(in crate::db) fn decode_primary_key_component_binary_value_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(None);
    }

    let value = match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            decode_optional_primary_key_component_field_binary_bytes(raw_bytes, key_kind)?
                .map_or(Value::Null, PrimaryKeyComponent::as_runtime_value)
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => match inner.as_ref() {
            AcceptedFieldKind::Relation { key_kind, .. } => Value::List(
                decode_relation_primary_key_component_binary_list_bytes(raw_bytes, key_kind)?
                    .into_iter()
                    .map(PrimaryKeyComponent::as_runtime_value)
                    .collect(),
            ),
            _ => return Err(FieldDecodeError::new()),
        },
        _ => {
            let root = CompleteBinaryValue::parse(raw_bytes)?;
            if root.tag() == TAG_NULL {
                Value::Null
            } else {
                decode_primary_key_component_field(&root, kind)?.as_runtime_value()
            }
        }
    };

    Ok(Some(value))
}

/// Validate one Structural Binary v1 primary-key-component field payload
/// without routing through the generic structural value lane.
#[cfg(test)]
pub(in crate::db) fn validate_primary_key_component_binary_value_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<bool, FieldDecodeError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(false);
    }

    decode_primary_key_component_binary_value_bytes(raw_bytes, kind)?;

    Ok(true)
}

// Decode one singular component payload, treating explicit null as "no target".
pub(in crate::db) fn decode_optional_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    key_kind: &AcceptedFieldKind,
) -> Result<Option<PrimaryKeyComponent>, FieldDecodeError> {
    let root = CompleteBinaryValue::parse(raw_bytes)?;
    if root.tag() == TAG_NULL {
        return Ok(None);
    }

    decode_primary_key_component_field(&root, key_kind).map(Some)
}

// Decode one list/set relation payload from Structural Binary v1 into
// canonical primary-key components while preserving current null-item semantics.
fn decode_relation_primary_key_component_binary_list_bytes(
    raw_bytes: &[u8],
    key_kind: &AcceptedFieldKind,
) -> Result<Vec<PrimaryKeyComponent>, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    if tag == TAG_NULL {
        return Ok(Vec::new());
    }
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new());
    }

    let mut components = Vec::new();
    walk_structural_binary_list_items(raw_bytes, &mut |item_bytes| {
        if let Some(value) =
            decode_optional_primary_key_component_field_binary_bytes(item_bytes, key_kind)?
        {
            components.push(value);
        }

        Ok(())
    })?;

    Ok(components)
}
