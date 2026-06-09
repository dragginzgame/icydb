//! Module: data::structural_field::primary_key_component::decode
//! Responsibility: primary-key-component Structural Binary decode and validation.
//! Does not own: relation indexing policy, runtime row decode, or generic value-storage envelopes.
//! Boundary: callers provide field-kind authority; this module returns primary-key components/runtime values only.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_LIST, TAG_NULL, parse_binary_head as parse_structural_binary_head,
            skip_binary_value as skip_structural_binary_value,
            walk_binary_list_items as walk_structural_binary_list_items,
        },
        primary_key_component::{
            AcceptedRelationKeyDecodeState, RelationKeyDecodeState,
            scalar::{
                decode_account_primary_key_component_binary_bytes,
                decode_int_primary_key_component_binary_bytes,
                decode_nat_primary_key_component_binary_bytes,
                decode_principal_primary_key_component_binary_bytes,
                decode_subaccount_primary_key_component_binary_bytes,
                decode_timestamp_primary_key_component_binary_bytes,
                decode_ulid_primary_key_component_binary_bytes,
                decode_unit_primary_key_component_binary_bytes,
            },
            supports_primary_key_component_binary_kind,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
    db::schema::PersistedFieldKind,
    model::field::FieldKind,
    value::Value,
};

/// Decode one strong-relation field payload from Structural Binary v1 directly
/// into target primary-key components.
#[cfg(test)]
pub(in crate::db) fn decode_relation_target_primary_key_components_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Vec<PrimaryKeyComponent>, FieldDecodeError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => Ok(
            decode_optional_relation_primary_key_component_binary_bytes(raw_bytes, *key_kind)?
                .into_iter()
                .collect(),
        ),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            decode_relation_primary_key_component_binary_list_bytes(raw_bytes, **key_kind)
        }
        _ => Err(FieldDecodeError::new()),
    }
}

/// Decode one accepted strong-relation field payload from Structural Binary v1
/// directly into target primary-key components.
pub(in crate::db) fn decode_accepted_relation_target_primary_key_components_binary_bytes(
    raw_bytes: &[u8],
    kind: &PersistedFieldKind,
) -> Result<Vec<PrimaryKeyComponent>, FieldDecodeError> {
    match kind {
        PersistedFieldKind::Relation { key_kind, .. } => Ok(
            decode_optional_accepted_primary_key_component_field_binary_bytes(raw_bytes, key_kind)?
                .into_iter()
                .collect(),
        ),
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner)
            if matches!(inner.as_ref(), PersistedFieldKind::Relation { .. }) =>
        {
            let PersistedFieldKind::Relation { key_kind, .. } = inner.as_ref() else {
                unreachable!("relation shape checked above");
            };

            decode_accepted_relation_primary_key_component_binary_list_bytes(raw_bytes, key_kind)
        }
        _ => Err(FieldDecodeError::new()),
    }
}

/// Decode one primary-key-component Structural Binary v1 field payload
/// directly into its canonical `PrimaryKeyComponent` form.
pub(in crate::db) fn decode_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    match kind {
        FieldKind::Account => decode_account_primary_key_component_binary_bytes(raw_bytes),
        FieldKind::Int8 | FieldKind::Int16 | FieldKind::Int32 | FieldKind::Int64 => {
            decode_int_primary_key_component_binary_bytes(raw_bytes)
        }
        FieldKind::Int128 => {
            crate::db::data::structural_field::primary_key_component::scalar::decode_int128_primary_key_component_binary_bytes(raw_bytes)
        }
        FieldKind::Principal => decode_principal_primary_key_component_binary_bytes(raw_bytes),
        FieldKind::Relation { key_kind, .. } => {
            decode_primary_key_component_field_binary_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Subaccount => decode_subaccount_primary_key_component_binary_bytes(raw_bytes),
        FieldKind::Timestamp => decode_timestamp_primary_key_component_binary_bytes(raw_bytes),
        FieldKind::Nat8 | FieldKind::Nat16 | FieldKind::Nat32 | FieldKind::Nat64 => {
            decode_nat_primary_key_component_binary_bytes(raw_bytes)
        }
        FieldKind::Nat128 => {
            crate::db::data::structural_field::primary_key_component::scalar::decode_nat128_primary_key_component_binary_bytes(raw_bytes)
        }
        FieldKind::Ulid => decode_ulid_primary_key_component_binary_bytes(raw_bytes),
        FieldKind::Unit => decode_unit_primary_key_component_binary_bytes(raw_bytes),
        _ => Err(FieldDecodeError::new()),
    }
}

/// Decode one optional primary-key-component Structural Binary v1 field
/// payload directly into its canonical `PrimaryKeyComponent` form.
pub(in crate::db) fn decode_optional_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<PrimaryKeyComponent>, FieldDecodeError> {
    if binary_payload_is_null(raw_bytes)? {
        return Ok(None);
    }

    decode_primary_key_component_field_binary_bytes(raw_bytes, kind).map(Some)
}

/// Decode one Structural Binary v1 primary-key-component field payload
/// directly into its semantic runtime value.
pub(in crate::db) fn decode_primary_key_component_binary_value_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(None);
    }

    let value = match kind {
        FieldKind::Relation { key_kind, .. } => {
            decode_optional_relation_primary_key_component_binary_bytes(raw_bytes, *key_kind)?
                .map_or(Value::Null, PrimaryKeyComponent::as_runtime_value)
        }
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => Value::List(
            decode_relation_primary_key_component_binary_list_bytes(raw_bytes, **key_kind)?
                .into_iter()
                .map(PrimaryKeyComponent::as_runtime_value)
                .collect(),
        ),
        _ if binary_payload_is_null(raw_bytes)? => Value::Null,
        _ => decode_primary_key_component_field_binary_bytes(raw_bytes, kind)?.as_runtime_value(),
    };

    Ok(Some(value))
}

/// Validate one Structural Binary v1 primary-key-component field payload
/// without routing through the generic structural value lane.
pub(in crate::db) fn validate_primary_key_component_binary_value_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<bool, FieldDecodeError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(false);
    }

    decode_primary_key_component_binary_value_bytes(raw_bytes, kind)?;

    Ok(true)
}

// Return whether one Structural Binary v1 payload is the explicit null form.
fn binary_payload_is_null(raw_bytes: &[u8]) -> Result<bool, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }

    Ok(tag == TAG_NULL)
}

// Decode one singular relation payload from Structural Binary v1, treating
// explicit null as "no target".
fn decode_optional_relation_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Option<PrimaryKeyComponent>, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }
    if tag == TAG_NULL {
        return Ok(None);
    }

    decode_relation_primary_key_component_binary_scalar_bytes(raw_bytes, key_kind).map(Some)
}

// Decode one accepted singular relation payload from Structural Binary v1,
// treating explicit null as "no target".
pub(in crate::db) fn decode_optional_accepted_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    key_kind: &PersistedFieldKind,
) -> Result<Option<PrimaryKeyComponent>, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new());
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new());
    }
    if tag == TAG_NULL {
        return Ok(None);
    }

    decode_accepted_primary_key_component_field_binary_bytes(raw_bytes, key_kind).map(Some)
}

// Decode one list/set relation payload from Structural Binary v1 into
// canonical primary-key components while preserving current null-item semantics.
fn decode_relation_primary_key_component_binary_list_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
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

    let mut state = (Vec::new(), key_kind);
    walk_structural_binary_list_items(
        raw_bytes,
        (&raw mut state).cast(),
        push_relation_primary_key_component_binary_item,
    )?;

    Ok(state.0)
}

// Decode one accepted list/set relation payload from Structural Binary v1 into
// canonical primary-key components while preserving current null-item semantics.
fn decode_accepted_relation_primary_key_component_binary_list_bytes(
    raw_bytes: &[u8],
    key_kind: &PersistedFieldKind,
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

    let mut state = (Vec::new(), key_kind);
    walk_structural_binary_list_items(
        raw_bytes,
        (&raw mut state).cast(),
        push_accepted_relation_primary_key_component_binary_item,
    )?;

    Ok(state.0)
}

// Decode one relation-compatible scalar field payload from Structural Binary
// v1 into its primary-key-component form.
fn decode_relation_primary_key_component_binary_scalar_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    decode_primary_key_component_field_binary_bytes(raw_bytes, key_kind)
}

// Decode one accepted relation-compatible scalar field payload from Structural
// Binary v1 into its primary-key-component form.
fn decode_accepted_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    kind: &PersistedFieldKind,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    match kind {
        PersistedFieldKind::Account => decode_account_primary_key_component_binary_bytes(raw_bytes),
        PersistedFieldKind::Int8
        | PersistedFieldKind::Int16
        | PersistedFieldKind::Int32
        | PersistedFieldKind::Int64 => decode_int_primary_key_component_binary_bytes(raw_bytes),
        PersistedFieldKind::Int128 => {
            crate::db::data::structural_field::primary_key_component::scalar::decode_int128_primary_key_component_binary_bytes(raw_bytes)
        }
        PersistedFieldKind::Principal => {
            decode_principal_primary_key_component_binary_bytes(raw_bytes)
        }
        PersistedFieldKind::Relation { key_kind, .. } => {
            decode_accepted_primary_key_component_field_binary_bytes(raw_bytes, key_kind)
        }
        PersistedFieldKind::Subaccount => {
            decode_subaccount_primary_key_component_binary_bytes(raw_bytes)
        }
        PersistedFieldKind::Timestamp => {
            decode_timestamp_primary_key_component_binary_bytes(raw_bytes)
        }
        PersistedFieldKind::Nat8
        | PersistedFieldKind::Nat16
        | PersistedFieldKind::Nat32
        | PersistedFieldKind::Nat64 => decode_nat_primary_key_component_binary_bytes(raw_bytes),
        PersistedFieldKind::Nat128 => {
            crate::db::data::structural_field::primary_key_component::scalar::decode_nat128_primary_key_component_binary_bytes(raw_bytes)
        }
        PersistedFieldKind::Ulid => decode_ulid_primary_key_component_binary_bytes(raw_bytes),
        PersistedFieldKind::Unit => decode_unit_primary_key_component_binary_bytes(raw_bytes),
        _ => Err(FieldDecodeError::new()),
    }
}

// Push one Structural Binary relation-key list item into the decoded
// target-key buffer.
//
// Safety:
// `context` must be a valid `RelationKeyDecodeState`.
fn push_relation_primary_key_component_binary_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<RelationKeyDecodeState>() };
    if let Some(value) =
        decode_optional_relation_primary_key_component_binary_bytes(item_bytes, state.1)?
    {
        state.0.push(value);
    }

    Ok(())
}

// Push one accepted Structural Binary relation-key list item into the decoded
// target-key buffer.
//
// Safety:
// `context` must be a valid `AcceptedRelationKeyDecodeState`.
fn push_accepted_relation_primary_key_component_binary_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<AcceptedRelationKeyDecodeState<'_>>() };
    if let Some(value) =
        decode_optional_accepted_primary_key_component_field_binary_bytes(item_bytes, state.1)?
    {
        state.0.push(value);
    }

    Ok(())
}
