//! Module: data::structural_field::composite
//! Responsibility: recursive composite `ByKind` decode for lists, maps, enums, and relation re-entry.
//! Does not own: low-level CBOR parsing, scalar fast paths, or non-recursive typed leaves.
//! Boundary: the structural-field root routes composite kinds here after scalar and leaf lanes are ruled out.

use crate::db::data::structural_field::cbor::{
    parse_tagged_variant_payload_bytes, walk_cbor_array_items, walk_cbor_map_entries,
};
use crate::db::data::structural_field::value_storage::{
    decode_untyped_enum_payload_bytes, normalize_map_entries_or_preserve,
    validate_structural_value_storage_bytes, validate_untyped_enum_payload_bytes,
};
use crate::db::data::structural_field::{
    FieldDecodeError, decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
    validate_structural_field_by_kind_bytes,
};
use crate::{
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    value::{Value, ValueEnum},
};

///
/// KindArrayDecodeState
///
/// KindArrayDecodeState carries the recursive list/set decode buffer together
/// with the declared inner field contract.
///

type KindArrayDecodeState = (Vec<Value>, FieldKind);

///
/// KindMapDecodeState
///
/// KindMapDecodeState carries the recursive map decode buffer together with
/// the declared key/value field contracts.
///

type KindMapDecodeState = (Vec<(Value, Value)>, FieldKind, FieldKind);

// Carry the declared item contract while the validate-only list/set walker
// checks each recursive element without allocating a `Vec<Value>`.
type KindArrayValidateState = FieldKind;

// Carry the declared key/value contracts while the validate-only map walker
// checks each recursive entry without allocating a `Vec<(Value, Value)>`.
type KindMapValidateState = (FieldKind, FieldKind);

// Push one by-kind list item into the decoded runtime value buffer.
//
// Safety:
// `context` must be a valid `KindArrayDecodeState`.
fn push_kind_array_item(item_bytes: &[u8], context: *mut ()) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindArrayDecodeState>() };
    state
        .0
        .push(decode_structural_field_by_kind_bytes(item_bytes, state.1)?);

    Ok(())
}

// Push one by-kind map entry into the decoded runtime entry buffer.
//
// Safety:
// `context` must be a valid `KindMapDecodeState`.
fn push_kind_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindMapDecodeState>() };
    state.0.push((
        decode_structural_field_by_kind_bytes(key_bytes, state.1)?,
        decode_structural_field_by_kind_bytes(value_bytes, state.2)?,
    ));

    Ok(())
}

// Validate one by-kind list item recursively without pushing it into a decode
// buffer.
//
// Safety:
// `context` must be a valid `KindArrayValidateState`.
fn validate_kind_array_item(item_bytes: &[u8], context: *mut ()) -> Result<(), FieldDecodeError> {
    let kind = unsafe { *context.cast::<KindArrayValidateState>() };

    validate_structural_field_by_kind_bytes(item_bytes, kind)
}

// Validate one by-kind map entry recursively without allocating decoded
// runtime keys or values.
//
// Safety:
// `context` must be a valid `KindMapValidateState`.
fn validate_kind_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let (key_kind, value_kind) = unsafe { *context.cast::<KindMapValidateState>() };
    validate_structural_field_by_kind_bytes(key_bytes, key_kind)?;
    validate_structural_field_by_kind_bytes(value_bytes, value_kind)
}

// Decode one list/set field directly from CBOR bytes and recurse only through
// the declared item contract.
fn decode_list_bytes(raw_bytes: &[u8], inner: FieldKind) -> Result<Value, FieldDecodeError> {
    let mut state = (Vec::new(), inner);
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for list/set field",
        "typed CBOR: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_kind_array_item,
    )?;

    Ok(Value::List(state.0))
}

// Decode one map field directly from CBOR bytes and recurse only through the
// declared key/value contracts.
fn decode_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    let mut state = (Vec::new(), key_kind, value_kind);
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for map field",
        "typed CBOR: trailing bytes after map field",
        (&raw mut state).cast(),
        push_kind_map_entry,
    )?;

    Ok(normalize_map_entries_or_preserve(state.0))
}

// Validate one list/set field directly from CBOR bytes while keeping
// row-open validation independent from runtime `Value` allocation.
fn validate_list_bytes(raw_bytes: &[u8], inner: FieldKind) -> Result<(), FieldDecodeError> {
    let mut state = inner;
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for list/set field",
        "typed CBOR: trailing bytes after list/set field",
        (&raw mut state).cast(),
        validate_kind_array_item,
    )
}

// Validate one map field directly from CBOR bytes while keeping row-open
// validation independent from runtime entry buffers.
fn validate_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    let mut state = (key_kind, value_kind);
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for map field",
        "typed CBOR: trailing bytes after map field",
        (&raw mut state).cast(),
        validate_kind_map_entry,
    )
}

// Decode one enum field directly from CBOR bytes using the schema-declared
// variant payload contract when available.
fn decode_enum_bytes(
    raw_bytes: &[u8],
    path: &'static str,
    variants: &'static [EnumVariantModel],
) -> Result<Value, FieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR: truncated CBOR value",
        "expected text or one-entry CBOR map for enum field",
        "expected one-entry CBOR map for enum payload variant",
        "typed CBOR: trailing bytes after enum field",
    )?;

    if let Some(payload_bytes) = payload_bytes {
        let payload =
            if let Some(variant_model) = variants.iter().find(|item| item.ident() == variant) {
                if let Some(payload_kind) = variant_model.payload_kind() {
                    match variant_model.payload_storage_decode() {
                        FieldStorageDecode::ByKind => {
                            decode_structural_field_by_kind_bytes(payload_bytes, *payload_kind)?
                        }
                        FieldStorageDecode::Value => {
                            decode_structural_value_storage_bytes(payload_bytes)?
                        }
                    }
                } else {
                    decode_untyped_enum_payload_bytes(payload_bytes)?
                }
            } else {
                decode_untyped_enum_payload_bytes(payload_bytes)?
            };

        Ok(Value::Enum(
            ValueEnum::new(variant, Some(path)).with_payload(payload),
        ))
    } else {
        Ok(Value::Enum(ValueEnum::new(variant, Some(path))))
    }
}

// Validate one enum field directly from CBOR bytes using the schema-declared
// payload contract when available, but without building the final runtime
// `Value::Enum`.
fn validate_enum_bytes(
    raw_bytes: &[u8],
    variants: &'static [EnumVariantModel],
) -> Result<(), FieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR: truncated CBOR value",
        "expected text or one-entry CBOR map for enum field",
        "expected one-entry CBOR map for enum payload variant",
        "typed CBOR: trailing bytes after enum field",
    )?;

    let Some(payload_bytes) = payload_bytes else {
        return Ok(());
    };

    if let Some(variant_model) = variants.iter().find(|item| item.ident() == variant)
        && let Some(payload_kind) = variant_model.payload_kind()
    {
        return match variant_model.payload_storage_decode() {
            FieldStorageDecode::ByKind => {
                validate_structural_field_by_kind_bytes(payload_bytes, *payload_kind)
            }
            FieldStorageDecode::Value => validate_structural_value_storage_bytes(payload_bytes),
        };
    }

    validate_untyped_enum_payload_bytes(payload_bytes)
}

/// Decode one recursive composite `ByKind` field payload.
///
/// Composite decode owns all recursive re-entry back into the structural-field
/// boundary. Leaf kinds are intentionally rejected here so the root stays a
/// thin lane router instead of a mixed recursive hub.
pub(super) fn decode_composite_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    match kind {
        FieldKind::Enum { path, variants } => decode_enum_bytes(raw_bytes, path, variants),
        FieldKind::List(inner) | FieldKind::Set(inner) => decode_list_bytes(raw_bytes, *inner),
        FieldKind::Map { key, value } => decode_map_bytes(raw_bytes, *key, *value),
        FieldKind::Relation { key_kind, .. } => {
            decode_structural_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => Err(FieldDecodeError::new(
            "leaf field unexpectedly routed through composite decode",
        )),
    }
}

/// Validate one recursive composite `ByKind` field payload without eagerly
/// rebuilding its runtime `Value`.
pub(super) fn validate_composite_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    match kind {
        FieldKind::Enum { variants, .. } => validate_enum_bytes(raw_bytes, variants),
        FieldKind::List(inner) | FieldKind::Set(inner) => validate_list_bytes(raw_bytes, *inner),
        FieldKind::Map { key, value } => validate_map_bytes(raw_bytes, *key, *value),
        FieldKind::Relation { key_kind, .. } => {
            validate_structural_field_by_kind_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => Err(FieldDecodeError::new(
            "leaf field unexpectedly routed through composite decode",
        )),
    }
}
