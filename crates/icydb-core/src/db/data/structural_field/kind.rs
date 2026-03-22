//! Module: data::structural_field::kind
//! Responsibility: composite `ByKind` field decode for lists, maps, and schema enums.
//! Does not own: low-level CBOR parsing, typed wrapper payloads, or `FieldStorageDecode::Value`.
//! Boundary: the structural-field root calls into this module when a field contract requires recursive `ByKind` decode.

use crate::db::data::structural_field::cbor::{
    parse_tagged_variant_payload_bytes, walk_cbor_array_items, walk_cbor_map_entries,
};
use crate::db::data::structural_field::value_storage::{
    decode_untyped_enum_payload_bytes, normalize_map_entries_or_preserve,
};
use crate::db::data::structural_field::{
    StructuralFieldDecodeError, decode_structural_field_by_kind_bytes,
    decode_structural_value_storage_bytes,
};
use crate::{
    model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    value::{Value, ValueEnum},
};

// Carry the output buffer plus the declared inner field kind for list/set decode.
type KindArrayDecodeState = (Vec<Value>, FieldKind);

// Carry the output buffer plus declared key/value kinds for map decode.
type KindMapDecodeState = (Vec<(Value, Value)>, FieldKind, FieldKind);

// Push one by-kind list item into the decoded runtime value buffer.
//
// Safety:
// `context` must be a valid `KindArrayDecodeState`.
fn push_kind_array_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), StructuralFieldDecodeError> {
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
) -> Result<(), StructuralFieldDecodeError> {
    let state = unsafe { &mut *context.cast::<KindMapDecodeState>() };
    state.0.push((
        decode_structural_field_by_kind_bytes(key_bytes, state.1)?,
        decode_structural_field_by_kind_bytes(value_bytes, state.2)?,
    ));

    Ok(())
}

// Decode one list/set field directly from CBOR bytes and recurse only through
// the declared item contract.
pub(super) fn decode_list_bytes(
    raw_bytes: &[u8],
    inner: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let mut state = (Vec::new(), inner);
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for list/set field",
        "typed CBOR decode failed: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_kind_array_item,
    )?;

    Ok(Value::List(state.0))
}

// Decode one map field directly from CBOR bytes and recurse only through the
// declared key/value contracts.
pub(super) fn decode_map_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
    value_kind: FieldKind,
) -> Result<Value, StructuralFieldDecodeError> {
    let mut state = (Vec::new(), key_kind, value_kind);
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for map field",
        "typed CBOR decode failed: trailing bytes after map field",
        (&raw mut state).cast(),
        push_kind_map_entry,
    )?;

    Ok(normalize_map_entries_or_preserve(state.0))
}

// Decode one enum field directly from CBOR bytes using the schema-declared
// variant payload contract when available.
pub(super) fn decode_enum_bytes(
    raw_bytes: &[u8],
    path: &'static str,
    variants: &'static [EnumVariantModel],
) -> Result<Value, StructuralFieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR decode failed: truncated CBOR value",
        "expected text or one-entry CBOR map for enum field",
        "expected one-entry CBOR map for enum payload variant",
        "typed CBOR decode failed: trailing bytes after enum field",
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
