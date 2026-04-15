//! Module: data::structural_field::value_storage
//! Responsibility: externally tagged `Value` payload decode plus shallow untyped fallback behavior.
//! Does not own: top-level `ByKind` dispatch, typed wrapper payload definitions, or raw CBOR policy.
//! Boundary: `FieldStorageDecode::Value` and conservative fallback paths route through this module.

use crate::db::data::structural_field::cbor::{
    decode_cbor_float, decode_cbor_integer, decode_text_scalar_bytes, parse_tagged_cbor_head,
    parse_tagged_variant_payload_bytes, payload_bytes, push_account_payload, push_array_len,
    push_bool, push_byte_string, push_decimal_payload, push_float32, push_float64,
    push_int_big_payload, push_map_len, push_null, push_signed_integer, push_subaccount_payload,
    push_text, push_timestamp_payload, push_uint_big_payload, push_unsigned_integer,
    skip_cbor_value, walk_cbor_array_items, walk_cbor_map_entries,
};
use crate::db::data::structural_field::leaf::{
    decode_account_value_bytes, decode_date_value_bytes, decode_decimal_value_bytes,
    decode_duration_value_bytes, decode_int_big_value_bytes, decode_null_value_bytes,
    decode_principal_value_bytes, decode_subaccount_value_bytes, decode_timestamp_value_bytes,
    decode_uint_big_value_bytes, decode_unit_value_bytes,
};
use crate::db::data::structural_field::{
    FieldDecodeError, decode_structural_field_by_kind_bytes,
    validate_structural_field_by_kind_bytes,
};
use crate::{
    error::InternalError,
    model::field::FieldKind,
    types::{Float64, Int, Nat},
    value::{Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};

// Carry the output buffer for recursively decoded `Value::List` items.
type ValueArrayDecodeState = Vec<Value>;

// Carry the output buffer for shallow fallback map entry decode.
type UntypedMapDecodeState = Vec<(Value, Value)>;

// Tag the externally tagged `Value` envelope so payload decode can dispatch
// without repeated string matching downstream.
#[derive(Clone, Copy)]
enum ValueVariantTag {
    Account,
    Date,
    Decimal,
    Duration,
    Enum,
    IntBig,
    List,
    Map,
    Null,
    Principal,
    Scalar(FieldKind),
    Subaccount,
    Timestamp,
    UintBig,
    Unit,
}

// Tag the fixed field names inside the persisted `ValueEnum` payload struct.
#[derive(Clone, Copy)]
enum ValueEnumFieldTag {
    Variant,
    Path,
    Payload,
}

// Resolve one tagged `Value` variant label into its decode contract.
fn parse_value_variant_tag(variant: &str) -> Result<ValueVariantTag, FieldDecodeError> {
    let tag = match variant {
        "Account" => ValueVariantTag::Account,
        "Blob" => ValueVariantTag::Scalar(FieldKind::Blob),
        "Bool" => ValueVariantTag::Scalar(FieldKind::Bool),
        "Date" => ValueVariantTag::Date,
        "Decimal" => ValueVariantTag::Decimal,
        "Duration" => ValueVariantTag::Duration,
        "Enum" => ValueVariantTag::Enum,
        "Float32" => ValueVariantTag::Scalar(FieldKind::Float32),
        "Float64" => ValueVariantTag::Scalar(FieldKind::Float64),
        "Int" => ValueVariantTag::Scalar(FieldKind::Int),
        "Int128" => ValueVariantTag::Scalar(FieldKind::Int128),
        "IntBig" => ValueVariantTag::IntBig,
        "List" => ValueVariantTag::List,
        "Map" => ValueVariantTag::Map,
        "Null" => ValueVariantTag::Null,
        "Principal" => ValueVariantTag::Principal,
        "Subaccount" => ValueVariantTag::Subaccount,
        "Text" => ValueVariantTag::Scalar(FieldKind::Text),
        "Timestamp" => ValueVariantTag::Timestamp,
        "Uint" => ValueVariantTag::Scalar(FieldKind::Uint),
        "Uint128" => ValueVariantTag::Scalar(FieldKind::Uint128),
        "UintBig" => ValueVariantTag::UintBig,
        "Ulid" => ValueVariantTag::Scalar(FieldKind::Ulid),
        "Unit" => ValueVariantTag::Unit,
        other => {
            return Err(FieldDecodeError::new(format!(
                "unsupported value variant '{other}'"
            )));
        }
    };

    Ok(tag)
}

// Resolve one raw CBOR-encoded `ValueEnum` field name without re-running text
// literal decoding for each known field.
fn parse_value_enum_field_tag(raw_bytes: &[u8]) -> Option<ValueEnumFieldTag> {
    match raw_bytes {
        b"\x67variant" => Some(ValueEnumFieldTag::Variant),
        b"\x64path" => Some(ValueEnumFieldTag::Path),
        b"\x67payload" => Some(ValueEnumFieldTag::Payload),
        _ => None,
    }
}

/// Encode one persisted `FieldStorageDecode::Value` payload through the
/// owner-local structural value-storage contract.
pub(in crate::db) fn encode_structural_value_storage_bytes(
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_value_storage_into(&mut encoded, value)?;

    Ok(encoded)
}

// Encode one runtime `Value` into the canonical externally tagged storage
// envelope consumed by the structural value-storage decoder.
fn encode_value_storage_into(out: &mut Vec<u8>, value: &Value) -> Result<(), InternalError> {
    match value {
        Value::Null => push_text_variant_label(out, "Null"),
        Value::Unit => push_text_variant_label(out, "Unit"),
        Value::Account(value) => {
            push_single_entry_variant_label(out, "Account");
            push_account_payload(out, *value);
        }
        Value::Blob(value) => {
            push_single_entry_variant_label(out, "Blob");
            push_byte_string(out, value.as_slice());
        }
        Value::Bool(value) => {
            push_single_entry_variant_label(out, "Bool");
            push_bool(out, *value);
        }
        Value::Date(value) => {
            push_single_entry_variant_label(out, "Date");
            push_text(out, &value.to_string());
        }
        Value::Decimal(value) => {
            push_single_entry_variant_label(out, "Decimal");
            push_decimal_payload(out, *value);
        }
        Value::Duration(value) => {
            push_single_entry_variant_label(out, "Duration");
            push_unsigned_integer(out, u128::from(value.as_millis()));
        }
        Value::Enum(value) => {
            push_single_entry_variant_label(out, "Enum");
            push_value_enum_payload(out, value)?;
        }
        Value::Float32(value) => {
            push_single_entry_variant_label(out, "Float32");
            push_float32(out, value.get());
        }
        Value::Float64(value) => {
            push_single_entry_variant_label(out, "Float64");
            push_float64(out, value.get());
        }
        Value::Int(value) => {
            push_single_entry_variant_label(out, "Int");
            push_signed_integer(out, i128::from(*value));
        }
        Value::Int128(value) => {
            push_single_entry_variant_label(out, "Int128");
            push_byte_string(out, &value.get().to_be_bytes());
        }
        Value::IntBig(value) => {
            push_single_entry_variant_label(out, "IntBig");
            push_int_big_payload(out, value);
        }
        Value::List(items) => {
            push_single_entry_variant_label(out, "List");
            push_value_list_payload(out, items.as_slice())?;
        }
        Value::Map(entries) => {
            push_single_entry_variant_label(out, "Map");
            push_value_map_payload(out, entries.as_slice())?;
        }
        Value::Principal(value) => {
            push_single_entry_variant_label(out, "Principal");
            push_byte_string(out, value.as_slice());
        }
        Value::Subaccount(value) => {
            push_single_entry_variant_label(out, "Subaccount");
            push_subaccount_payload(out, *value);
        }
        Value::Text(value) => {
            push_single_entry_variant_label(out, "Text");
            push_text(out, value);
        }
        Value::Timestamp(value) => {
            push_single_entry_variant_label(out, "Timestamp");
            push_timestamp_payload(out, *value)?;
        }
        Value::Uint(value) => {
            push_single_entry_variant_label(out, "Uint");
            push_unsigned_integer(out, u128::from(*value));
        }
        Value::Uint128(value) => {
            push_single_entry_variant_label(out, "Uint128");
            push_byte_string(out, &value.get().to_be_bytes());
        }
        Value::UintBig(value) => {
            push_single_entry_variant_label(out, "UintBig");
            push_uint_big_payload(out, value);
        }
        Value::Ulid(value) => {
            push_single_entry_variant_label(out, "Ulid");
            push_text(out, &value.to_string());
        }
    }

    Ok(())
}

// Encode the single-entry externally tagged enum envelope used for non-unit
// `Value` variants.
fn push_single_entry_variant_label(out: &mut Vec<u8>, label: &str) {
    push_map_len(out, 1);
    push_text(out, label);
}

// Encode the text-only externally tagged enum envelope used for unit variants.
fn push_text_variant_label(out: &mut Vec<u8>, label: &str) {
    push_text(out, label);
}

// Encode one `Value::List` payload as an array of recursively tagged nested
// `Value` items.
fn push_value_list_payload(out: &mut Vec<u8>, items: &[Value]) -> Result<(), InternalError> {
    push_array_len(out, items.len());
    for item in items {
        encode_value_storage_into(out, item)?;
    }

    Ok(())
}

// Encode one `Value::Map` payload as the canonical array-of-entry-pairs shape.
fn push_value_map_payload(
    out: &mut Vec<u8>,
    entries: &[(Value, Value)],
) -> Result<(), InternalError> {
    push_array_len(out, entries.len());
    for (key, value) in entries {
        push_array_len(out, 2);
        encode_value_storage_into(out, key)?;
        encode_value_storage_into(out, value)?;
    }

    Ok(())
}

// Encode one `ValueEnum` payload struct, preserving the stable field order and
// explicit `null` markers used by the derived serde wire.
fn push_value_enum_payload(out: &mut Vec<u8>, value: &ValueEnum) -> Result<(), InternalError> {
    push_map_len(out, 3);

    push_text(out, "variant");
    push_text(out, value.variant());

    push_text(out, "path");
    match value.path() {
        Some(path) => push_text(out, path),
        None => push_null(out),
    }

    push_text(out, "payload");
    match value.payload() {
        Some(payload) => encode_value_storage_into(out, payload)?,
        None => push_null(out),
    }

    Ok(())
}

// Push one recursively tagged `Value` list item into the decoded buffer.
//
// Safety:
// `context` must be a valid `ValueArrayDecodeState`.
fn push_value_array_item(item_bytes: &[u8], context: *mut ()) -> Result<(), FieldDecodeError> {
    let items = unsafe { &mut *context.cast::<ValueArrayDecodeState>() };
    items.push(decode_structural_value_storage_bytes(item_bytes)?);

    Ok(())
}

// Push one shallow fallback list item into the decoded buffer.
//
// Safety:
// `context` must be a valid `ValueArrayDecodeState`.
fn push_untyped_array_item(item_bytes: &[u8], context: *mut ()) -> Result<(), FieldDecodeError> {
    let items = unsafe { &mut *context.cast::<ValueArrayDecodeState>() };
    items.push(decode_untyped_shallow_bytes(item_bytes)?);

    Ok(())
}

// Push one decoded `Value::Map` entry into the runtime entry buffer.
//
// Safety:
// `context` must be a valid `Vec<(Value, Value)>`.
fn push_value_storage_map_entry_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let entries = unsafe { &mut *context.cast::<Vec<(Value, Value)>>() };
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(item_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: truncated value map entry",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(FieldDecodeError::new(
            "expected two-item CBOR array for value map entry",
        ));
    }

    let key_start = cursor;
    cursor = skip_cbor_value(item_bytes, cursor)?;
    let value_start = cursor;
    cursor = skip_cbor_value(item_bytes, cursor)?;
    if cursor != item_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after value map entry",
        ));
    }

    entries.push((
        decode_structural_value_storage_bytes(&item_bytes[key_start..value_start])?,
        decode_structural_value_storage_bytes(&item_bytes[value_start..cursor])?,
    ));

    Ok(())
}

// Push one shallow fallback map entry into the decoded runtime entry buffer.
//
// Safety:
// `context` must be a valid `UntypedMapDecodeState`.
fn push_untyped_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let entries = unsafe { &mut *context.cast::<UntypedMapDecodeState>() };
    entries.push((
        decode_untyped_shallow_bytes(key_bytes)?,
        decode_untyped_shallow_bytes(value_bytes)?,
    ));

    Ok(())
}

// Validate one recursively tagged `Value` list item without pushing it into a
// runtime buffer.
//
// Safety:
// `context` is unused for this callback.
fn validate_value_array_item(item_bytes: &[u8], _context: *mut ()) -> Result<(), FieldDecodeError> {
    validate_structural_value_storage_bytes(item_bytes)
}

// Validate one encoded `Value::Map` entry without allocating decoded key/value
// pairs.
//
// Safety:
// `context` is unused for this callback.
fn validate_value_storage_map_entry_item(
    item_bytes: &[u8],
    _context: *mut (),
) -> Result<(), FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(item_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: truncated value map entry",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(FieldDecodeError::new(
            "expected two-item CBOR array for value map entry",
        ));
    }

    let key_start = cursor;
    cursor = skip_cbor_value(item_bytes, cursor)?;
    let value_start = cursor;
    cursor = skip_cbor_value(item_bytes, cursor)?;
    if cursor != item_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after value map entry",
        ));
    }

    validate_structural_value_storage_bytes(&item_bytes[key_start..value_start])?;
    validate_structural_value_storage_bytes(&item_bytes[value_start..cursor])
}

// Validate one shallow fallback list item without pushing it into a runtime
// buffer.
//
// Safety:
// `context` is unused for this callback.
fn validate_untyped_array_item(
    item_bytes: &[u8],
    _context: *mut (),
) -> Result<(), FieldDecodeError> {
    validate_untyped_shallow_bytes(item_bytes)
}

// Validate one shallow fallback map entry without allocating runtime keys or
// values.
//
// Safety:
// `context` is unused for this callback.
fn validate_untyped_map_entry(
    key_bytes: &[u8],
    value_bytes: &[u8],
    _context: *mut (),
) -> Result<(), FieldDecodeError> {
    validate_untyped_shallow_bytes(key_bytes)?;
    validate_untyped_shallow_bytes(value_bytes)
}

// Decode one `FieldStorageDecode::Value` payload directly from the externally
// tagged `Value` wire shape without routing through serde's recursive enum
// visitor graph.
pub(in crate::db) fn decode_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR: truncated value payload",
        "expected text or one-entry CBOR map for value payload",
        "expected one-entry CBOR map for value payload",
        "typed CBOR: trailing bytes after value payload",
    )?;
    let variant = parse_value_variant_tag(variant)?;

    if let Some(payload_bytes) = payload_bytes {
        decode_value_variant_payload(variant, payload_bytes)
    } else {
        decode_unit_value_variant(variant)
    }
}

/// Validate one `FieldStorageDecode::Value` payload directly from the
/// externally tagged `Value` wire shape without eagerly rebuilding the final
/// runtime `Value`.
pub(in crate::db) fn validate_structural_value_storage_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    let (variant, payload_bytes) = parse_tagged_variant_payload_bytes(
        raw_bytes,
        "typed CBOR: truncated value payload",
        "expected text or one-entry CBOR map for value payload",
        "expected one-entry CBOR map for value payload",
        "typed CBOR: trailing bytes after value payload",
    )?;
    let variant = parse_value_variant_tag(variant)?;

    if let Some(payload_bytes) = payload_bytes {
        validate_value_variant_payload(variant, payload_bytes)
    } else {
        validate_unit_value_variant(variant)
    }
}

// Decode one conservative enum payload directly from bytes.
//
// This keeps the fallback shallow: scalar payloads decode directly, and
// composite payloads decode only one structural level before degrading nested
// composites to `Null`.
pub(super) fn decode_untyped_enum_payload_bytes(
    raw_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };

    match major {
        0 | 1 | 2 | 3 | 7 => decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start),
        4 => decode_untyped_list_bytes(raw_bytes),
        5 => decode_untyped_map_bytes(raw_bytes),
        _ => Err(FieldDecodeError::new("unsupported enum payload CBOR shape")),
    }
}

// Normalize decoded map entries in place when they satisfy the runtime map
// invariants, but preserve the original decoded order when validation rejects
// the shape. This keeps current semantics without cloning the whole entry list.
pub(super) fn normalize_map_entries_or_preserve(mut entries: Vec<(Value, Value)>) -> Value {
    if Value::validate_map_entries(&entries).is_err() {
        return Value::Map(entries);
    }

    Value::sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == std::cmp::Ordering::Equal {
            return Value::Map(entries);
        }
    }

    Value::Map(entries)
}

// Decode one unit `Value` variant from the externally tagged wire shape.
fn decode_unit_value_variant(variant: ValueVariantTag) -> Result<Value, FieldDecodeError> {
    match variant {
        ValueVariantTag::Null => Ok(Value::Null),
        ValueVariantTag::Unit => Ok(Value::Unit),
        _ => Err(FieldDecodeError::new("unsupported unit value variant")),
    }
}

// Validate one unit `Value` variant from the externally tagged wire shape.
fn validate_unit_value_variant(variant: ValueVariantTag) -> Result<(), FieldDecodeError> {
    match variant {
        ValueVariantTag::Null | ValueVariantTag::Unit => Ok(()),
        _ => Err(FieldDecodeError::new("unsupported unit value variant")),
    }
}

// Decode one non-unit `Value` payload variant using the variant's declared
// runtime contract.
fn decode_value_variant_payload(
    variant: ValueVariantTag,
    payload_bytes: &[u8],
) -> Result<Value, FieldDecodeError> {
    match variant {
        ValueVariantTag::Account => decode_account_value_bytes(payload_bytes),
        ValueVariantTag::Date => decode_date_value_bytes(payload_bytes),
        ValueVariantTag::Decimal => decode_decimal_value_bytes(payload_bytes),
        ValueVariantTag::Duration => decode_duration_value_bytes(payload_bytes),
        ValueVariantTag::Enum => decode_value_enum_payload_bytes(payload_bytes),
        ValueVariantTag::IntBig => decode_int_big_value_bytes(payload_bytes),
        ValueVariantTag::List => decode_value_storage_list_bytes(payload_bytes),
        ValueVariantTag::Map => decode_value_storage_map_bytes(payload_bytes),
        ValueVariantTag::Null => decode_null_value_bytes(payload_bytes),
        ValueVariantTag::Principal => decode_principal_value_bytes(payload_bytes),
        ValueVariantTag::Scalar(kind) => decode_structural_field_by_kind_bytes(payload_bytes, kind),
        ValueVariantTag::Subaccount => decode_subaccount_value_bytes(payload_bytes),
        ValueVariantTag::Timestamp => decode_timestamp_value_bytes(payload_bytes),
        ValueVariantTag::UintBig => decode_uint_big_value_bytes(payload_bytes),
        ValueVariantTag::Unit => decode_unit_value_bytes(payload_bytes),
    }
}

// Validate one non-unit `Value` payload variant using the variant's declared
// runtime contract without eagerly rebuilding the final `Value`.
fn validate_value_variant_payload(
    variant: ValueVariantTag,
    payload_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    match variant {
        ValueVariantTag::Account => decode_account_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Date => decode_date_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Decimal => decode_decimal_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Duration => decode_duration_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Enum => validate_value_enum_payload_bytes(payload_bytes),
        ValueVariantTag::IntBig => decode_int_big_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::List => validate_value_storage_list_bytes(payload_bytes),
        ValueVariantTag::Map => validate_value_storage_map_bytes(payload_bytes),
        ValueVariantTag::Null => decode_null_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Principal => decode_principal_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Scalar(kind) => {
            validate_structural_field_by_kind_bytes(payload_bytes, kind)
        }
        ValueVariantTag::Subaccount => decode_subaccount_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Timestamp => decode_timestamp_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::UintBig => decode_uint_big_value_bytes(payload_bytes).map(|_| ()),
        ValueVariantTag::Unit => decode_unit_value_bytes(payload_bytes).map(|_| ()),
    }
}

// Decode one persisted `Value::List` payload recursively from raw element bytes.
fn decode_value_storage_list_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let mut items = Vec::new();
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for value list payload",
        "typed CBOR: trailing bytes after value list payload",
        (&raw mut items).cast(),
        push_value_array_item,
    )?;

    Ok(Value::List(items))
}

// Validate one persisted `Value::List` payload recursively from raw element
// bytes without building a `Vec<Value>`.
fn validate_value_storage_list_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for value list payload",
        "typed CBOR: trailing bytes after value list payload",
        std::ptr::null_mut(),
        validate_value_array_item,
    )
}

// Decode one persisted `Value::Map` payload recursively while preserving
// runtime map invariants.
fn decode_value_storage_map_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let mut entries = Vec::new();
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for value map payload",
        "typed CBOR: trailing bytes after value map payload",
        (&raw mut entries).cast(),
        push_value_storage_map_entry_item,
    )?;

    Value::from_map(entries).map_err(|err| FieldDecodeError::new(format!("typed CBOR: {err}")))
}

// Validate one persisted `Value::Map` payload recursively while avoiding a
// temporary runtime entry buffer.
fn validate_value_storage_map_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for value map payload",
        "typed CBOR: trailing bytes after value map payload",
        std::ptr::null_mut(),
        validate_value_storage_map_entry_item,
    )
}

// Decode one persisted `Value::Enum` payload struct without routing through the
// generic `Value` deserializer.
fn decode_value_enum_payload_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: truncated value enum payload",
        ));
    };
    if major != 5 {
        return Err(FieldDecodeError::new(
            "expected CBOR map for value enum payload",
        ));
    }

    let entry_count = usize::try_from(argument)
        .map_err(|_| FieldDecodeError::new("expected bounded CBOR map length"))?;
    let mut variant = None;
    let mut path = None;
    let mut payload = None;

    // Phase 1: collect the struct fields while preserving serde's tolerant
    // unknown-field behavior.
    for _ in 0..entry_count {
        let field_name_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let field_name = &raw_bytes[field_name_start..cursor];

        let field_value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let field_value = &raw_bytes[field_value_start..cursor];

        match parse_value_enum_field_tag(field_name) {
            Some(ValueEnumFieldTag::Variant) => {
                variant = Some(decode_required_text_value_field(field_value)?);
            }
            Some(ValueEnumFieldTag::Path) => {
                path = decode_optional_text_value_field(field_value)?;
            }
            Some(ValueEnumFieldTag::Payload) => {
                payload = decode_optional_nested_value_field(field_value)?;
            }
            None => {}
        }
    }

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after value enum payload",
        ));
    }

    let variant =
        variant.ok_or_else(|| FieldDecodeError::new("typed CBOR: missing enum variant field"))?;
    let mut value = ValueEnum::new(variant, path);
    if let Some(payload) = payload {
        value = value.with_payload(payload);
    }

    Ok(Value::Enum(value))
}

// Validate one persisted `Value::Enum` payload struct without routing through
// the generic `Value` deserializer or allocating the final runtime `ValueEnum`.
fn validate_value_enum_payload_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: truncated value enum payload",
        ));
    };
    if major != 5 {
        return Err(FieldDecodeError::new(
            "expected CBOR map for value enum payload",
        ));
    }

    let entry_count = usize::try_from(argument)
        .map_err(|_| FieldDecodeError::new("expected bounded CBOR map length"))?;
    let mut variant = None;

    // Phase 1: validate the known struct fields while preserving serde's
    // tolerant unknown-field behavior.
    for _ in 0..entry_count {
        let field_name_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let field_name = &raw_bytes[field_name_start..cursor];

        let field_value_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        let field_value = &raw_bytes[field_value_start..cursor];

        match parse_value_enum_field_tag(field_name) {
            Some(ValueEnumFieldTag::Variant) => {
                decode_required_text_value_field(field_value)?;
                variant = Some(());
            }
            Some(ValueEnumFieldTag::Path) => {
                validate_optional_text_value_field(field_value)?;
            }
            Some(ValueEnumFieldTag::Payload) => {
                validate_optional_nested_value_field(field_value)?;
            }
            None => {}
        }
    }

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after value enum payload",
        ));
    }

    variant.ok_or_else(|| FieldDecodeError::new("typed CBOR: missing enum variant field"))?;

    Ok(())
}

fn decode_required_text_value_field(raw_bytes: &[u8]) -> Result<&str, FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: missing text field"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after text field",
        ));
    }
    if major != 3 {
        return Err(FieldDecodeError::new("typed CBOR: expected a text string"));
    }

    decode_text_scalar_bytes(raw_bytes, argument, payload_start)
}

// Decode one optional text field from the `ValueEnum` payload struct.
fn decode_optional_text_value_field(raw_bytes: &[u8]) -> Result<Option<&str>, FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: missing optional text field",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after optional text field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }
    if major != 3 {
        return Err(FieldDecodeError::new("typed CBOR: expected a text string"));
    }

    Ok(Some(decode_text_scalar_bytes(
        raw_bytes,
        argument,
        payload_start,
    )?))
}

// Validate one optional text field from the `ValueEnum` payload struct.
fn validate_optional_text_value_field(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    decode_optional_text_value_field(raw_bytes).map(|_| ())
}

// Decode one optional nested `Value` field from the `ValueEnum` payload struct.
fn decode_optional_nested_value_field(raw_bytes: &[u8]) -> Result<Option<Value>, FieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: missing nested value field",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after nested value field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }

    decode_structural_value_storage_bytes(raw_bytes).map(Some)
}

// Validate one optional nested `Value` field from the `ValueEnum` payload
// struct without eagerly rebuilding the nested runtime `Value`.
fn validate_optional_nested_value_field(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR: missing nested value field",
        ));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after nested value field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(());
    }

    validate_structural_value_storage_bytes(raw_bytes)
}

// Decode one untyped scalar payload directly from bytes.
fn decode_untyped_scalar_bytes(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<Value, FieldDecodeError> {
    let value = match major {
        0 | 1 => decode_untyped_integer(decode_cbor_integer(major, argument)?),
        2 => {
            Value::Blob(payload_bytes(raw_bytes, argument, payload_start, "byte string")?.to_vec())
        }
        3 => Value::Text(decode_text_scalar_bytes(raw_bytes, argument, payload_start)?.to_string()),
        7 => match argument {
            20 => Value::Bool(false),
            21 => Value::Bool(true),
            22 => Value::Null,
            26 | 27 => Value::Float64(
                Float64::try_new(decode_cbor_float(raw_bytes, argument, payload_start)?)
                    .ok_or_else(|| FieldDecodeError::new("non-finite CBOR float payload"))?,
            ),
            _ => {
                return Err(FieldDecodeError::new("unsupported enum payload CBOR shape"));
            }
        },
        _ => {
            return Err(FieldDecodeError::new("unsupported enum payload CBOR shape"));
        }
    };

    Ok(value)
}

// Validate one untyped scalar payload directly from bytes.
fn validate_untyped_scalar_bytes(
    raw_bytes: &[u8],
    major: u8,
    argument: u64,
    payload_start: usize,
) -> Result<(), FieldDecodeError> {
    decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start).map(|_| ())
}

// Decode one untyped list payload one level deep directly from bytes.
fn decode_untyped_list_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let mut values = Vec::new();
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for enum payload array",
        "typed CBOR: trailing bytes after enum payload array",
        (&raw mut values).cast(),
        push_untyped_array_item,
    )?;

    Ok(Value::List(values))
}

// Validate one untyped list payload one level deep directly from bytes.
fn validate_untyped_list_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for enum payload array",
        "typed CBOR: trailing bytes after enum payload array",
        std::ptr::null_mut(),
        validate_untyped_array_item,
    )
}

// Decode one untyped map payload one level deep directly from bytes.
fn decode_untyped_map_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let mut values = Vec::new();
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for enum payload map",
        "typed CBOR: trailing bytes after enum payload map",
        (&raw mut values).cast(),
        push_untyped_map_entry,
    )?;

    Ok(normalize_map_entries_or_preserve(values))
}

// Validate one untyped map payload one level deep directly from bytes.
fn validate_untyped_map_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for enum payload map",
        "typed CBOR: trailing bytes after enum payload map",
        std::ptr::null_mut(),
        validate_untyped_map_entry,
    )
}

// Decode one fallback payload item without rebuilding nested composites.
fn decode_untyped_shallow_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };

    match major {
        0 | 1 | 2 | 3 | 7 => decode_untyped_scalar_bytes(raw_bytes, major, argument, payload_start),
        4 | 5 => Ok(Value::Null),
        _ => Err(FieldDecodeError::new("unsupported enum payload CBOR shape")),
    }
}

// Validate one fallback payload item without rebuilding nested composites.
fn validate_untyped_shallow_bytes(raw_bytes: &[u8]) -> Result<(), FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after enum payload item",
        ));
    }

    match major {
        0 | 1 | 2 | 3 | 7 => {
            validate_untyped_scalar_bytes(raw_bytes, major, argument, payload_start)
        }
        4 | 5 => Ok(()),
        _ => Err(FieldDecodeError::new("unsupported enum payload CBOR shape")),
    }
}

// Validate one conservative enum payload directly from bytes.
//
// This keeps the fallback shallow: scalar payloads validate directly, and
// composite payloads validate only one structural level before nested
// composites degrade to `Null` at runtime.
pub(super) fn validate_untyped_enum_payload_bytes(
    raw_bytes: &[u8],
) -> Result<(), FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };

    match major {
        0 | 1 | 2 | 3 | 7 => {
            validate_untyped_scalar_bytes(raw_bytes, major, argument, payload_start)
        }
        4 => validate_untyped_list_bytes(raw_bytes),
        5 => validate_untyped_map_bytes(raw_bytes),
        _ => Err(FieldDecodeError::new("unsupported enum payload CBOR shape")),
    }
}

// Decode one untyped CBOR integer into the narrowest deterministic runtime value.
fn decode_untyped_integer(value: i128) -> Value {
    if let Ok(value) = u64::try_from(value) {
        return Value::Uint(value);
    }
    if let Ok(value) = i64::try_from(value) {
        return Value::Int(value);
    }

    if value.is_negative() {
        Value::IntBig(Int::from(WrappedInt::from(value)))
    } else {
        Value::UintBig(Nat::from(WrappedNat::from(value.cast_unsigned())))
    }
}
